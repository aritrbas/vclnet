// Package vclpoll is the CGo bridge to VPP's VCL Locked Sessions (vls_*) API.
//
// Current implementation:
//   - All sessions are non-blocking.
//   - One persistent VLS epoll poller tracks multiple read/write waiters per
//     session and exact waiter cancellation.
//   - Public-package TCP and UDP connects use split-connect plus the shared
//     poller so context cancellation can close in-flight sessions.
//   - Legacy low-level DialTCP*/ConnectUDP* helpers retain a one-shot epoll
//     timeout for compatibility.
//   - Every immediate VLS call pins its goroutine to the current OS thread and
//     registers that pthread once.
package vclpoll

/*
// VPP discovery is driven entirely by pkg-config. The repository ships
// pkgconfig/vppcom.pc.in; run
//
//	make pc VPP_PREFIX=/path/to/vpp
//
// to render pkgconfig/vppcom.pc for a specific VPP install, then build with
// PKG_CONFIG_PATH pointed at that directory (the Makefile does this for you).
// Alternatively, install a vppcom.pc file into the system pkg-config search
// path (or set PKG_CONFIG_PATH yourself).
#cgo pkg-config: vppcom

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <vcl/vppcom.h>
#include <vcl/vcl_locked.h>

static unsigned long vclpoll_pthread_self(void) {
    return (unsigned long)pthread_self();
}

static int vclpoll_app_create(const char *name) {
    return vls_app_create((char *)name);
}

static void vclpoll_app_destroy(void) {
    vppcom_app_destroy();
}

static void vclpoll_register_worker(void) {
    vls_register_vcl_worker();
}

// Create a non-blocking TCP listener bound to the given IPv4+port (BE).
static int vclpoll_listen_tcp4_nb(uint32_t ip4_be, uint16_t port_be,
                                  int backlog) {
    vls_handle_t vlsh = vls_create(VPPCOM_PROTO_TCP, 1);
    if (vlsh < 0) return vlsh;

    uint8_t ip[4];
    memcpy(ip, &ip4_be, 4);

    vppcom_endpt_t ep;
    memset(&ep, 0, sizeof ep);
    ep.is_ip4 = VPPCOM_IS_IP4;
    ep.ip     = ip;
    ep.port   = port_be;

    int rv = vls_bind(vlsh, &ep);
    if (rv < 0) { vls_close(vlsh); return rv; }

    rv = vls_listen(vlsh, backlog);
    if (rv < 0) { vls_close(vlsh); return rv; }

    return (int)vlsh;
}

// Create a non-blocking TCP socket and initiate a connect to ip4:port.
// Returns:
//   >=0 on success (immediate connect — rare for VCL),
//   -EINPROGRESS if the connect is in flight (caller must wait for EPOLLOUT),
//   <0 errno on hard failure.
// On any negative return the caller is expected to call vclpoll_close on
// out_vlsh if it was set (>=0).
static int vclpoll_connect_tcp4_nb(uint32_t ip4_be, uint16_t port_be,
                                   int *out_vlsh) {
    vls_handle_t vlsh = vls_create(VPPCOM_PROTO_TCP, 1);
    if (vlsh < 0) { *out_vlsh = -1; return (int)vlsh; }
    *out_vlsh = (int)vlsh;

    uint8_t ip[4];
    memcpy(ip, &ip4_be, 4);

    vppcom_endpt_t ep;
    memset(&ep, 0, sizeof ep);
    ep.is_ip4 = VPPCOM_IS_IP4;
    ep.ip     = ip;
    ep.port   = port_be;

    return vls_connect(vlsh, &ep);
}

// Non-blocking accept. Returns:
//   >=0 : new vlsh
//   <0  : negative errno (-EAGAIN if no pending conn)
static int vclpoll_accept_nb(int listener_vlsh,
                             uint32_t *peer_ip4_be,
                             uint16_t *peer_port_be) {
    uint8_t ip[16] = {0};
    vppcom_endpt_t ep;
    memset(&ep, 0, sizeof ep);
    ep.ip = ip;

    vls_handle_t conn = vls_accept((vls_handle_t)listener_vlsh, &ep,
                                   O_NONBLOCK);
    if (conn < 0) return (int)conn;

    if (peer_ip4_be && ep.is_ip4 == VPPCOM_IS_IP4) memcpy(peer_ip4_be, ip, 4);
    if (peer_port_be) *peer_port_be = ep.port;
    return (int)conn;
}

static int vclpoll_read(int vlsh, void *buf, size_t n) {
    return (int)vls_read((vls_handle_t)vlsh, buf, n);
}

static int vclpoll_write(int vlsh, const void *buf, size_t n) {
    return vls_write((vls_handle_t)vlsh, (void *)buf, n);
}

static int vclpoll_close(int vlsh) {
    return vls_close((vls_handle_t)vlsh);
}

// Wait for a session to become readable / writable using a one-shot
// vls_epoll_create + ctl + wait + close. timeout_s: seconds (negative = infinite).
// events: bitmask of EPOLLIN/EPOLLOUT etc.
//
// Returns: 1 if event ready, 0 on timeout, <0 on error.
static int vclpoll_wait_once(int vlsh, uint32_t events, double timeout_s) {
    vls_handle_t ep = vls_epoll_create();
    if (ep < 0) return (int)ep;

    struct epoll_event ev;
    memset(&ev, 0, sizeof ev);
    ev.events  = events;
    ev.data.u64 = (uint64_t)vlsh;

    int rv = vls_epoll_ctl(ep, EPOLL_CTL_ADD, (vls_handle_t)vlsh, &ev);
    if (rv < 0) { vls_close(ep); return rv; }

    struct epoll_event out;
    int n = vls_epoll_wait(ep, &out, 1, timeout_s);
    int saved = (n < 0) ? n : ((n == 0) ? 0 : 1);

    vls_close(ep);
    return saved;
}

// --- IPv6 support ---

// Create a non-blocking TCP listener bound to an IPv6 address + port (BE).
static int vclpoll_listen_tcp6_nb(const uint8_t ip6[16], uint16_t port_be,
                                  int backlog) {
    vls_handle_t vlsh = vls_create(VPPCOM_PROTO_TCP, 1);
    if (vlsh < 0) return vlsh;

    uint8_t ip[16];
    memcpy(ip, ip6, 16);

    vppcom_endpt_t ep;
    memset(&ep, 0, sizeof ep);
    ep.is_ip4 = VPPCOM_IS_IP6;
    ep.ip     = ip;
    ep.port   = port_be;

    int rv = vls_bind(vlsh, &ep);
    if (rv < 0) { vls_close(vlsh); return rv; }

    rv = vls_listen(vlsh, backlog);
    if (rv < 0) { vls_close(vlsh); return rv; }

    return (int)vlsh;
}

// Create a non-blocking TCP socket and connect to an IPv6 address + port.
static int vclpoll_connect_tcp6_nb(const uint8_t ip6[16], uint16_t port_be,
                                   int *out_vlsh) {
    vls_handle_t vlsh = vls_create(VPPCOM_PROTO_TCP, 1);
    if (vlsh < 0) { *out_vlsh = -1; return (int)vlsh; }
    *out_vlsh = (int)vlsh;

    uint8_t ip[16];
    memcpy(ip, ip6, 16);

    vppcom_endpt_t ep;
    memset(&ep, 0, sizeof ep);
    ep.is_ip4 = VPPCOM_IS_IP6;
    ep.ip     = ip;
    ep.port   = port_be;

    return vls_connect(vlsh, &ep);
}

// Non-blocking accept returning full address info (IPv4 or IPv6).
// peer_ip must be at least 16 bytes. is_ip4 is set to 1 for IPv4, 0 for IPv6.
static int vclpoll_accept_nb_full(int listener_vlsh,
                                  uint8_t *peer_ip, uint16_t *peer_port_be,
                                  int *is_ip4) {
    uint8_t ip[16] = {0};
    vppcom_endpt_t ep;
    memset(&ep, 0, sizeof ep);
    ep.ip = ip;

    vls_handle_t conn = vls_accept((vls_handle_t)listener_vlsh, &ep,
                                   O_NONBLOCK);
    if (conn < 0) return (int)conn;

    if (peer_ip) memcpy(peer_ip, ip, 16);
    if (peer_port_be) *peer_port_be = ep.port;
    if (is_ip4) *is_ip4 = (ep.is_ip4 == VPPCOM_IS_IP4) ? 1 : 0;
    return (int)conn;
}

// --- Address query via vls_attr ---

// Get the local address of a session. ip must be >= 16 bytes.
// Returns 0 on success, <0 on error.
static int vclpoll_get_local_addr(int vlsh, uint8_t *ip, uint16_t *port_be,
                                  int *is_ip4) {
    uint8_t buf[16] = {0};
    vppcom_endpt_t ep;
    memset(&ep, 0, sizeof ep);
    ep.ip = buf;

    uint32_t buflen = sizeof(ep);
    int rv = vls_attr((vls_handle_t)vlsh, VPPCOM_ATTR_GET_LCL_ADDR,
                      &ep, &buflen);
    if (rv < 0) return rv;

    if (ip) memcpy(ip, buf, 16);
    if (port_be) *port_be = ep.port;
    if (is_ip4) *is_ip4 = (ep.is_ip4 == VPPCOM_IS_IP4) ? 1 : 0;
    return 0;
}

// Get the peer address of a session. ip must be >= 16 bytes.
// Returns 0 on success, <0 on error.
static int vclpoll_get_peer_addr(int vlsh, uint8_t *ip, uint16_t *port_be,
                                 int *is_ip4) {
    uint8_t buf[16] = {0};
    vppcom_endpt_t ep;
    memset(&ep, 0, sizeof ep);
    ep.ip = buf;

    uint32_t buflen = sizeof(ep);
    int rv = vls_attr((vls_handle_t)vlsh, VPPCOM_ATTR_GET_PEER_ADDR,
                      &ep, &buflen);
    if (rv < 0) return rv;

    if (ip) memcpy(ip, buf, 16);
    if (port_be) *port_be = ep.port;
    if (is_ip4) *is_ip4 = (ep.is_ip4 == VPPCOM_IS_IP4) ? 1 : 0;
    return 0;
}

// Set IPV6_V6ONLY on a VLS session. value: 1=v6only, 0=dual-stack.
static int vclpoll_set_v6only(int vlsh, int value) {
    uint32_t buflen = sizeof(value);
    return vls_attr((vls_handle_t)vlsh, VPPCOM_ATTR_SET_V6ONLY,
                    &value, &buflen);
}

// --- UDP support ---

// Create a non-blocking UDP socket bound to an IPv4 address + port.
// Calls vls_listen after bind so VPP's session layer routes incoming
// datagrams to this socket (VPP UDP requires listen for server-side reception).
static int vclpoll_bind_udp4_nb(uint32_t ip4_be, uint16_t port_be) {
    vls_handle_t vlsh = vls_create(VPPCOM_PROTO_UDP, 1);
    if (vlsh < 0) return vlsh;

    uint8_t ip[4];
    memcpy(ip, &ip4_be, 4);

    vppcom_endpt_t ep;
    memset(&ep, 0, sizeof ep);
    ep.is_ip4 = VPPCOM_IS_IP4;
    ep.ip     = ip;
    ep.port   = port_be;

    int rv = vls_bind(vlsh, &ep);
    if (rv < 0) { vls_close(vlsh); return rv; }

    rv = vls_listen(vlsh, 1);
    if (rv < 0) { vls_close(vlsh); return rv; }

    return (int)vlsh;
}

// Create a non-blocking UDP socket bound to an IPv6 address + port.
static int vclpoll_bind_udp6_nb(const uint8_t ip6[16], uint16_t port_be) {
    vls_handle_t vlsh = vls_create(VPPCOM_PROTO_UDP, 1);
    if (vlsh < 0) return vlsh;

    uint8_t ip[16];
    memcpy(ip, ip6, 16);

    vppcom_endpt_t ep;
    memset(&ep, 0, sizeof ep);
    ep.is_ip4 = VPPCOM_IS_IP6;
    ep.ip     = ip;
    ep.port   = port_be;

    int rv = vls_bind(vlsh, &ep);
    if (rv < 0) { vls_close(vlsh); return rv; }

    rv = vls_listen(vlsh, 1);
    if (rv < 0) { vls_close(vlsh); return rv; }

    return (int)vlsh;
}

// Create a non-blocking UDP socket and connect to an IPv4 address + port.
static int vclpoll_connect_udp4_nb(uint32_t ip4_be, uint16_t port_be,
                                   int *out_vlsh) {
    vls_handle_t vlsh = vls_create(VPPCOM_PROTO_UDP, 1);
    if (vlsh < 0) { *out_vlsh = -1; return (int)vlsh; }
    *out_vlsh = (int)vlsh;

    uint8_t ip[4];
    memcpy(ip, &ip4_be, 4);

    vppcom_endpt_t ep;
    memset(&ep, 0, sizeof ep);
    ep.is_ip4 = VPPCOM_IS_IP4;
    ep.ip     = ip;
    ep.port   = port_be;

    return vls_connect(vlsh, &ep);
}

// Create a non-blocking UDP socket and connect to an IPv6 address + port.
static int vclpoll_connect_udp6_nb(const uint8_t ip6[16], uint16_t port_be,
                                   int *out_vlsh) {
    vls_handle_t vlsh = vls_create(VPPCOM_PROTO_UDP, 1);
    if (vlsh < 0) { *out_vlsh = -1; return (int)vlsh; }
    *out_vlsh = (int)vlsh;

    uint8_t ip[16];
    memcpy(ip, ip6, 16);

    vppcom_endpt_t ep;
    memset(&ep, 0, sizeof ep);
    ep.is_ip4 = VPPCOM_IS_IP6;
    ep.ip     = ip;
    ep.port   = port_be;

    return vls_connect(vlsh, &ep);
}

// Send a UDP datagram to a specific destination.
static int vclpoll_sendto(int vlsh, const void *buf, size_t n,
                          int is_ip4, const uint8_t *ip, uint16_t port_be) {
    vppcom_endpt_t ep;
    memset(&ep, 0, sizeof ep);
    ep.is_ip4 = is_ip4 ? VPPCOM_IS_IP4 : VPPCOM_IS_IP6;
    ep.ip     = (uint8_t *)ip;
    ep.port   = port_be;

    return vls_sendto((vls_handle_t)vlsh, (void *)buf, n, 0, &ep);
}

// Receive a UDP datagram and populate the sender's address.
static int vclpoll_recvfrom(int vlsh, void *buf, size_t n,
                            uint8_t *peer_ip, uint16_t *peer_port_be,
                            int *is_ip4) {
    uint8_t ip[16] = {0};
    vppcom_endpt_t ep;
    memset(&ep, 0, sizeof ep);
    ep.ip = ip;

    int rv = vls_recvfrom((vls_handle_t)vlsh, buf, n, 0, &ep);
    if (rv < 0) return rv;

    if (peer_ip) memcpy(peer_ip, ip, 16);
    if (peer_port_be) *peer_port_be = ep.port;
    if (is_ip4) *is_ip4 = (ep.is_ip4 == VPPCOM_IS_IP4) ? 1 : 0;
    return rv;
}

// --- Split connect (for context-aware dial) ---

// Initiate a non-blocking TCP4 connect without waiting.
// Returns the vlsh via out_vlsh.
// Return value: >=0 immediate connect, -EINPROGRESS = in flight, <0 hard error.
static int vclpoll_connect_tcp4_start(uint32_t ip4_be, uint16_t port_be,
                                      int *out_vlsh) {
    vls_handle_t vlsh = vls_create(VPPCOM_PROTO_TCP, 1);
    if (vlsh < 0) { *out_vlsh = -1; return (int)vlsh; }
    *out_vlsh = (int)vlsh;

    uint8_t ip[4];
    memcpy(ip, &ip4_be, 4);

    vppcom_endpt_t ep;
    memset(&ep, 0, sizeof ep);
    ep.is_ip4 = VPPCOM_IS_IP4;
    ep.ip     = ip;
    ep.port   = port_be;

    return vls_connect(vlsh, &ep);
}

// Initiate a non-blocking TCP6 connect without waiting.
static int vclpoll_connect_tcp6_start(const uint8_t ip6[16], uint16_t port_be,
                                      int *out_vlsh) {
    vls_handle_t vlsh = vls_create(VPPCOM_PROTO_TCP, 1);
    if (vlsh < 0) { *out_vlsh = -1; return (int)vlsh; }
    *out_vlsh = (int)vlsh;

    uint8_t ip[16];
    memcpy(ip, ip6, 16);

    vppcom_endpt_t ep;
    memset(&ep, 0, sizeof ep);
    ep.is_ip4 = VPPCOM_IS_IP6;
    ep.ip     = ip;
    ep.port   = port_be;

    return vls_connect(vlsh, &ep);
}

// --- Shared poller primitives ---

static int vclpoll_epoll_create(void) {
    return (int)vls_epoll_create();
}

static int vclpoll_epoll_ctl(int ep_vlsh, int op, int vlsh,
                             struct epoll_event *ev) {
    return vls_epoll_ctl((vls_handle_t)ep_vlsh, op,
                         (vls_handle_t)vlsh, ev);
}

static int vclpoll_epoll_wait(int ep_vlsh, struct epoll_event *events,
                              int maxevents, double timeout_s) {
    return vls_epoll_wait((vls_handle_t)ep_vlsh, events, maxevents, timeout_s);
}
*/
import "C"

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// VLSH is a VCL Locked Session handle. It does not leave this package's
// callers as a Go file descriptor — vclnet wraps it.
type VLSH int32

const invalidVLSH VLSH = -1

const (
	// epoll event bits (must match Linux <sys/epoll.h>; VLS uses these as-is).
	epollIn    = 0x001
	epollOut   = 0x004
	epollErr   = 0x008
	epollHup   = 0x010
	epollRDHup = 0x2000
)

var (
	appOnce    sync.Once
	appErr     error
	appCreated atomic.Bool
	appLive    atomic.Bool

	workerRegistry sync.Map // pthread id (uintptr) -> struct{}
)

// AppInit performs the one-time VLS application registration (idempotent).
// Must be called before any other vclpoll function. The OS thread that runs
// AppInit becomes worker 0 — we pre-record it as registered.
func AppInit(appName string) error {
	appOnce.Do(func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		cname := C.CString(appName)
		defer C.free(unsafe.Pointer(cname))
		rv := C.vclpoll_app_create(cname)
		if rv != 0 {
			appErr = fmt.Errorf("vls_app_create failed: rv=%d", int(rv))
			return
		}
		workerRegistry.Store(uintptr(C.vclpoll_pthread_self()), struct{}{})
		appCreated.Store(true)
		appLive.Store(true)
	})
	return appErr
}

// registerThisThread ensures the current OS thread is registered as a VCL
// worker. Must be called with runtime.LockOSThread() held.
func registerThisThread() {
	tid := uintptr(C.vclpoll_pthread_self())
	if _, ok := workerRegistry.Load(tid); !ok {
		C.vclpoll_register_worker()
		workerRegistry.Store(tid, struct{}{})
	}
}

// pin locks the calling goroutine to its current OS thread and ensures
// that thread has been registered as a VCL worker. Callers MUST defer the
// returned unpin function.
func pin() func() {
	runtime.LockOSThread()
	registerThisThread()
	return runtime.UnlockOSThread
}

// --- Shared poller bridge functions (used by poller.go) ---

// pollEvent holds one event returned by pollerEpollWait.
type pollEvent struct {
	Vlsh   VLSH
	Events uint32
}

// pollerEpollCreate creates a persistent vls_epoll handle.
func pollerEpollCreate() (VLSH, error) {
	rv := C.vclpoll_epoll_create()
	if rv < 0 {
		return invalidVLSH, vppErr("epoll_create", int(rv))
	}
	return VLSH(rv), nil
}

// pollerEpollCtlAdd adds a session to the poller's epoll instance.
func pollerEpollCtlAdd(epVLSH, vlsh VLSH, events uint32) error {
	var ev C.struct_epoll_event
	C.memset(unsafe.Pointer(&ev), 0, C.sizeof_struct_epoll_event)
	ev.events = C.uint32_t(events)
	*(*C.uint64_t)(unsafe.Pointer(&ev.data[0])) = C.uint64_t(vlsh)
	rv := C.vclpoll_epoll_ctl(C.int(epVLSH), C.EPOLL_CTL_ADD, C.int(vlsh), &ev)
	if rv < 0 {
		return vppErr("epoll_ctl_add", int(rv))
	}
	return nil
}

// pollerEpollCtlMod updates the event mask for an existing session.
func pollerEpollCtlMod(epVLSH, vlsh VLSH, events uint32) error {
	var ev C.struct_epoll_event
	C.memset(unsafe.Pointer(&ev), 0, C.sizeof_struct_epoll_event)
	ev.events = C.uint32_t(events)
	*(*C.uint64_t)(unsafe.Pointer(&ev.data[0])) = C.uint64_t(vlsh)
	rv := C.vclpoll_epoll_ctl(C.int(epVLSH), C.EPOLL_CTL_MOD, C.int(vlsh), &ev)
	if rv < 0 {
		return vppErr("epoll_ctl_mod", int(rv))
	}
	return nil
}

// pollerEpollCtlDel removes a session from the poller's epoll instance.
func pollerEpollCtlDel(epVLSH, vlsh VLSH) {
	C.vclpoll_epoll_ctl(C.int(epVLSH), C.EPOLL_CTL_DEL, C.int(vlsh), nil)
}

// pollerEpollWait calls vls_epoll_wait on the poller's handle.
// Returns the number of ready events written to buf.
func pollerEpollWait(epVLSH VLSH, buf []pollEvent) int {
	if len(buf) == 0 {
		return 0
	}
	maxEv := len(buf)
	if maxEv > 64 {
		maxEv = 64
	}
	var events [64]C.struct_epoll_event
	n := C.vclpoll_epoll_wait(C.int(epVLSH), &events[0], C.int(maxEv), 0.1)
	if n <= 0 {
		return 0
	}
	for i := 0; i < int(n); i++ {
		buf[i] = pollEvent{
			Vlsh:   VLSH(*(*C.uint64_t)(unsafe.Pointer(&events[i].data[0]))),
			Events: uint32(events[i].events),
		}
	}
	return int(n)
}

func ipBE(ip4 [4]byte) uint32 {
	return uint32(ip4[0]) | uint32(ip4[1])<<8 | uint32(ip4[2])<<16 | uint32(ip4[3])<<24
}

func portBE(p uint16) uint16 { return uint16(p>>8) | uint16(p&0xff)<<8 }

// ListenTCP4 creates a non-blocking TCP listener bound to ip4:port.
func ListenTCP4(ip4 [4]byte, port uint16, backlog int) (VLSH, error) {
	defer pin()()
	rv := C.vclpoll_listen_tcp4_nb(C.uint32_t(ipBE(ip4)), C.uint16_t(portBE(port)),
		C.int(backlog))
	if rv < 0 {
		return invalidVLSH, vppErr("listen_tcp4", int(rv))
	}
	return VLSH(rv), nil
}

// DialTCP4 creates a non-blocking TCP socket and connects to ip4:port,
// waiting for the handshake to complete via a temp-epoll EPOLLOUT wait.
//
// Note: VPP's VPPCOM_ATTR_GET_ERROR is a stub that always returns 0
// (memory file findings from frida-vpp), so we do not double-check
// connection success via SO_ERROR — EPOLLOUT is taken to mean connected,
// matching what LDP itself does in practice.
func DialTCP4(ip4 [4]byte, port uint16) (VLSH, error) {
	defer pin()()

	var outVLSH C.int = -1
	rv := C.vclpoll_connect_tcp4_nb(C.uint32_t(ipBE(ip4)), C.uint16_t(portBE(port)),
		&outVLSH)
	vlsh := VLSH(outVLSH)

	if rv >= 0 {
		// Immediate connect (rare with VCL).
		return vlsh, nil
	}

	// EINPROGRESS or EAGAIN both mean: wait for EPOLLOUT.
	if !isInProgress(int(rv)) && !isAgain(int(rv)) {
		if vlsh >= 0 {
			C.vclpoll_close(C.int(vlsh))
		}
		return invalidVLSH, vppErr("connect", int(rv))
	}

	// Use finite timeout; connect should complete quickly.
	w := C.vclpoll_wait_once(C.int(vlsh), epollOut, 30.0)
	if w < 0 {
		C.vclpoll_close(C.int(vlsh))
		return invalidVLSH, vppErr("connect_wait", int(w))
	}
	if w == 0 {
		C.vclpoll_close(C.int(vlsh))
		return invalidVLSH, vppErr("connect_timeout", -int(syscall.ETIMEDOUT))
	}
	return vlsh, nil
}

// Accept blocks until a new connection arrives. Returns the new conn's VLSH
// and the peer's IPv4 + port (host order).
func Accept(listener VLSH) (VLSH, [4]byte, uint16, error) {
	runtime.LockOSThread()
	registerThisThread()

	for {
		if !appLive.Load() {
			runtime.UnlockOSThread()
			return invalidVLSH, [4]byte{}, 0, ErrClosed
		}
		var peerIP4 C.uint32_t
		var peerPort C.uint16_t
		rv := C.vclpoll_accept_nb(C.int(listener), &peerIP4, &peerPort)
		if rv >= 0 {
			runtime.UnlockOSThread()
			be := uint32(peerIP4)
			ip := [4]byte{byte(be), byte(be >> 8), byte(be >> 16), byte(be >> 24)}
			pBE := uint16(peerPort)
			port := uint16(pBE>>8) | uint16(pBE&0xff)<<8
			return VLSH(rv), ip, port, nil
		}
		if !isAgain(int(rv)) {
			runtime.UnlockOSThread()
			return invalidVLSH, [4]byte{}, 0, vppErr("accept", int(rv))
		}
		runtime.UnlockOSThread()
		pollWait(listener, epollIn)
		runtime.LockOSThread()
		registerThisThread()
	}
}

// Read reads up to len(p) bytes. On EAGAIN it parks via the shared poller.
func Read(vlsh VLSH, p []byte) (int, error) {
	return ReadContext(vlsh, p, nil)
}

// ReadContext is Read with a cancellation signal for the readiness wait.
func ReadContext(vlsh VLSH, p []byte, doneCh <-chan struct{}) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	runtime.LockOSThread()
	registerThisThread()

	for {
		if !appLive.Load() {
			runtime.UnlockOSThread()
			return 0, ErrClosed
		}
		rv := C.vclpoll_read(C.int(vlsh), unsafe.Pointer(&p[0]), C.size_t(len(p)))
		if rv >= 0 {
			runtime.UnlockOSThread()
			return int(rv), nil
		}
		if !isAgain(int(rv)) {
			runtime.UnlockOSThread()
			return 0, vppErr("read", int(rv))
		}
		runtime.UnlockOSThread()
		if !PollWaitContext(vlsh, epollIn, doneCh) {
			return 0, ErrWaitCanceled
		}
		runtime.LockOSThread()
		registerThisThread()
	}
}

// Write writes up to len(p) bytes. On EAGAIN it parks via the shared poller.
func Write(vlsh VLSH, p []byte) (int, error) {
	return WriteContext(vlsh, p, nil)
}

// WriteContext is Write with a cancellation signal for the readiness wait.
func WriteContext(vlsh VLSH, p []byte, doneCh <-chan struct{}) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	runtime.LockOSThread()
	registerThisThread()

	for {
		if !appLive.Load() {
			runtime.UnlockOSThread()
			return 0, ErrClosed
		}
		rv := C.vclpoll_write(C.int(vlsh), unsafe.Pointer(&p[0]), C.size_t(len(p)))
		if rv >= 0 {
			runtime.UnlockOSThread()
			return int(rv), nil
		}
		if !isAgain(int(rv)) {
			runtime.UnlockOSThread()
			return 0, vppErr("write", int(rv))
		}
		runtime.UnlockOSThread()
		if !PollWaitContext(vlsh, epollOut, doneCh) {
			return 0, ErrWaitCanceled
		}
		runtime.LockOSThread()
		registerThisThread()
	}
}

// Close releases the session.
func Close(vlsh VLSH) error {
	pollUnregister(vlsh)
	defer pin()()
	rv := C.vclpoll_close(C.int(vlsh))
	if rv < 0 {
		return vppErr("close", int(rv))
	}
	return nil
}

func isAgain(rv int) bool {
	return rv == -int(syscall.EAGAIN) || rv == -int(syscall.EWOULDBLOCK)
}

func isInProgress(rv int) bool { return rv == -int(syscall.EINPROGRESS) }

// VCLError represents an error returned by the VCL/VLS layer.
// It wraps a syscall.Errno so callers can use errors.Is(err, syscall.ECONNREFUSED) etc.
type VCLError struct {
	Op    string
	Errno syscall.Errno
}

func (e *VCLError) Error() string {
	return fmt.Sprintf("vclpoll: %s: %s", e.Op, e.Errno.Error())
}

func (e *VCLError) Unwrap() error {
	return e.Errno
}

func (e *VCLError) Is(target error) bool {
	if t, ok := target.(syscall.Errno); ok {
		return e.Errno == t
	}
	return false
}

func (e *VCLError) Timeout() bool {
	return e.Errno == syscall.ETIMEDOUT
}

func (e *VCLError) Temporary() bool {
	return e.Errno == syscall.EAGAIN || e.Errno == syscall.EWOULDBLOCK || e.Errno == syscall.EINTR
}

func vppErr(op string, rv int) error {
	if rv >= 0 {
		return nil
	}
	return &VCLError{Op: op, Errno: syscall.Errno(-rv)}
}

// ErrClosed reports that the VCL application or session is closed.
var ErrClosed = errors.New("vclpoll: session closed")

// ErrWaitCanceled reports that a readiness wait was interrupted.
var ErrWaitCanceled = errors.New("vclpoll: readiness wait canceled")

// --- IPv6 support ---

// ListenTCP6 creates a non-blocking TCP listener bound to an IPv6 address.
func ListenTCP6(ip6 [16]byte, port uint16, backlog int) (VLSH, error) {
	defer pin()()

	rv := C.vclpoll_listen_tcp6_nb((*C.uint8_t)(&ip6[0]), C.uint16_t(portBE(port)),
		C.int(backlog))
	if rv < 0 {
		return invalidVLSH, vppErr("listen_tcp6", int(rv))
	}
	return VLSH(rv), nil
}

// DialTCP6 creates a non-blocking TCP socket and connects to an IPv6 address.
func DialTCP6(ip6 [16]byte, port uint16) (VLSH, error) {
	defer pin()()

	var outVLSH C.int = -1
	rv := C.vclpoll_connect_tcp6_nb((*C.uint8_t)(&ip6[0]), C.uint16_t(portBE(port)),
		&outVLSH)
	vlsh := VLSH(outVLSH)

	if rv >= 0 {
		return vlsh, nil
	}

	if !isInProgress(int(rv)) && !isAgain(int(rv)) {
		if vlsh >= 0 {
			C.vclpoll_close(C.int(vlsh))
		}
		return invalidVLSH, vppErr("connect6", int(rv))
	}

	w := C.vclpoll_wait_once(C.int(vlsh), epollOut, 30.0)
	if w < 0 {
		C.vclpoll_close(C.int(vlsh))
		return invalidVLSH, vppErr("connect6_wait", int(w))
	}
	if w == 0 {
		C.vclpoll_close(C.int(vlsh))
		return invalidVLSH, vppErr("connect6_timeout", -int(syscall.ETIMEDOUT))
	}
	return vlsh, nil
}

// AddrInfo holds an IP address (v4 or v6) and port returned from VLS.
type AddrInfo struct {
	IP   [16]byte
	Port uint16
	IsV4 bool
}

// AcceptFull blocks until a new connection arrives, returning full address info.
func AcceptFull(listener VLSH) (VLSH, AddrInfo, error) {
	runtime.LockOSThread()
	registerThisThread()

	for {
		if !appLive.Load() {
			runtime.UnlockOSThread()
			return invalidVLSH, AddrInfo{}, ErrClosed
		}
		var peerIP [16]C.uint8_t
		var peerPort C.uint16_t
		var isIP4 C.int
		rv := C.vclpoll_accept_nb_full(C.int(listener), &peerIP[0], &peerPort, &isIP4)
		if rv >= 0 {
			runtime.UnlockOSThread()
			var info AddrInfo
			for i := 0; i < 16; i++ {
				info.IP[i] = byte(peerIP[i])
			}
			pBE := uint16(peerPort)
			info.Port = uint16(pBE>>8) | uint16(pBE&0xff)<<8
			info.IsV4 = isIP4 != 0
			return VLSH(rv), info, nil
		}
		if !isAgain(int(rv)) {
			runtime.UnlockOSThread()
			return invalidVLSH, AddrInfo{}, vppErr("accept", int(rv))
		}
		runtime.UnlockOSThread()
		pollWait(listener, epollIn)
		runtime.LockOSThread()
		registerThisThread()
	}
}

// AcceptFullContext is like AcceptFull but respects context cancellation.
// Returns ErrClosed if doneCh is closed before a connection arrives.
func AcceptFullContext(listener VLSH, doneCh <-chan struct{}) (VLSH, AddrInfo, error) {
	runtime.LockOSThread()
	registerThisThread()

	for {
		if !appLive.Load() {
			runtime.UnlockOSThread()
			return invalidVLSH, AddrInfo{}, ErrClosed
		}
		var peerIP [16]C.uint8_t
		var peerPort C.uint16_t
		var isIP4 C.int
		rv := C.vclpoll_accept_nb_full(C.int(listener), &peerIP[0], &peerPort, &isIP4)
		if rv >= 0 {
			runtime.UnlockOSThread()
			var info AddrInfo
			for i := 0; i < 16; i++ {
				info.IP[i] = byte(peerIP[i])
			}
			pBE := uint16(peerPort)
			info.Port = uint16(pBE>>8) | uint16(pBE&0xff)<<8
			info.IsV4 = isIP4 != 0
			return VLSH(rv), info, nil
		}
		if !isAgain(int(rv)) {
			runtime.UnlockOSThread()
			return invalidVLSH, AddrInfo{}, vppErr("accept", int(rv))
		}
		runtime.UnlockOSThread()
		if !PollWaitContext(listener, epollIn, doneCh) {
			return invalidVLSH, AddrInfo{}, ErrClosed
		}
		runtime.LockOSThread()
		registerThisThread()
	}
}

// BeginShutdown prevents parked operations from re-entering VLS after the
// shared poller wakes them.
func BeginShutdown() {
	appLive.Store(false)
}

// AppDestroy performs VLS application teardown when AppInit succeeded.
func AppDestroy() {
	if !appCreated.CompareAndSwap(true, false) {
		return
	}
	defer pin()()
	C.vclpoll_app_destroy()
}

// GetLocalAddr retrieves the local address of a session.
func GetLocalAddr(vlsh VLSH) (AddrInfo, error) {
	defer pin()()

	var ip [16]C.uint8_t
	var portBE C.uint16_t
	var isIP4 C.int
	rv := C.vclpoll_get_local_addr(C.int(vlsh), &ip[0], &portBE, &isIP4)
	if rv < 0 {
		return AddrInfo{}, vppErr("get_local_addr", int(rv))
	}
	var info AddrInfo
	for i := 0; i < 16; i++ {
		info.IP[i] = byte(ip[i])
	}
	pBE := uint16(portBE)
	info.Port = uint16(pBE>>8) | uint16(pBE&0xff)<<8
	info.IsV4 = isIP4 != 0
	return info, nil
}

// GetPeerAddr retrieves the remote address of a session.
func GetPeerAddr(vlsh VLSH) (AddrInfo, error) {
	defer pin()()

	var ip [16]C.uint8_t
	var portBE C.uint16_t
	var isIP4 C.int
	rv := C.vclpoll_get_peer_addr(C.int(vlsh), &ip[0], &portBE, &isIP4)
	if rv < 0 {
		return AddrInfo{}, vppErr("get_peer_addr", int(rv))
	}
	var info AddrInfo
	for i := 0; i < 16; i++ {
		info.IP[i] = byte(ip[i])
	}
	pBE := uint16(portBE)
	info.Port = uint16(pBE>>8) | uint16(pBE&0xff)<<8
	info.IsV4 = isIP4 != 0
	return info, nil
}

// SetV6Only sets the IPV6_V6ONLY option on a VLS session.
func SetV6Only(vlsh VLSH, v6only bool) error {
	defer pin()()
	val := 0
	if v6only {
		val = 1
	}
	rv := C.vclpoll_set_v6only(C.int(vlsh), C.int(val))
	if rv < 0 {
		return vppErr("set_v6only", int(rv))
	}
	return nil
}

// --- UDP support ---

// BindUDP4 creates a non-blocking UDP socket bound to ip4:port.
func BindUDP4(ip4 [4]byte, port uint16) (VLSH, error) {
	defer pin()()
	rv := C.vclpoll_bind_udp4_nb(C.uint32_t(ipBE(ip4)), C.uint16_t(portBE(port)))
	if rv < 0 {
		return invalidVLSH, vppErr("bind_udp4", int(rv))
	}
	return VLSH(rv), nil
}

// BindUDP6 creates a non-blocking UDP socket bound to an IPv6 address.
func BindUDP6(ip6 [16]byte, port uint16) (VLSH, error) {
	defer pin()()
	rv := C.vclpoll_bind_udp6_nb((*C.uint8_t)(&ip6[0]), C.uint16_t(portBE(port)))
	if rv < 0 {
		return invalidVLSH, vppErr("bind_udp6", int(rv))
	}
	return VLSH(rv), nil
}

// ConnectUDP4 creates a connected UDP socket to ip4:port.
// Waits for the connect to complete (VPP UDP connect is asynchronous).
func ConnectUDP4(ip4 [4]byte, port uint16) (VLSH, error) {
	defer pin()()
	var outVLSH C.int = -1
	rv := C.vclpoll_connect_udp4_nb(C.uint32_t(ipBE(ip4)), C.uint16_t(portBE(port)), &outVLSH)
	vlsh := VLSH(outVLSH)
	if rv >= 0 {
		return vlsh, nil
	}
	if !isInProgress(int(rv)) && !isAgain(int(rv)) {
		if vlsh >= 0 {
			C.vclpoll_close(C.int(vlsh))
		}
		return invalidVLSH, vppErr("connect_udp4", int(rv))
	}
	w := C.vclpoll_wait_once(C.int(vlsh), epollOut, 10.0)
	if w < 0 {
		C.vclpoll_close(C.int(vlsh))
		return invalidVLSH, vppErr("connect_udp4_wait", int(w))
	}
	if w == 0 {
		C.vclpoll_close(C.int(vlsh))
		return invalidVLSH, vppErr("connect_udp4_timeout", -int(syscall.ETIMEDOUT))
	}
	return vlsh, nil
}

// ConnectUDP6 creates a connected UDP socket to an IPv6 address.
// Waits for the connect to complete (VPP UDP connect is asynchronous).
func ConnectUDP6(ip6 [16]byte, port uint16) (VLSH, error) {
	defer pin()()
	var outVLSH C.int = -1
	rv := C.vclpoll_connect_udp6_nb((*C.uint8_t)(&ip6[0]), C.uint16_t(portBE(port)), &outVLSH)
	vlsh := VLSH(outVLSH)
	if rv >= 0 {
		return vlsh, nil
	}
	if !isInProgress(int(rv)) && !isAgain(int(rv)) {
		if vlsh >= 0 {
			C.vclpoll_close(C.int(vlsh))
		}
		return invalidVLSH, vppErr("connect_udp6", int(rv))
	}
	w := C.vclpoll_wait_once(C.int(vlsh), epollOut, 10.0)
	if w < 0 {
		C.vclpoll_close(C.int(vlsh))
		return invalidVLSH, vppErr("connect_udp6_wait", int(w))
	}
	if w == 0 {
		C.vclpoll_close(C.int(vlsh))
		return invalidVLSH, vppErr("connect_udp6_timeout", -int(syscall.ETIMEDOUT))
	}
	return vlsh, nil
}

// ConnectUDP4Start initiates a non-blocking UDP4 connect without waiting.
func ConnectUDP4Start(ip4 [4]byte, port uint16) (VLSH, bool, error) {
	defer pin()()
	var outVLSH C.int = -1
	rv := C.vclpoll_connect_udp4_nb(C.uint32_t(ipBE(ip4)), C.uint16_t(portBE(port)), &outVLSH)
	vlsh := VLSH(outVLSH)
	if rv >= 0 {
		return vlsh, true, nil
	}
	if isInProgress(int(rv)) || isAgain(int(rv)) {
		return vlsh, false, nil
	}
	if vlsh >= 0 {
		C.vclpoll_close(C.int(vlsh))
	}
	return invalidVLSH, false, vppErr("connect_udp4_start", int(rv))
}

// ConnectUDP6Start initiates a non-blocking UDP6 connect without waiting.
func ConnectUDP6Start(ip6 [16]byte, port uint16) (VLSH, bool, error) {
	defer pin()()
	var outVLSH C.int = -1
	rv := C.vclpoll_connect_udp6_nb((*C.uint8_t)(&ip6[0]), C.uint16_t(portBE(port)), &outVLSH)
	vlsh := VLSH(outVLSH)
	if rv >= 0 {
		return vlsh, true, nil
	}
	if isInProgress(int(rv)) || isAgain(int(rv)) {
		return vlsh, false, nil
	}
	if vlsh >= 0 {
		C.vclpoll_close(C.int(vlsh))
	}
	return invalidVLSH, false, vppErr("connect_udp6_start", int(rv))
}

// SendTo sends a UDP datagram to the specified address.
func SendTo(vlsh VLSH, p []byte, addr AddrInfo) (int, error) {
	return SendToContext(vlsh, p, addr, nil)
}

// SendToContext is SendTo with a cancellation signal for readiness waits.
func SendToContext(vlsh VLSH, p []byte, addr AddrInfo, doneCh <-chan struct{}) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	runtime.LockOSThread()
	registerThisThread()

	var ip [16]byte
	copy(ip[:], addr.IP[:])
	isIP4 := 0
	if addr.IsV4 {
		isIP4 = 1
	}

	for {
		if !appLive.Load() {
			runtime.UnlockOSThread()
			return 0, ErrClosed
		}
		rv := C.vclpoll_sendto(C.int(vlsh), unsafe.Pointer(&p[0]), C.size_t(len(p)),
			C.int(isIP4), (*C.uint8_t)(&ip[0]), C.uint16_t(portBE(addr.Port)))
		if rv >= 0 {
			runtime.UnlockOSThread()
			return int(rv), nil
		}
		if !isAgain(int(rv)) {
			runtime.UnlockOSThread()
			return 0, vppErr("sendto", int(rv))
		}
		runtime.UnlockOSThread()
		if !PollWaitContext(vlsh, epollOut, doneCh) {
			return 0, ErrWaitCanceled
		}
		runtime.LockOSThread()
		registerThisThread()
	}
}

// RecvFrom receives a UDP datagram and returns the sender's address.
func RecvFrom(vlsh VLSH, p []byte) (int, AddrInfo, error) {
	return RecvFromContext(vlsh, p, nil)
}

// RecvFromContext is RecvFrom with a cancellation signal for readiness waits.
func RecvFromContext(vlsh VLSH, p []byte, doneCh <-chan struct{}) (int, AddrInfo, error) {
	if len(p) == 0 {
		return 0, AddrInfo{}, nil
	}
	runtime.LockOSThread()
	registerThisThread()

	for {
		if !appLive.Load() {
			runtime.UnlockOSThread()
			return 0, AddrInfo{}, ErrClosed
		}
		var peerIP [16]C.uint8_t
		var peerPort C.uint16_t
		var isIP4 C.int
		rv := C.vclpoll_recvfrom(C.int(vlsh), unsafe.Pointer(&p[0]), C.size_t(len(p)),
			&peerIP[0], &peerPort, &isIP4)
		if rv >= 0 {
			runtime.UnlockOSThread()
			var info AddrInfo
			for i := 0; i < 16; i++ {
				info.IP[i] = byte(peerIP[i])
			}
			pBE := uint16(peerPort)
			info.Port = uint16(pBE>>8) | uint16(pBE&0xff)<<8
			info.IsV4 = isIP4 != 0
			return int(rv), info, nil
		}
		if !isAgain(int(rv)) {
			runtime.UnlockOSThread()
			return 0, AddrInfo{}, vppErr("recvfrom", int(rv))
		}
		runtime.UnlockOSThread()
		if !PollWaitContext(vlsh, epollIn, doneCh) {
			return 0, AddrInfo{}, ErrWaitCanceled
		}
		runtime.LockOSThread()
		registerThisThread()
	}
}

// --- Split connect (context-aware dial) ---

// ConnectTCP4Start initiates a non-blocking TCP4 connect without waiting.
// Returns (vlsh, true, nil) on immediate connect.
// Returns (vlsh, false, nil) on EINPROGRESS.
// Returns (invalidVLSH, false, err) on hard failure.
func ConnectTCP4Start(ip4 [4]byte, port uint16) (VLSH, bool, error) {
	defer pin()()
	var outVLSH C.int = -1
	rv := C.vclpoll_connect_tcp4_start(C.uint32_t(ipBE(ip4)), C.uint16_t(portBE(port)), &outVLSH)
	vlsh := VLSH(outVLSH)
	if rv >= 0 {
		return vlsh, true, nil
	}
	if isInProgress(int(rv)) || isAgain(int(rv)) {
		return vlsh, false, nil
	}
	if vlsh >= 0 {
		C.vclpoll_close(C.int(vlsh))
	}
	return invalidVLSH, false, vppErr("connect_tcp4_start", int(rv))
}

// ConnectTCP6Start initiates a non-blocking TCP6 connect without waiting.
func ConnectTCP6Start(ip6 [16]byte, port uint16) (VLSH, bool, error) {
	defer pin()()
	var outVLSH C.int = -1
	rv := C.vclpoll_connect_tcp6_start((*C.uint8_t)(&ip6[0]), C.uint16_t(portBE(port)), &outVLSH)
	vlsh := VLSH(outVLSH)
	if rv >= 0 {
		return vlsh, true, nil
	}
	if isInProgress(int(rv)) || isAgain(int(rv)) {
		return vlsh, false, nil
	}
	if vlsh >= 0 {
		C.vclpoll_close(C.int(vlsh))
	}
	return invalidVLSH, false, vppErr("connect_tcp6_start", int(rv))
}

// CloseVLSH closes a raw VLS handle (used for cancellation cleanup).
func CloseVLSH(vlsh VLSH) {
	pollUnregister(vlsh)
	defer pin()()
	C.vclpoll_close(C.int(vlsh))
}
