// Package vclnet provides a net-compatible API backed by VPP's VCL library.
//
// It exposes Listen, ListenPacket, Dial, and DialContext that return standard
// net.Listener, net.PacketConn, and net.Conn interfaces, allowing existing Go
// code to use VPP's user-space networking stack with minimal changes —
// typically just replacing "net" with "vclnet" in the import path.
//
// Supported networks: "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6".
// UDP requires VLS Mode 3; Mode 2 returns an error wrapping
// syscall.EOPNOTSUPP before allocating a VLS datagram session.
//
// Before using any vclnet function, the calling process must have VPP
// running with the session layer enabled, and VCL_CONFIG must point to a
// valid vcl.conf file. Call Init() once at program start.
package vclnet

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/aritrbas/vclnet/internal/vclpoll"
)

const defaultBacklog = 128

// Options controls VCL worker selection during initialization. Workers 0 or 1
// preserves the mode-3 shared-worker implementation. Workers > 1 selects the
// experimental mode-2 session-affine worker pool.
type Options struct {
	Workers int
}

// Init initializes the VCL application layer with mode-3 compatibility by
// default. Environment overrides are honored by InitWithOptions.
func Init(appName string) error {
	return InitWithOptions(appName, Options{Workers: 1})
}

// InitWithOptions initializes VCL with an explicit worker count. Environment
// variables VCLNET_VLS_MODE (2 or 3) and VCLNET_WORKERS override opts so CI
// and deployments can select a mode without changing application code.
func InitWithOptions(appName string, opts Options) error {
	if shutdownStarted.Load() {
		return ErrClosed
	}
	mode, workers, err := resolveInitOptions(opts, os.LookupEnv)
	if err != nil {
		return err
	}
	return vclpoll.AppInitWithOptions(appName, vclpoll.InitOptions{Mode: mode, Workers: workers})
}

func resolveInitOptions(opts Options, lookupEnv func(string) (string, bool)) (vclpoll.Mode, int, error) {
	workers := opts.Workers
	if workers == 0 {
		workers = 1
	}
	if workers < 0 {
		return 0, 0, fmt.Errorf("vclnet: Workers must be >= 0")
	}

	if value, ok := lookupEnv("VCLNET_WORKERS"); ok {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 {
			return 0, 0, fmt.Errorf("vclnet: invalid VCLNET_WORKERS %q", value)
		}
		workers = parsed
	}

	mode := vclpoll.Mode3
	if workers > 1 {
		mode = vclpoll.Mode2
	}
	if value, ok := lookupEnv("VCLNET_VLS_MODE"); ok {
		switch value {
		case "2":
			mode = vclpoll.Mode2
		case "3":
			mode = vclpoll.Mode3
		default:
			return 0, 0, fmt.Errorf("vclnet: invalid VCLNET_VLS_MODE %q (want 2 or 3)", value)
		}
	}
	return mode, workers, nil
}

// Listen announces on the local network address.
//
// The network must be "tcp", "tcp4", or "tcp6".
// If the host in the address is empty or unspecified, it listens on all
// available interfaces for the specified network.
//
// Examples:
//
//	vclnet.Listen("tcp", ":8080")           // IPv4 0.0.0.0:8080
//	vclnet.Listen("tcp4", "127.0.0.1:80")   // IPv4 loopback
//	vclnet.Listen("tcp6", "[::1]:443")       // IPv6 loopback
func Listen(network, address string) (net.Listener, error) {
	if shutdownStarted.Load() {
		return nil, opError("listen", network, address, ErrClosed)
	}
	_, ipv6Only, err := parseNetwork(network)
	if err != nil {
		return nil, opError("listen", network, address, err)
	}

	if isUDP(network) {
		return nil, opError("listen", network, address, net.UnknownNetworkError(network))
	}

	addr, err := resolveAddr(network, address)
	if err != nil {
		return nil, opError("listen", network, address, err)
	}

	if addr.Port == 0 {
		return nil, opError("listen", network, address, &net.AddrError{Err: "port 0 is not supported by VCL", Addr: address})
	}

	var vlsh vclpoll.VLSH

	if addr.IP.To4() != nil && !ipv6Only {
		var ip4 [4]byte
		copy(ip4[:], addr.IP.To4())
		vlsh, err = vclpoll.ListenTCP4(ip4, uint16(addr.Port), defaultBacklog)
	} else {
		var ip6 [16]byte
		copy(ip6[:], addr.IP.To16())
		vlsh, err = vclpoll.ListenTCP6(ip6, uint16(addr.Port), defaultBacklog)
		if err == nil && ipv6Only {
			err = vclpoll.SetV6Only(vlsh, true)
			if err != nil {
				_ = vclpoll.Close(vlsh)
			}
		}
	}

	if err != nil {
		return nil, opError("listen", network, address, err)
	}

	info, err := vclpoll.GetLocalAddr(vlsh)
	if err != nil {
		_ = vclpoll.Close(vlsh)
		return nil, opError("listen", network, address, err)
	}
	addr = addrFromInfo(info)
	ln := newTCPListener(vlsh, addr, network)
	live.addListener(ln)
	ln.tracked.Store(true)
	return ln, nil
}

// ListenPacket creates a UDP PacketConn backed by VPP's session-based UDP
// model. Mode 2 returns an error wrapping syscall.EOPNOTSUPP before VLS
// allocation.
//
// VPP's UDP layer is session-oriented: each peer that sends data to this
// address gets its own internal VLS session (similar to TCP accept). The
// returned PacketConn transparently manages these per-peer sessions:
//
//   - ReadFrom returns datagrams from any peer along with the sender's address.
//   - WriteTo routes data to a peer's session. The peer must have previously
//     sent data (VPP cannot originate a session to an arbitrary address from a
//     listener). WriteTo to an unknown peer returns ErrUnknownPeer.
//
// For sending to arbitrary addresses, use connected UDP via Dial("udp", addr).
//
// The network must be "udp", "udp4", or "udp6".
//
// Examples:
//
//	vclnet.ListenPacket("udp", ":9000")
//	vclnet.ListenPacket("udp4", "127.0.0.1:9000")
//	vclnet.ListenPacket("udp6", "[::1]:9000")
func ListenPacket(network, address string) (net.PacketConn, error) {
	if shutdownStarted.Load() {
		return nil, opError("listen", network, address, ErrClosed)
	}
	_, ipv6Only, err := parseNetwork(network)
	if err != nil {
		return nil, opError("listen", network, address, err)
	}

	if !isUDP(network) {
		return nil, opError("listen", network, address, net.UnknownNetworkError(network))
	}

	addr, err := resolveUDPAddr(context.Background(), network, address)
	if err != nil {
		return nil, opError("listen", network, address, err)
	}

	if addr.Port == 0 {
		return nil, opError("listen", network, address, &net.AddrError{Err: "port 0 is not supported by VCL", Addr: address})
	}

	var vlsh vclpoll.VLSH

	if addr.IP.To4() != nil && !ipv6Only {
		var ip4 [4]byte
		copy(ip4[:], addr.IP.To4())
		vlsh, err = vclpoll.BindUDP4(ip4, uint16(addr.Port))
	} else {
		var ip6 [16]byte
		copy(ip6[:], addr.IP.To16())
		vlsh, err = vclpoll.BindUDP6(ip6, uint16(addr.Port))
		if err == nil && ipv6Only {
			err = vclpoll.SetV6Only(vlsh, true)
			if err != nil {
				_ = vclpoll.Close(vlsh)
			}
		}
	}

	if err != nil {
		return nil, opError("listen", network, address, err)
	}

	info, err := vclpoll.GetLocalAddr(vlsh)
	if err != nil {
		_ = vclpoll.Close(vlsh)
		return nil, opError("listen", network, address, err)
	}
	addr = udpAddrFromInfo(info)
	pc := newPacketConn(vlsh, addr)
	live.addPacketConn(pc)
	pc.tracked.Store(true)
	return pc, nil
}

// DialContext connects to the address on the named network, respecting the
// context's deadline and cancellation.
//
// Supported networks: "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6".
// UDP requires VLS Mode 3; Mode 2 returns an error wrapping
// syscall.EOPNOTSUPP.
//
// For "tcp" (no suffix), DialContext uses RFC 6555 Happy Eyeballs to try
// both IPv4 and IPv6 concurrently.
func DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	d := &Dialer{}
	return d.DialContext(ctx, network, address)
}

// Dial connects to the address on the named network.
//
// Supported networks: "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6".
// UDP requires VLS Mode 3; Mode 2 returns an error wrapping
// syscall.EOPNOTSUPP.
//
// Examples:
//
//	vclnet.Dial("tcp", "10.0.0.1:80")
//	vclnet.Dial("tcp6", "[::1]:443")
//	vclnet.Dial("udp", "10.0.0.1:53")
func Dial(network, address string) (net.Conn, error) {
	return DialContext(context.Background(), network, address)
}

// DialTimeout acts like Dial but takes a timeout.
// The timeout includes name resolution, if any.
func DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return DialContext(ctx, network, address)
}

// ListenContext is like Listen but returns a *TCPListener that supports
// AcceptContext for context-aware accept operations.
func ListenContext(network, address string) (*TCPListener, error) {
	ln, err := Listen(network, address)
	if err != nil {
		return nil, err
	}
	return &TCPListener{ln.(*tcpListener)}, nil
}

// TCPListener wraps the standard net.Listener with context-aware Accept.
type TCPListener struct {
	inner *tcpListener
}

// Accept waits for and returns the next connection to the listener.
func (l *TCPListener) Accept() (net.Conn, error) {
	return l.inner.Accept()
}

// AcceptContext waits for the next connection, respecting context cancellation.
// Returns an error wrapping context.Canceled or context.DeadlineExceeded if
// the context is done before a connection arrives.
func (l *TCPListener) AcceptContext(ctx context.Context) (net.Conn, error) {
	return l.inner.AcceptContext(ctx)
}

// Close closes the listener. Any blocked Accept or AcceptContext calls will
// be unblocked and return errors. The listener is fully deregistered from VPP.
func (l *TCPListener) Close() error {
	return l.inner.Close()
}

// Addr returns the listener's network address.
func (l *TCPListener) Addr() net.Addr {
	return l.inner.Addr()
}
