package vclpoll

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// dispatcher is the stable Go boundary between vclnet and a VLS threading
// model. Package-level functions below deliberately preserve the existing API;
// only the selected dispatcher is allowed to decide which OS thread enters
// VLS for an operation.
type dispatcher interface {
	appInit(string) error
	beginShutdown()
	stop()
	appDestroy()

	listenTCP4([4]byte, uint16, int) (VLSH, error)
	listenTCP6([16]byte, uint16, int) (VLSH, error)
	dialTCP4([4]byte, uint16) (VLSH, error)
	dialTCP6([16]byte, uint16) (VLSH, error)
	connectTCP4Start([4]byte, uint16) (VLSH, bool, error)
	connectTCP6Start([16]byte, uint16) (VLSH, bool, error)

	// Native VCL TLS (VPPCOM_PROTO_TLS). ckp is a ckpair index previously
	// registered via addCertKeyPair; ckpValid controls whether the ckpair
	// is attached on the client side (server-side always attaches).
	addCertKeyPair([]byte, []byte) (uint32, error)
	delCertKeyPair(uint32) error
	listenTLS4([4]byte, uint16, int, uint32) (VLSH, error)
	listenTLS6([16]byte, uint16, int, uint32) (VLSH, error)
	connectTLS4Start([4]byte, uint16, uint32, bool) (VLSH, bool, error)
	connectTLS6Start([16]byte, uint16, uint32, bool) (VLSH, bool, error)

	accept(VLSH) (VLSH, [4]byte, uint16, error)
	acceptFull(VLSH) (VLSH, AddrInfo, error)
	acceptFullContext(VLSH, <-chan struct{}) (VLSH, AddrInfo, error)
	read(VLSH, []byte) (int, error)
	readContext(VLSH, []byte, <-chan struct{}) (int, error)
	write(VLSH, []byte) (int, error)
	writeContext(VLSH, []byte, <-chan struct{}) (int, error)
	shutdown(VLSH, int) error
	close(VLSH) error
	closeVLSH(VLSH)
	getLocalAddr(VLSH) (AddrInfo, error)
	getPeerAddr(VLSH) (AddrInfo, error)
	setV6Only(VLSH, bool) error

	bindUDP4([4]byte, uint16) (VLSH, error)
	bindUDP6([16]byte, uint16) (VLSH, error)
	connectUDP4([4]byte, uint16) (VLSH, error)
	connectUDP6([16]byte, uint16) (VLSH, error)
	connectUDP4Start([4]byte, uint16) (VLSH, bool, error)
	connectUDP6Start([16]byte, uint16) (VLSH, bool, error)
	sendTo(VLSH, []byte, AddrInfo) (int, error)
	sendToContext(VLSH, []byte, AddrInfo, <-chan struct{}) (int, error)
	recvFrom(VLSH, []byte) (int, AddrInfo, error)
	recvFromContext(VLSH, []byte, <-chan struct{}) (int, AddrInfo, error)

	pollWaitContext(VLSH, uint32, <-chan struct{}) bool
}

type Mode int

const (
	Mode2 Mode = 2
	Mode3 Mode = 3
)

type InitOptions struct {
	Mode    Mode
	Workers int
}

type dispatcherBox struct{ dispatcher dispatcher }

var (
	activeDispatcher atomic.Value // dispatcherBox
	dispatchInitOnce sync.Once
	dispatchInitErr  error
)

func init() {
	activeDispatcher.Store(dispatcherBox{dispatcher: mode3Dispatcher{}})
}

func currentDispatcher() dispatcher {
	return activeDispatcher.Load().(dispatcherBox).dispatcher
}

func AppInit(appName string) error {
	return AppInitWithOptions(appName, InitOptions{Mode: Mode3, Workers: 1})
}

func AppInitWithOptions(appName string, opts InitOptions) error {
	dispatchInitOnce.Do(func() {
		var selected dispatcher
		switch opts.Mode {
		case Mode3:
			selected = mode3Dispatcher{}
		case Mode2:
			if opts.Workers < 1 {
				dispatchInitErr = fmt.Errorf("vclpoll: mode 2 requires Workers >= 1")
				return
			}
			selected = newMode2Dispatcher(opts.Workers)
		default:
			dispatchInitErr = fmt.Errorf("vclpoll: unsupported VLS mode %d", opts.Mode)
			return
		}
		if err := selected.appInit(appName); err != nil {
			dispatchInitErr = err
			return
		}
		activeDispatcher.Store(dispatcherBox{dispatcher: selected})
	})
	return dispatchInitErr
}

func CurrentMode() Mode {
	if _, ok := currentDispatcher().(*mode2Dispatcher); ok {
		return Mode2
	}
	return Mode3
}

func Mode2OwnershipViolations() uint64 {
	if selected, ok := currentDispatcher().(*mode2Dispatcher); ok {
		return selected.ownershipViolations.Load()
	}
	return 0
}

func BeginShutdown() { currentDispatcher().beginShutdown() }
func StopPoller()    { currentDispatcher().stop() }
func AppDestroy()    { currentDispatcher().appDestroy() }

func ListenTCP4(ip [4]byte, port uint16, backlog int) (VLSH, error) {
	return currentDispatcher().listenTCP4(ip, port, backlog)
}
func ListenTCP6(ip [16]byte, port uint16, backlog int) (VLSH, error) {
	return currentDispatcher().listenTCP6(ip, port, backlog)
}
func DialTCP4(ip [4]byte, port uint16) (VLSH, error) {
	return currentDispatcher().dialTCP4(ip, port)
}
func DialTCP6(ip [16]byte, port uint16) (VLSH, error) {
	return currentDispatcher().dialTCP6(ip, port)
}
func ConnectTCP4Start(ip [4]byte, port uint16) (VLSH, bool, error) {
	return currentDispatcher().connectTCP4Start(ip, port)
}
func ConnectTCP6Start(ip [16]byte, port uint16) (VLSH, bool, error) {
	return currentDispatcher().connectTCP6Start(ip, port)
}

// AddCertKeyPair registers a PEM-encoded certificate and matching key with
// VPP and returns a process-global ckpair index. The index is later attached
// to a VPPCOM_PROTO_TLS session via ListenTLS4/6 or ConnectTLS4/6Start.
// Callers that ship a stable server identity should register once and reuse
// the returned index rather than adding a fresh pair per Dial/Listen.
func AddCertKeyPair(cert, key []byte) (uint32, error) {
	return currentDispatcher().addCertKeyPair(cert, key)
}

// DelCertKeyPair releases a previously registered ckpair. It is safe (but
// not required) to call this on process shutdown — vppcom_app_destroy tears
// down all cert/key state.
func DelCertKeyPair(idx uint32) error {
	return currentDispatcher().delCertKeyPair(idx)
}

// ListenTLS4 creates a non-blocking native VCL TLS listener bound to
// ip4:port. The ckp index must have come from AddCertKeyPair; VPP will use
// its cert/key to complete the server side of the handshake for every
// accepted session (inherited via the listener's transport ext_config).
func ListenTLS4(ip [4]byte, port uint16, backlog int, ckp uint32) (VLSH, error) {
	return currentDispatcher().listenTLS4(ip, port, backlog, ckp)
}

// ListenTLS6 creates a non-blocking native VCL TLS listener bound to
// ip6:port.
func ListenTLS6(ip [16]byte, port uint16, backlog int, ckp uint32) (VLSH, error) {
	return currentDispatcher().listenTLS6(ip, port, backlog, ckp)
}

// ConnectTLS4Start initiates a non-blocking native VCL TLS4 connect. When
// ckpValid is true the caller-supplied ckpair is attached before connect
// (mTLS or when the server enforces a client cert); when false VPP uses the
// default ckpair (index 0), which is anonymous client TLS.
//
// Returns (vlsh, true, nil) on an immediate ready state (rare for TLS),
// (vlsh, false, nil) on EINPROGRESS — caller must wait for EPOLLOUT to
// signal handshake completion — and (invalidVLSH, false, err) on hard
// failure.
func ConnectTLS4Start(ip [4]byte, port uint16, ckp uint32, ckpValid bool) (VLSH, bool, error) {
	return currentDispatcher().connectTLS4Start(ip, port, ckp, ckpValid)
}

// ConnectTLS6Start initiates a non-blocking native VCL TLS6 connect.
func ConnectTLS6Start(ip [16]byte, port uint16, ckp uint32, ckpValid bool) (VLSH, bool, error) {
	return currentDispatcher().connectTLS6Start(ip, port, ckp, ckpValid)
}

func Accept(listener VLSH) (VLSH, [4]byte, uint16, error) {
	return currentDispatcher().accept(listener)
}
func AcceptFull(listener VLSH) (VLSH, AddrInfo, error) {
	return currentDispatcher().acceptFull(listener)
}
func AcceptFullContext(listener VLSH, done <-chan struct{}) (VLSH, AddrInfo, error) {
	return currentDispatcher().acceptFullContext(listener, done)
}
func Read(vlsh VLSH, p []byte) (int, error) { return currentDispatcher().read(vlsh, p) }
func ReadContext(vlsh VLSH, p []byte, done <-chan struct{}) (int, error) {
	return currentDispatcher().readContext(vlsh, p, done)
}
func Write(vlsh VLSH, p []byte) (int, error) { return currentDispatcher().write(vlsh, p) }
func WriteContext(vlsh VLSH, p []byte, done <-chan struct{}) (int, error) {
	return currentDispatcher().writeContext(vlsh, p, done)
}
func Close(vlsh VLSH) error { return currentDispatcher().close(vlsh) }
func CloseVLSH(vlsh VLSH)   { currentDispatcher().closeVLSH(vlsh) }

// POSIX shutdown directions accepted by Shutdown. Values match
// <sys/socket.h> SHUT_RD/SHUT_WR/SHUT_RDWR because they are forwarded
// verbatim into vls_shutdown → vppcom_session_shutdown.
const (
	ShutRD   = 0
	ShutWR   = 1
	ShutRDWR = 2
)

// Shutdown maps to vls_shutdown(vlsh, how). SHUT_RD marks the reading side
// so subsequent reads observe EOF once the receive buffer is drained. SHUT_WR
// marks the writing side and asks VPP to notify the peer; subsequent writes
// return EPIPE. Listeners cannot be shutdown.
//
// Shutdown does not release the vlsh — callers must still Close it. Waiters
// parked in the readiness poller are not woken by vls_shutdown alone; the
// higher-level Conn is responsible for that.
func Shutdown(vlsh VLSH, how int) error {
	return currentDispatcher().shutdown(vlsh, how)
}
func GetLocalAddr(vlsh VLSH) (AddrInfo, error) {
	return currentDispatcher().getLocalAddr(vlsh)
}
func GetPeerAddr(vlsh VLSH) (AddrInfo, error) { return currentDispatcher().getPeerAddr(vlsh) }
func SetV6Only(vlsh VLSH, value bool) error   { return currentDispatcher().setV6Only(vlsh, value) }

func BindUDP4(ip [4]byte, port uint16) (VLSH, error) {
	return currentDispatcher().bindUDP4(ip, port)
}
func BindUDP6(ip [16]byte, port uint16) (VLSH, error) {
	return currentDispatcher().bindUDP6(ip, port)
}
func ConnectUDP4(ip [4]byte, port uint16) (VLSH, error) {
	return currentDispatcher().connectUDP4(ip, port)
}
func ConnectUDP6(ip [16]byte, port uint16) (VLSH, error) {
	return currentDispatcher().connectUDP6(ip, port)
}
func ConnectUDP4Start(ip [4]byte, port uint16) (VLSH, bool, error) {
	return currentDispatcher().connectUDP4Start(ip, port)
}
func ConnectUDP6Start(ip [16]byte, port uint16) (VLSH, bool, error) {
	return currentDispatcher().connectUDP6Start(ip, port)
}
func SendTo(vlsh VLSH, p []byte, addr AddrInfo) (int, error) {
	return currentDispatcher().sendTo(vlsh, p, addr)
}
func SendToContext(vlsh VLSH, p []byte, addr AddrInfo, done <-chan struct{}) (int, error) {
	return currentDispatcher().sendToContext(vlsh, p, addr, done)
}
func RecvFrom(vlsh VLSH, p []byte) (int, AddrInfo, error) {
	return currentDispatcher().recvFrom(vlsh, p)
}
func RecvFromContext(vlsh VLSH, p []byte, done <-chan struct{}) (int, AddrInfo, error) {
	return currentDispatcher().recvFromContext(vlsh, p, done)
}
func PollWaitContext(vlsh VLSH, events uint32, done <-chan struct{}) bool {
	return currentDispatcher().pollWaitContext(vlsh, events, done)
}

type mode3Dispatcher struct{}

func (mode3Dispatcher) appInit(name string) error { return mode3AppInit(name) }
func (mode3Dispatcher) beginShutdown()            { mode3BeginShutdown() }
func (mode3Dispatcher) stop()                     { mode3StopPoller() }
func (mode3Dispatcher) appDestroy()               { mode3AppDestroy() }
func (mode3Dispatcher) listenTCP4(ip [4]byte, p uint16, b int) (VLSH, error) {
	return mode3ListenTCP4(ip, p, b)
}
func (mode3Dispatcher) listenTCP6(ip [16]byte, p uint16, b int) (VLSH, error) {
	return mode3ListenTCP6(ip, p, b)
}
func (mode3Dispatcher) dialTCP4(ip [4]byte, p uint16) (VLSH, error) {
	return mode3DialTCP4(ip, p)
}
func (mode3Dispatcher) dialTCP6(ip [16]byte, p uint16) (VLSH, error) {
	return mode3DialTCP6(ip, p)
}
func (mode3Dispatcher) connectTCP4Start(ip [4]byte, p uint16) (VLSH, bool, error) {
	return mode3ConnectTCP4Start(ip, p)
}
func (mode3Dispatcher) connectTCP6Start(ip [16]byte, p uint16) (VLSH, bool, error) {
	return mode3ConnectTCP6Start(ip, p)
}
func (mode3Dispatcher) addCertKeyPair(cert, key []byte) (uint32, error) {
	return mode3AddCertKeyPair(cert, key)
}
func (mode3Dispatcher) delCertKeyPair(idx uint32) error { return mode3DelCertKeyPair(idx) }
func (mode3Dispatcher) listenTLS4(ip [4]byte, p uint16, b int, ckp uint32) (VLSH, error) {
	return mode3ListenTLS4(ip, p, b, ckp)
}
func (mode3Dispatcher) listenTLS6(ip [16]byte, p uint16, b int, ckp uint32) (VLSH, error) {
	return mode3ListenTLS6(ip, p, b, ckp)
}
func (mode3Dispatcher) connectTLS4Start(ip [4]byte, p uint16, ckp uint32, ckpValid bool) (VLSH, bool, error) {
	return mode3ConnectTLS4Start(ip, p, ckp, ckpValid)
}
func (mode3Dispatcher) connectTLS6Start(ip [16]byte, p uint16, ckp uint32, ckpValid bool) (VLSH, bool, error) {
	return mode3ConnectTLS6Start(ip, p, ckp, ckpValid)
}
func (mode3Dispatcher) accept(h VLSH) (VLSH, [4]byte, uint16, error) {
	return mode3Accept(h)
}
func (mode3Dispatcher) acceptFull(h VLSH) (VLSH, AddrInfo, error) {
	return mode3AcceptFull(h)
}
func (mode3Dispatcher) acceptFullContext(h VLSH, done <-chan struct{}) (VLSH, AddrInfo, error) {
	return mode3AcceptFullContext(h, done)
}
func (mode3Dispatcher) read(h VLSH, p []byte) (int, error) { return mode3Read(h, p) }
func (mode3Dispatcher) readContext(h VLSH, p []byte, done <-chan struct{}) (int, error) {
	return mode3ReadContext(h, p, done)
}
func (mode3Dispatcher) write(h VLSH, p []byte) (int, error) { return mode3Write(h, p) }
func (mode3Dispatcher) writeContext(h VLSH, p []byte, done <-chan struct{}) (int, error) {
	return mode3WriteContext(h, p, done)
}
func (mode3Dispatcher) shutdown(h VLSH, how int) error               { return mode3Shutdown(h, how) }
func (mode3Dispatcher) close(h VLSH) error                           { return mode3Close(h) }
func (mode3Dispatcher) closeVLSH(h VLSH)                             { mode3CloseVLSH(h) }
func (mode3Dispatcher) getLocalAddr(h VLSH) (AddrInfo, error)        { return mode3GetLocalAddr(h) }
func (mode3Dispatcher) getPeerAddr(h VLSH) (AddrInfo, error)         { return mode3GetPeerAddr(h) }
func (mode3Dispatcher) setV6Only(h VLSH, value bool) error           { return mode3SetV6Only(h, value) }
func (mode3Dispatcher) bindUDP4(ip [4]byte, p uint16) (VLSH, error)  { return mode3BindUDP4(ip, p) }
func (mode3Dispatcher) bindUDP6(ip [16]byte, p uint16) (VLSH, error) { return mode3BindUDP6(ip, p) }
func (mode3Dispatcher) connectUDP4(ip [4]byte, p uint16) (VLSH, error) {
	return mode3ConnectUDP4(ip, p)
}
func (mode3Dispatcher) connectUDP6(ip [16]byte, p uint16) (VLSH, error) {
	return mode3ConnectUDP6(ip, p)
}
func (mode3Dispatcher) connectUDP4Start(ip [4]byte, p uint16) (VLSH, bool, error) {
	return mode3ConnectUDP4Start(ip, p)
}
func (mode3Dispatcher) connectUDP6Start(ip [16]byte, p uint16) (VLSH, bool, error) {
	return mode3ConnectUDP6Start(ip, p)
}
func (mode3Dispatcher) sendTo(h VLSH, p []byte, a AddrInfo) (int, error) {
	return mode3SendTo(h, p, a)
}
func (mode3Dispatcher) sendToContext(h VLSH, p []byte, a AddrInfo, done <-chan struct{}) (int, error) {
	return mode3SendToContext(h, p, a, done)
}
func (mode3Dispatcher) recvFrom(h VLSH, p []byte) (int, AddrInfo, error) {
	return mode3RecvFrom(h, p)
}
func (mode3Dispatcher) recvFromContext(h VLSH, p []byte, done <-chan struct{}) (int, AddrInfo, error) {
	return mode3RecvFromContext(h, p, done)
}
func (mode3Dispatcher) pollWaitContext(h VLSH, events uint32, done <-chan struct{}) bool {
	return mode3PollWaitContext(h, events, done)
}
