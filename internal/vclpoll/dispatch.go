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
	accept(VLSH) (VLSH, [4]byte, uint16, error)
	acceptFull(VLSH) (VLSH, AddrInfo, error)
	acceptFullContext(VLSH, <-chan struct{}) (VLSH, AddrInfo, error)
	read(VLSH, []byte) (int, error)
	readContext(VLSH, []byte, <-chan struct{}) (int, error)
	write(VLSH, []byte) (int, error)
	writeContext(VLSH, []byte, <-chan struct{}) (int, error)
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
