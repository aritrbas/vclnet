package vclpoll

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const maxVirtualHandle = uint32(1<<31 - 1)

// ErrMode2UDPUnsupported is returned before creating a VLS datagram session.
// The VPP 26.06 VLS Mode 2 implementation can leave a cut-through TX event
// referring to freed session state during close, which crashes VPP after the
// application has otherwise completed successfully.
var ErrMode2UDPUnsupported = fmt.Errorf("vclpoll: UDP is not supported in VLS mode 2 with this VPP build: %w", syscall.EOPNOTSUPP)

type sessionRef struct {
	worker *worker
	raw    VLSH
}

type connectReply struct {
	handle    VLSH
	immediate bool
}

type acceptReply struct {
	handle VLSH
	addr   AddrInfo
}

// mode2Dispatcher routes every operation through the permanently pinned
// worker that created the session. Public handles are virtual because raw VLS
// handles are indices in a worker-local pool and therefore collide between
// workers.
type mode2Dispatcher struct {
	workerCount int
	workers     []*worker
	owners      sync.Map // VLSH -> sessionRef
	rr          atomic.Uint32
	nextHandle  atomic.Uint32

	stopping            atomic.Bool
	ownershipViolations atomic.Uint64
	stopOnce            sync.Once
	destroyOnce         sync.Once
}

func newMode2Dispatcher(workerCount int) *mode2Dispatcher {
	return &mode2Dispatcher{workerCount: workerCount}
}

func (d *mode2Dispatcher) appInit(appName string) error {
	if d.workerCount < 1 {
		return fmt.Errorf("vclpoll: mode 2 requires at least one worker")
	}

	start := make(chan struct{})
	for i := 0; i < d.workerCount; i++ {
		w := newWorker(i, i == 0, d, start)
		go w.loop(appName)
		if err := <-w.registered; err != nil {
			close(start)
			d.stop()
			d.appDestroy()
			return fmt.Errorf("vclpoll: initialize mode-2 worker %d: %w", i, err)
		}
		d.workers = append(d.workers, w)
	}

	close(start)
	for _, w := range d.workers {
		if err := <-w.ready; err != nil {
			d.stop()
			d.appDestroy()
			return fmt.Errorf("vclpoll: initialize mode-2 worker %d epoll: %w", w.id, err)
		}
	}

	appCreated.Store(true)
	appLive.Store(true)
	return nil
}

func (d *mode2Dispatcher) beginShutdown() { appLive.Store(false) }

func (d *mode2Dispatcher) stop() {
	d.stopOnce.Do(func() {
		d.stopping.Store(true)
		for _, w := range d.workers {
			close(w.stopCh)
		}
		for _, w := range d.workers {
			<-w.quiesced
		}
		for _, w := range d.workers {
			if w.bootstrap {
				continue
			}
			<-w.exited
			waitThreadGone(w.tid)
		}
	})
}

func (d *mode2Dispatcher) appDestroy() {
	d.destroyOnce.Do(func() {
		d.stop()
		if len(d.workers) == 0 {
			return
		}
		bootstrap := d.workers[0]
		close(bootstrap.destroyCh)
		<-bootstrap.destroyed
		appCreated.Store(false)
	})
}

func waitThreadGone(tid int) {
	// Go cannot terminate the process main thread. If a lifetime-pinned worker
	// happened to acquire m0, the runtime parks that M during goroutine exit
	// instead (runtime.mexit). It cannot run another goroutine or VLS call, and
	// exit_group will terminate it without running a late VLS TLS destructor.
	if tid <= 0 || tid == os.Getpid() {
		return
	}
	path := filepath.Join("/proc/self/task", strconv.Itoa(tid))
	for {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func (d *mode2Dispatcher) submit(w *worker, run func(*worker) (any, error)) (any, error) {
	if d.stopping.Load() || !appLive.Load() {
		return nil, ErrClosed
	}
	op := &workerOp{run: run, resp: make(chan workerResult, 1)}
	select {
	case w.opCh <- op:
	case <-w.quiesced:
		return nil, ErrClosed
	}
	select {
	case result := <-op.resp:
		return result.value, result.err
	case <-w.quiesced:
		return nil, ErrClosed
	}
}

func (d *mode2Dispatcher) pickWorker() *worker {
	idx := (d.rr.Add(1) - 1) % uint32(len(d.workers))
	return d.workers[idx]
}

func (d *mode2Dispatcher) registerSession(w *worker, raw VLSH) (VLSH, error) {
	if err := w.checkOwnership(raw); err != nil {
		_ = rawClose(raw)
		return invalidVLSH, err
	}
	id := d.nextHandle.Add(1)
	if id == 0 || id > maxVirtualHandle {
		_ = rawClose(raw)
		return invalidVLSH, errors.New("vclpoll: virtual session handle space exhausted")
	}
	public := VLSH(id)
	w.sessions[public] = raw
	d.owners.Store(public, sessionRef{worker: w, raw: raw})
	return public, nil
}

func (d *mode2Dispatcher) lookup(public VLSH) (sessionRef, error) {
	value, ok := d.owners.Load(public)
	if !ok {
		return sessionRef{}, ErrClosed
	}
	return value.(sessionRef), nil
}

func (d *mode2Dispatcher) unregisterSession(w *worker, public VLSH) {
	delete(w.sessions, public)
	d.owners.Delete(public)
}

func (d *mode2Dispatcher) sessionCall(public VLSH, fn func(*worker, VLSH) (any, error)) (any, error) {
	ref, err := d.lookup(public)
	if err != nil {
		return nil, err
	}
	return d.submit(ref.worker, func(w *worker) (any, error) {
		raw, err := w.ownedRaw(public, ref.raw)
		if err != nil {
			return nil, err
		}
		return fn(w, raw)
	})
}

func (d *mode2Dispatcher) create(fn func() (VLSH, error)) (VLSH, error) {
	w := d.pickWorker()
	value, err := d.submit(w, func(w *worker) (any, error) {
		raw, err := fn()
		if err != nil {
			return nil, err
		}
		return d.registerSession(w, raw)
	})
	if err != nil {
		return invalidVLSH, err
	}
	return value.(VLSH), nil
}

func (d *mode2Dispatcher) createConnect(fn func() (VLSH, bool, error)) (VLSH, bool, error) {
	w := d.pickWorker()
	value, err := d.submit(w, func(w *worker) (any, error) {
		raw, immediate, err := fn()
		if err != nil {
			return nil, err
		}
		public, err := d.registerSession(w, raw)
		if err != nil {
			return nil, err
		}
		return connectReply{handle: public, immediate: immediate}, nil
	})
	if err != nil {
		return invalidVLSH, false, err
	}
	reply := value.(connectReply)
	return reply.handle, reply.immediate, nil
}

func (d *mode2Dispatcher) listenTCP4(ip [4]byte, port uint16, backlog int) (VLSH, error) {
	return d.create(func() (VLSH, error) { return mode3ListenTCP4(ip, port, backlog) })
}

func (d *mode2Dispatcher) listenTCP6(ip [16]byte, port uint16, backlog int) (VLSH, error) {
	return d.create(func() (VLSH, error) { return mode3ListenTCP6(ip, port, backlog) })
}

func (d *mode2Dispatcher) connectTCP4Start(ip [4]byte, port uint16) (VLSH, bool, error) {
	return d.createConnect(func() (VLSH, bool, error) { return mode3ConnectTCP4Start(ip, port) })
}

func (d *mode2Dispatcher) connectTCP6Start(ip [16]byte, port uint16) (VLSH, bool, error) {
	return d.createConnect(func() (VLSH, bool, error) { return mode3ConnectTCP6Start(ip, port) })
}

func (d *mode2Dispatcher) dialTCP4(ip [4]byte, port uint16) (VLSH, error) {
	handle, immediate, err := d.connectTCP4Start(ip, port)
	return d.finishLegacyConnect(handle, immediate, err, 30*time.Second)
}

func (d *mode2Dispatcher) dialTCP6(ip [16]byte, port uint16) (VLSH, error) {
	handle, immediate, err := d.connectTCP6Start(ip, port)
	return d.finishLegacyConnect(handle, immediate, err, 30*time.Second)
}

func (d *mode2Dispatcher) finishLegacyConnect(handle VLSH, immediate bool, err error, timeout time.Duration) (VLSH, error) {
	if err != nil || immediate {
		return handle, err
	}
	timedOut := make(chan struct{})
	timer := time.AfterFunc(timeout, func() { close(timedOut) })
	defer timer.Stop()
	if d.pollWaitContext(handle, epollOut, timedOut) {
		return handle, nil
	}
	d.closeVLSH(handle)
	if d.stopping.Load() {
		return invalidVLSH, ErrClosed
	}
	return invalidVLSH, vppErr("connect_timeout", -int(syscall.ETIMEDOUT))
}

func (d *mode2Dispatcher) accept(listener VLSH) (VLSH, [4]byte, uint16, error) {
	handle, info, err := d.acceptFull(listener)
	var ip [4]byte
	copy(ip[:], info.IP[:4])
	return handle, ip, info.Port, err
}

func (d *mode2Dispatcher) acceptFull(listener VLSH) (VLSH, AddrInfo, error) {
	return d.acceptFullContext(listener, nil)
}

func (d *mode2Dispatcher) acceptFullContext(listener VLSH, done <-chan struct{}) (VLSH, AddrInfo, error) {
	for {
		ref, err := d.lookup(listener)
		if err != nil {
			return invalidVLSH, AddrInfo{}, err
		}
		value, err := d.submit(ref.worker, func(w *worker) (any, error) {
			rawListener, err := w.ownedRaw(listener, ref.raw)
			if err != nil {
				return nil, err
			}
			rawConn, addr, err := rawAcceptFull(rawListener)
			if err != nil {
				return nil, err
			}
			public, err := d.registerSession(w, rawConn)
			if err != nil {
				return nil, err
			}
			return acceptReply{handle: public, addr: addr}, nil
		})
		if err == nil {
			reply := value.(acceptReply)
			return reply.handle, reply.addr, nil
		}
		if !isAgainError(err) {
			return invalidVLSH, AddrInfo{}, err
		}
		if !d.pollWaitContext(listener, epollIn, done) {
			return invalidVLSH, AddrInfo{}, ErrClosed
		}
	}
}

func (d *mode2Dispatcher) read(handle VLSH, p []byte) (int, error) {
	return d.readContext(handle, p, nil)
}

func (d *mode2Dispatcher) readContext(handle VLSH, p []byte, done <-chan struct{}) (int, error) {
	for {
		value, err := d.sessionCall(handle, func(_ *worker, raw VLSH) (any, error) {
			n, err := rawRead(raw, p)
			return n, err
		})
		if err == nil {
			return value.(int), nil
		}
		if !isAgainError(err) {
			return 0, err
		}
		if !d.pollWaitContext(handle, epollIn, done) {
			return 0, d.waitInterruptedError(handle, done)
		}
	}
}

func (d *mode2Dispatcher) write(handle VLSH, p []byte) (int, error) {
	return d.writeContext(handle, p, nil)
}

func (d *mode2Dispatcher) writeContext(handle VLSH, p []byte, done <-chan struct{}) (int, error) {
	for {
		value, err := d.sessionCall(handle, func(_ *worker, raw VLSH) (any, error) {
			n, err := rawWrite(raw, p)
			return n, err
		})
		if err == nil {
			return value.(int), nil
		}
		if !isAgainError(err) {
			return 0, err
		}
		if !d.pollWaitContext(handle, epollOut, done) {
			return 0, d.waitInterruptedError(handle, done)
		}
	}
}

func (d *mode2Dispatcher) close(handle VLSH) error {
	ref, err := d.lookup(handle)
	if err != nil {
		return err
	}
	_, err = d.submit(ref.worker, func(w *worker) (any, error) {
		raw, err := w.ownedRaw(handle, ref.raw)
		if err != nil {
			return nil, err
		}
		w.removeWaiters(handle, raw)
		closeErr := rawClose(raw)
		d.unregisterSession(w, handle)
		return nil, closeErr
	})
	return err
}

func (d *mode2Dispatcher) closeVLSH(handle VLSH) { _ = d.close(handle) }

func (d *mode2Dispatcher) getLocalAddr(handle VLSH) (AddrInfo, error) {
	value, err := d.sessionCall(handle, func(_ *worker, raw VLSH) (any, error) {
		return mode3GetLocalAddr(raw)
	})
	if err != nil {
		return AddrInfo{}, err
	}
	return value.(AddrInfo), nil
}

func (d *mode2Dispatcher) getPeerAddr(handle VLSH) (AddrInfo, error) {
	value, err := d.sessionCall(handle, func(_ *worker, raw VLSH) (any, error) {
		return mode3GetPeerAddr(raw)
	})
	if err != nil {
		return AddrInfo{}, err
	}
	return value.(AddrInfo), nil
}

func (d *mode2Dispatcher) setV6Only(handle VLSH, value bool) error {
	_, err := d.sessionCall(handle, func(_ *worker, raw VLSH) (any, error) {
		return nil, mode3SetV6Only(raw, value)
	})
	return err
}

func (d *mode2Dispatcher) bindUDP4(_ [4]byte, _ uint16) (VLSH, error) {
	return invalidVLSH, ErrMode2UDPUnsupported
}

func (d *mode2Dispatcher) bindUDP6(_ [16]byte, _ uint16) (VLSH, error) {
	return invalidVLSH, ErrMode2UDPUnsupported
}

func (d *mode2Dispatcher) connectUDP4Start(_ [4]byte, _ uint16) (VLSH, bool, error) {
	return invalidVLSH, false, ErrMode2UDPUnsupported
}

func (d *mode2Dispatcher) connectUDP6Start(_ [16]byte, _ uint16) (VLSH, bool, error) {
	return invalidVLSH, false, ErrMode2UDPUnsupported
}

func (d *mode2Dispatcher) connectUDP4(_ [4]byte, _ uint16) (VLSH, error) {
	return invalidVLSH, ErrMode2UDPUnsupported
}

func (d *mode2Dispatcher) connectUDP6(_ [16]byte, _ uint16) (VLSH, error) {
	return invalidVLSH, ErrMode2UDPUnsupported
}

func (d *mode2Dispatcher) sendTo(_ VLSH, _ []byte, _ AddrInfo) (int, error) {
	return 0, ErrMode2UDPUnsupported
}

func (d *mode2Dispatcher) sendToContext(_ VLSH, _ []byte, _ AddrInfo, _ <-chan struct{}) (int, error) {
	return 0, ErrMode2UDPUnsupported
}

func (d *mode2Dispatcher) recvFrom(_ VLSH, _ []byte) (int, AddrInfo, error) {
	return 0, AddrInfo{}, ErrMode2UDPUnsupported
}

func (d *mode2Dispatcher) recvFromContext(_ VLSH, _ []byte, _ <-chan struct{}) (int, AddrInfo, error) {
	return 0, AddrInfo{}, ErrMode2UDPUnsupported
}

func (d *mode2Dispatcher) pollWaitContext(handle VLSH, events uint32, done <-chan struct{}) bool {
	ref, err := d.lookup(handle)
	if err != nil {
		return false
	}
	waiter := &waiter{vlsh: handle, events: events, ready: make(chan struct{})}
	_, err = d.submit(ref.worker, func(w *worker) (any, error) {
		return nil, w.addWaiter(handle, ref.raw, waiter)
	})
	if err != nil {
		return false
	}

	select {
	case <-waiter.ready:
		return true
	case <-done:
		value, cancelErr := d.submit(ref.worker, func(w *worker) (any, error) {
			return w.cancelWaiter(handle, ref.raw, waiter), nil
		})
		if cancelErr != nil {
			select {
			case <-waiter.ready:
				return true
			default:
				return false
			}
		}
		return value.(bool)
	case <-ref.worker.quiesced:
		return false
	}
}

func (d *mode2Dispatcher) waitInterruptedError(handle VLSH, done <-chan struct{}) error {
	select {
	case <-done:
		return ErrWaitCanceled
	default:
	}
	if _, err := d.lookup(handle); err != nil || d.stopping.Load() {
		return ErrClosed
	}
	return ErrWaitCanceled
}

func isAgainError(err error) bool {
	return errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK)
}
