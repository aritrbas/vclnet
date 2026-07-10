package vclpoll

import (
	"fmt"
	"runtime"
	"syscall"
	"time"
)

const workerPollIntervalSeconds = 0.01
const workerShutdownDrain = 100 * time.Millisecond

type workerResult struct {
	value any
	err   error
}

type workerOp struct {
	run  func(*worker) (any, error)
	resp chan workerResult
}

// worker owns one VCL worker, one VLS epoll handle, and every raw VLS handle
// in sessions. Its goroutine remains pinned to one OS thread for its lifetime.
type worker struct {
	id         int
	tid        int
	bootstrap  bool
	vclIndex   uint32
	epVLSH     VLSH
	dispatcher *mode2Dispatcher

	opCh      chan *workerOp
	startCh   <-chan struct{}
	stopCh    chan struct{}
	destroyCh chan struct{}

	registered chan error
	ready      chan error
	quiesced   chan struct{}
	destroyed  chan struct{}
	exited     chan struct{}

	sessions map[VLSH]VLSH // public virtual handle -> worker-local raw VLSH
	waiters  map[VLSH]*waitSet

	// Tests inject epoll operations and disable raw ownership inspection.
	epollAdd           func(VLSH, VLSH, VLSH, uint32) error
	epollMod           func(VLSH, VLSH, VLSH, uint32) error
	epollDel           func(VLSH, VLSH)
	epollWait          func(VLSH, []pollEvent, float64) int
	skipOwnershipCheck bool
}

func newWorker(id int, bootstrap bool, d *mode2Dispatcher, start <-chan struct{}) *worker {
	return &worker{
		id:         id,
		bootstrap:  bootstrap,
		epVLSH:     invalidVLSH,
		dispatcher: d,
		opCh:       make(chan *workerOp, 256),
		startCh:    start,
		stopCh:     make(chan struct{}),
		destroyCh:  make(chan struct{}),
		registered: make(chan error, 1),
		ready:      make(chan error, 1),
		quiesced:   make(chan struct{}),
		destroyed:  make(chan struct{}),
		exited:     make(chan struct{}),
		sessions:   make(map[VLSH]VLSH),
		waiters:    make(map[VLSH]*waitSet),
	}
}

func (w *worker) loop(appName string) {
	runtime.LockOSThread()
	w.tid = syscall.Gettid()

	var err error
	if w.bootstrap {
		err = rawAppCreate(appName)
		if err == nil && !rawMode2Enabled() {
			err = fmt.Errorf("vclpoll: mode 2 requires multi-thread-workers in VCL_CONFIG")
			rawAppDestroy()
		}
	} else {
		err = rawRegisterWorker()
	}
	if err == nil {
		markCurrentThreadRegistered()
		w.vclIndex = rawWorkerIndex()
	}
	w.registered <- err
	if err != nil {
		close(w.exited)
		return
	}

	select {
	case <-w.startCh:
	case <-w.stopCh:
		w.quiesce()
		return
	}

	w.epVLSH, err = rawEpollCreate()
	w.ready <- err
	if err != nil {
		w.quiesce()
		return
	}

	events := make([]pollEvent, 64)
	for {
		select {
		case <-w.stopCh:
			w.quiesce()
			return
		default:
		}

		timeout := workerPollIntervalSeconds
		if w.drainOps(64) {
			// The op batch filled, so more work may already be queued. Still
			// service VLS readiness, but do not sleep before the next batch.
			timeout = 0
		}
		n := w.wait(w.epVLSH, events, timeout)
		for i := 0; i < n; i++ {
			w.handleEvent(events[i].Vlsh, events[i].Events)
		}
	}
}

func (w *worker) drainOps(limit int) bool {
	for i := 0; i < limit; i++ {
		select {
		case op := <-w.opCh:
			if w.dispatcher.stopping.Load() || !appLive.Load() {
				op.resp <- workerResult{err: ErrClosed}
				continue
			}
			value, err := op.run(w)
			op.resp <- workerResult{value: value, err: err}
		default:
			return false
		}
	}
	return true
}

func (w *worker) quiesce() {
	w.wakeAllWaiters()
	for public, raw := range w.sessions {
		_ = rawClose(raw)
		w.dispatcher.unregisterSession(w, public)
	}
	w.drainCloseNotifications()
	if w.epVLSH >= 0 {
		_ = rawClose(w.epVLSH)
		w.epVLSH = invalidVLSH
	}
	for {
		select {
		case op := <-w.opCh:
			op.resp <- workerResult{err: ErrClosed}
		default:
			close(w.quiesced)
			if !w.bootstrap {
				close(w.exited)
				return
			}
			<-w.destroyCh
			rawAppDestroy()
			close(w.destroyed)
			// vppcom_app_destroy invalidates the pthread-specific VLS state.
			// Keep the bootstrap M parked so its VLS destructor cannot run
			// against already-destroyed process state.
			select {}
		}
	}
}

// drainCloseNotifications gives VPP a bounded interval to consume disconnect
// events before worker unregister/app teardown releases session state. All
// workers drain concurrently, so this adds one interval per process shutdown.
func (w *worker) drainCloseNotifications() {
	if w.epVLSH < 0 || !appCreated.Load() {
		return
	}
	deadline := time.Now().Add(workerShutdownDrain)
	events := make([]pollEvent, 64)
	for time.Now().Before(deadline) {
		_ = w.wait(w.epVLSH, events, workerPollIntervalSeconds)
	}
}

func (w *worker) checkOwnership(raw VLSH) error {
	if w.skipOwnershipCheck {
		return nil
	}
	owner, ok := rawSessionWorker(raw)
	if ok && owner == w.vclIndex {
		return nil
	}
	w.dispatcher.ownershipViolations.Add(1)
	return fmt.Errorf("vclpoll: raw session %d belongs to VCL worker %d, dispatched to %d", raw, owner, w.vclIndex)
}

func (w *worker) ownedRaw(public, expected VLSH) (VLSH, error) {
	raw, ok := w.sessions[public]
	if !ok || raw != expected {
		return invalidVLSH, ErrClosed
	}
	if err := w.checkOwnership(raw); err != nil {
		return invalidVLSH, err
	}
	return raw, nil
}

func (w *worker) addWaiter(public, raw VLSH, wtr *waiter) error {
	if _, err := w.ownedRaw(public, raw); err != nil {
		return err
	}
	set, exists := w.waiters[public]
	if !exists {
		if err := w.add(w.epVLSH, raw, public, wtr.events); err != nil {
			return err
		}
		w.waiters[public] = &waitSet{
			waiters: map[*waiter]struct{}{wtr: {}},
			events:  wtr.events,
		}
		return nil
	}

	newEvents := set.events | wtr.events
	if newEvents != set.events {
		if err := w.mod(w.epVLSH, raw, public, newEvents); err != nil {
			return err
		}
	}
	set.waiters[wtr] = struct{}{}
	set.events = newEvents
	return nil
}

func (w *worker) cancelWaiter(public, raw VLSH, wtr *waiter) bool {
	set, ok := w.waiters[public]
	if !ok {
		return waiterIsReady(wtr)
	}
	if _, ok := set.waiters[wtr]; !ok {
		return waiterIsReady(wtr)
	}

	oldEvents := set.events
	delete(set.waiters, wtr)
	wakeWaiter(wtr)
	if len(set.waiters) == 0 {
		w.del(w.epVLSH, raw)
		delete(w.waiters, public)
		return false
	}

	set.events = combinedEvents(set)
	if set.events != oldEvents {
		if err := w.mod(w.epVLSH, raw, public, set.events); err != nil {
			w.dropWaitSet(public, raw, set)
		}
	}
	return false
}

func waiterIsReady(wtr *waiter) bool {
	select {
	case <-wtr.ready:
		return true
	default:
		return false
	}
}

func (w *worker) removeWaiters(public, raw VLSH) {
	if set, ok := w.waiters[public]; ok {
		w.del(w.epVLSH, raw)
		delete(w.waiters, public)
		wakeWaitSet(set)
	}
}

func (w *worker) handleEvent(public VLSH, events uint32) {
	set, ok := w.waiters[public]
	if !ok {
		return
	}
	raw, sessionExists := w.sessions[public]
	oldEvents := set.events
	terminal := events&(epollErr|epollHup|epollRDHup) != 0
	for waiter := range set.waiters {
		if terminal || events&waiter.events != 0 {
			delete(set.waiters, waiter)
			wakeWaiter(waiter)
		}
	}
	if len(set.waiters) == 0 || !sessionExists {
		if sessionExists {
			w.del(w.epVLSH, raw)
		}
		delete(w.waiters, public)
		return
	}
	set.events = combinedEvents(set)
	if set.events != oldEvents {
		if err := w.mod(w.epVLSH, raw, public, set.events); err != nil {
			w.dropWaitSet(public, raw, set)
		}
	}
}

func (w *worker) wakeAllWaiters() {
	for public, set := range w.waiters {
		if raw, ok := w.sessions[public]; ok && w.epVLSH >= 0 {
			w.del(w.epVLSH, raw)
		}
		wakeWaitSet(set)
	}
	w.waiters = make(map[VLSH]*waitSet)
}

func (w *worker) dropWaitSet(public, raw VLSH, set *waitSet) {
	w.del(w.epVLSH, raw)
	delete(w.waiters, public)
	wakeWaitSet(set)
}

// addWaiterRaw registers a waiter keyed by a raw VLSH that is NOT tracked as
// a virtual-handle session (used by sharded listener accept loops). We store
// the raw handle in the sessions map under a negative-space key so that
// handleEvent's epoll cleanup path works correctly.
func (w *worker) addWaiterRaw(raw VLSH, wtr *waiter) error {
	key := ^raw // negative-space key to avoid collision with virtual handles
	w.sessions[key] = raw
	set, exists := w.waiters[key]
	if !exists {
		if err := w.add(w.epVLSH, raw, key, wtr.events); err != nil {
			return err
		}
		w.waiters[key] = &waitSet{
			waiters: map[*waiter]struct{}{wtr: {}},
			events:  wtr.events,
		}
		return nil
	}

	newEvents := set.events | wtr.events
	if newEvents != set.events {
		if err := w.mod(w.epVLSH, raw, key, newEvents); err != nil {
			return err
		}
	}
	set.waiters[wtr] = struct{}{}
	set.events = newEvents
	return nil
}

// cancelWaiterRaw cancels a waiter previously registered via addWaiterRaw.
func (w *worker) cancelWaiterRaw(raw VLSH, wtr *waiter) {
	key := ^raw
	set, ok := w.waiters[key]
	if !ok {
		return
	}
	if _, ok := set.waiters[wtr]; !ok {
		return
	}

	oldEvents := set.events
	delete(set.waiters, wtr)
	wakeWaiter(wtr)
	if len(set.waiters) == 0 {
		w.del(w.epVLSH, raw)
		delete(w.waiters, key)
		delete(w.sessions, key)
		return
	}

	set.events = combinedEvents(set)
	if set.events != oldEvents {
		if err := w.mod(w.epVLSH, raw, key, set.events); err != nil {
			w.dropWaitSet(key, raw, set)
		}
	}
}

// removeWaitersRaw removes all waiters for a raw listener handle and cleans
// up the sessions mapping installed by addWaiterRaw.
func (w *worker) removeWaitersRaw(raw VLSH) {
	key := ^raw
	if set, ok := w.waiters[key]; ok {
		w.del(w.epVLSH, raw)
		delete(w.waiters, key)
		wakeWaitSet(set)
	}
	delete(w.sessions, key)
}

func (w *worker) add(ep, raw, key VLSH, events uint32) error {
	if w.epollAdd != nil {
		return w.epollAdd(ep, raw, key, events)
	}
	return rawEpollAdd(ep, raw, key, events)
}

func (w *worker) mod(ep, raw, key VLSH, events uint32) error {
	if w.epollMod != nil {
		return w.epollMod(ep, raw, key, events)
	}
	return rawEpollMod(ep, raw, key, events)
}

func (w *worker) wait(ep VLSH, events []pollEvent, timeout float64) int {
	if w.epollWait != nil {
		return w.epollWait(ep, events, timeout)
	}
	return rawEpollWait(ep, events, timeout)
}

func (w *worker) del(ep, raw VLSH) {
	if w.epollDel != nil {
		w.epollDel(ep, raw)
		return
	}
	rawEpollDel(ep, raw)
}
