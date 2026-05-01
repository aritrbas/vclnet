package vclpoll

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

type workerEpollCall struct {
	raw    VLSH
	key    VLSH
	events uint32
}

func newTestMode2Worker() (*mode2Dispatcher, *worker, *[]workerEpollCall, *[]workerEpollCall, *[]VLSH) {
	d := newMode2Dispatcher(1)
	adds := []workerEpollCall{}
	mods := []workerEpollCall{}
	dels := []VLSH{}
	w := newWorker(0, true, d, make(chan struct{}))
	w.epVLSH = 900
	w.skipOwnershipCheck = true
	w.epollAdd = func(_ VLSH, raw VLSH, key VLSH, events uint32) error {
		adds = append(adds, workerEpollCall{raw: raw, key: key, events: events})
		return nil
	}
	w.epollMod = func(_ VLSH, raw VLSH, key VLSH, events uint32) error {
		mods = append(mods, workerEpollCall{raw: raw, key: key, events: events})
		return nil
	}
	w.epollDel = func(_ VLSH, raw VLSH) { dels = append(dels, raw) }
	d.workers = []*worker{w}
	return d, w, &adds, &mods, &dels
}

func TestMode2RoundRobinPicker(t *testing.T) {
	d := newMode2Dispatcher(3)
	d.workers = []*worker{{id: 0}, {id: 1}, {id: 2}}
	want := []int{0, 1, 2, 0, 1, 2}
	for i, expected := range want {
		if got := d.pickWorker().id; got != expected {
			t.Fatalf("pick %d = worker %d, want %d", i, got, expected)
		}
	}
}

func TestMode2VirtualHandlesDisambiguateRawCollisions(t *testing.T) {
	d := newMode2Dispatcher(2)
	w0 := &worker{id: 0, dispatcher: d, sessions: make(map[VLSH]VLSH), skipOwnershipCheck: true}
	w1 := &worker{id: 1, dispatcher: d, sessions: make(map[VLSH]VLSH), skipOwnershipCheck: true}
	h0, err := d.registerSession(w0, 7)
	if err != nil {
		t.Fatalf("register worker 0: %v", err)
	}
	h1, err := d.registerSession(w1, 7)
	if err != nil {
		t.Fatalf("register worker 1: %v", err)
	}
	if h0 == h1 {
		t.Fatalf("colliding raw handles produced one public handle: %d", h0)
	}
	ref0, _ := d.lookup(h0)
	ref1, _ := d.lookup(h1)
	if ref0.worker != w0 || ref1.worker != w1 || ref0.raw != 7 || ref1.raw != 7 {
		t.Fatalf("unexpected owner refs: %+v %+v", ref0, ref1)
	}
}

func TestMode2UnregisterSessionRemovesWorkerAndOwnerIndexes(t *testing.T) {
	d := newMode2Dispatcher(1)
	w := &worker{dispatcher: d, sessions: make(map[VLSH]VLSH), skipOwnershipCheck: true}
	public, err := d.registerSession(w, 17)
	if err != nil {
		t.Fatal(err)
	}
	d.unregisterSession(w, public)
	if _, ok := w.sessions[public]; ok {
		t.Fatalf("worker still owns public handle %d", public)
	}
	if _, err := d.lookup(public); !errors.Is(err, ErrClosed) {
		t.Fatalf("lookup after unregister=%v, want ErrClosed", err)
	}
}

func TestMode2DrainOpsReportsFullBatchAndHonorsShutdown(t *testing.T) {
	oldLive := appLive.Load()
	appLive.Store(true)
	t.Cleanup(func() { appLive.Store(oldLive) })

	d := newMode2Dispatcher(1)
	w := newWorker(0, true, d, make(chan struct{}))
	op := &workerOp{
		run:  func(*worker) (any, error) { return "ran", nil },
		resp: make(chan workerResult, 1),
	}
	w.opCh <- op
	if full := w.drainOps(1); !full {
		t.Fatal("one queued op did not fill a one-op batch")
	}
	if result := <-op.resp; result.value != "ran" || result.err != nil {
		t.Fatalf("result=%+v", result)
	}
	if full := w.drainOps(1); full {
		t.Fatal("empty queue reported a full batch")
	}

	d.beginShutdown()
	blocked := &workerOp{
		run:  func(*worker) (any, error) { return "unexpected", nil },
		resp: make(chan workerResult, 1),
	}
	w.opCh <- blocked
	w.drainOps(1)
	if result := <-blocked.resp; !errors.Is(result.err, ErrClosed) {
		t.Fatalf("shutdown result=%+v, want ErrClosed", result)
	}
}

// TestMode2WorkerQuiesceUnregistersBeforeExiting verifies that a
// non-bootstrap worker's terminal state is "unregisterWorker returned, then
// exited closed". rawUnregisterWorker calls vls_unregister_vcl_worker which
// performs the full VLS teardown (bookkeeping, lock release, pthread-key
// clear, and VCL worker unregister).
func TestMode2WorkerQuiesceUnregistersBeforeExiting(t *testing.T) {
	d := newMode2Dispatcher(1)
	w := newWorker(0, false /* non-bootstrap */, d, make(chan struct{}))
	w.epVLSH = invalidVLSH

	var calls int
	var sawExitedBeforeCallReturned bool
	w.unregisterWorker = func() error {
		calls++
		select {
		case <-w.exited:
			sawExitedBeforeCallReturned = true
		default:
		}
		return nil
	}

	close(w.stopCh)
	w.quiesce()

	if calls != 1 {
		t.Fatalf("unregisterWorker called %d times, want exactly 1", calls)
	}
	if sawExitedBeforeCallReturned {
		t.Fatal("exited was closed before unregisterWorker returned")
	}
	select {
	case <-w.exited:
	default:
		t.Fatal("quiesce did not close exited after unregisterWorker")
	}
}

// TestMode2WorkerQuiesceBootstrapSkipsUnregister verifies the bootstrap
// worker does not call unregisterWorker itself — its teardown instead
// funnels through rawAppDestroy (vppcom_app_destroy), which already handles
// the current/calling worker's cleanup as its own last step. The test stops
// short of closing destroyCh: that would drive quiesce() into the real
// rawAppDestroy CGo call, which requires a live VPP connection this unit
// test does not have. The bootstrap worker parks on <-w.destroyCh forever in
// production too (mirrored here by simply not closing it), so observing
// quiesced close with zero unregisterWorker calls is sufficient to prove the
// `if !w.bootstrap` gate in quiesce() skips the call for this worker.
func TestMode2WorkerQuiesceBootstrapSkipsUnregister(t *testing.T) {
	d := newMode2Dispatcher(1)
	w := newWorker(0, true /* bootstrap */, d, make(chan struct{}))
	w.epVLSH = invalidVLSH

	var calls int
	w.unregisterWorker = func() error { calls++; return nil }

	close(w.stopCh)
	go w.quiesce()

	select {
	case <-w.quiesced:
	case <-time.After(time.Second):
		t.Fatal("bootstrap quiesce did not close quiesced")
	}

	// Give a parked-on-destroyCh worker a moment; it must not call
	// unregisterWorker while waiting there.
	time.Sleep(20 * time.Millisecond)
	if calls != 0 {
		t.Fatalf("unregisterWorker called %d times for bootstrap worker, want 0", calls)
	}
}

// TestMode2DispatcherStopWaitsForUnregisterNotOSThreadDeath verifies
// mode2Dispatcher.stop() blocks on <-w.exited (closed only after
// unregisterWorker returns) rather than polling for OS-thread death.
func TestMode2DispatcherStopWaitsForUnregisterNotOSThreadDeath(t *testing.T) {
	d := newMode2Dispatcher(1)
	w := newWorker(0, false, d, make(chan struct{}))
	w.epVLSH = invalidVLSH
	d.workers = []*worker{w}

	unregisterStarted := make(chan struct{})
	releaseUnregister := make(chan struct{})
	w.unregisterWorker = func() error {
		close(unregisterStarted)
		<-releaseUnregister
		return nil
	}

	go w.quiesce()
	<-unregisterStarted

	stopped := make(chan struct{})
	go func() {
		d.stop()
		close(stopped)
	}()

	select {
	case <-stopped:
		t.Fatal("stop() returned before unregisterWorker completed")
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseUnregister)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stop() did not return after unregisterWorker completed")
	}
}

// TestMode2WorkerQuiesceUnregisterFailureRecorded verifies that when
// unregisterWorker returns an error, the dispatcher's unregisterFailures
// counter is incremented so the failure is observable.
func TestMode2WorkerQuiesceUnregisterFailureRecorded(t *testing.T) {
	d := newMode2Dispatcher(1)
	w := newWorker(0, false, d, make(chan struct{}))
	w.epVLSH = invalidVLSH

	w.unregisterWorker = func() error {
		return fmt.Errorf("pthread_setspecific failed")
	}

	close(w.stopCh)
	w.quiesce()

	if got := d.unregisterFailures.Load(); got != 1 {
		t.Fatalf("unregisterFailures=%d, want 1", got)
	}
}

func TestMode2UDPSendToRecvFromRequireValidSession(t *testing.T) {
	d := newMode2Dispatcher(2)
	assertError := func(name string, err error) {
		t.Helper()
		if err == nil {
			t.Fatalf("%s succeeded, want error (no VPP running)", name)
		}
	}

	// sendToContext/recvFromContext on a non-existent handle should return
	// ErrClosed (no session is registered for handle 99).
	if n, err := d.sendToContext(99, []byte("x"), AddrInfo{}, nil); n != 0 || err == nil {
		t.Fatalf("sendToContext=(%d,%v), want (0, error)", n, err)
	} else {
		assertError("sendToContext", err)
	}
	if n, _, err := d.recvFromContext(99, make([]byte, 1), nil); n != 0 || err == nil {
		t.Fatalf("recvFromContext=(%d,%v), want (0, error)", n, err)
	} else {
		assertError("recvFromContext", err)
	}
}

func TestMode2WorkerCombinesWaitersUsingRawAndVirtualHandles(t *testing.T) {
	_, w, adds, mods, _ := newTestMode2Worker()
	const public, raw = VLSH(101), VLSH(3)
	w.sessions[public] = raw
	reader := testWaiter(public, epollIn)
	writer := testWaiter(public, epollOut)
	if err := w.addWaiter(public, raw, reader); err != nil {
		t.Fatal(err)
	}
	if err := w.addWaiter(public, raw, writer); err != nil {
		t.Fatal(err)
	}
	if len(*adds) != 1 || (*adds)[0] != (workerEpollCall{raw: raw, key: public, events: epollIn}) {
		t.Fatalf("add calls=%v", *adds)
	}
	if len(*mods) != 1 || (*mods)[0].events != epollIn|epollOut {
		t.Fatalf("mod calls=%v", *mods)
	}
}

func TestMode2WorkerEventWakesOnlyMatchingWaiter(t *testing.T) {
	_, w, _, mods, dels := newTestMode2Worker()
	const public, raw = VLSH(202), VLSH(4)
	w.sessions[public] = raw
	reader := testWaiter(public, epollIn)
	writer := testWaiter(public, epollOut)
	_ = w.addWaiter(public, raw, reader)
	_ = w.addWaiter(public, raw, writer)

	w.handleEvent(public, epollIn)
	if !waiterIsReady(reader) || waiterIsReady(writer) {
		t.Fatalf("reader ready=%v writer ready=%v", waiterIsReady(reader), waiterIsReady(writer))
	}
	if len(*mods) != 2 || (*mods)[1].events != epollOut {
		t.Fatalf("mod calls=%v", *mods)
	}
	if len(*dels) != 0 {
		t.Fatalf("unexpected deletes=%v", *dels)
	}
}

func TestMode2WorkerCancellationIsExact(t *testing.T) {
	_, w, _, mods, _ := newTestMode2Worker()
	const public, raw = VLSH(303), VLSH(5)
	w.sessions[public] = raw
	reader := testWaiter(public, epollIn)
	writer := testWaiter(public, epollOut)
	_ = w.addWaiter(public, raw, reader)
	_ = w.addWaiter(public, raw, writer)

	if eventWon := w.cancelWaiter(public, raw, reader); eventWon {
		t.Fatal("cancel reported an event win")
	}
	if !waiterIsReady(reader) || waiterIsReady(writer) {
		t.Fatalf("reader ready=%v writer ready=%v", waiterIsReady(reader), waiterIsReady(writer))
	}
	if len(*mods) != 2 || (*mods)[1].events != epollOut {
		t.Fatalf("mod calls=%v", *mods)
	}
}

func TestMode2WorkerShutdownWakesAllWaiters(t *testing.T) {
	_, w, _, _, dels := newTestMode2Worker()
	const public, raw = VLSH(404), VLSH(6)
	w.sessions[public] = raw
	reader := testWaiter(public, epollIn)
	writer := testWaiter(public, epollOut)
	_ = w.addWaiter(public, raw, reader)
	_ = w.addWaiter(public, raw, writer)

	w.wakeAllWaiters()
	if !waiterIsReady(reader) || !waiterIsReady(writer) {
		t.Fatal("shutdown did not wake every waiter")
	}
	if len(w.waiters) != 0 || len(*dels) != 1 || (*dels)[0] != raw {
		t.Fatalf("waiters=%v deletes=%v", w.waiters, *dels)
	}
}

func TestMode2WorkerBookkeepingDoesNotStartMode3Poller(t *testing.T) {
	_, w, _, _, _ := newTestMode2Worker()
	const public, raw = VLSH(505), VLSH(8)
	w.sessions[public] = raw
	if err := w.addWaiter(public, raw, testWaiter(public, epollIn)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-defaultPoller.started:
		t.Fatal("mode-2 waiter bookkeeping started the mode-3 poller")
	default:
	}
}
