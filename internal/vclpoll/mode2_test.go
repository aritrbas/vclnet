package vclpoll

import (
	"errors"
	"os"
	"syscall"
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

func TestMode2WaitThreadGoneSkipsProcessMainThread(t *testing.T) {
	done := make(chan struct{})
	go func() {
		waitThreadGone(os.Getpid())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("waitThreadGone waited for the non-terminating process main thread")
	}
}

func TestMode2UDPIsRejectedBeforeSessionCreation(t *testing.T) {
	d := newMode2Dispatcher(2)
	assertUnsupported := func(name string, err error) {
		t.Helper()
		if !errors.Is(err, ErrMode2UDPUnsupported) || !errors.Is(err, syscall.EOPNOTSUPP) {
			t.Fatalf("%s error=%v, want ErrMode2UDPUnsupported wrapping EOPNOTSUPP", name, err)
		}
	}

	if handle, err := d.bindUDP4([4]byte{}, 1); handle != invalidVLSH {
		t.Fatalf("bindUDP4 handle=%d, want invalid", handle)
	} else {
		assertUnsupported("bindUDP4", err)
	}
	if handle, err := d.bindUDP6([16]byte{}, 1); handle != invalidVLSH {
		t.Fatalf("bindUDP6 handle=%d, want invalid", handle)
	} else {
		assertUnsupported("bindUDP6", err)
	}
	if handle, immediate, err := d.connectUDP4Start([4]byte{}, 1); handle != invalidVLSH || immediate {
		t.Fatalf("connectUDP4Start=(%d,%v), want (invalid,false)", handle, immediate)
	} else {
		assertUnsupported("connectUDP4Start", err)
	}
	if handle, immediate, err := d.connectUDP6Start([16]byte{}, 1); handle != invalidVLSH || immediate {
		t.Fatalf("connectUDP6Start=(%d,%v), want (invalid,false)", handle, immediate)
	} else {
		assertUnsupported("connectUDP6Start", err)
	}
	if handle, err := d.connectUDP4([4]byte{}, 1); handle != invalidVLSH {
		t.Fatalf("connectUDP4 handle=%d, want invalid", handle)
	} else {
		assertUnsupported("connectUDP4", err)
	}
	if handle, err := d.connectUDP6([16]byte{}, 1); handle != invalidVLSH {
		t.Fatalf("connectUDP6 handle=%d, want invalid", handle)
	} else {
		assertUnsupported("connectUDP6", err)
	}
	if n, err := d.sendTo(99, []byte("x"), AddrInfo{}); n != 0 {
		t.Fatalf("sendTo n=%d, want 0", n)
	} else {
		assertUnsupported("sendTo", err)
	}
	if n, err := d.sendToContext(99, []byte("x"), AddrInfo{}, make(chan struct{})); n != 0 {
		t.Fatalf("sendToContext n=%d, want 0", n)
	} else {
		assertUnsupported("sendToContext", err)
	}
	if n, _, err := d.recvFrom(99, make([]byte, 1)); n != 0 {
		t.Fatalf("recvFrom n=%d, want 0", n)
	} else {
		assertUnsupported("recvFrom", err)
	}
	if n, _, err := d.recvFromContext(99, make([]byte, 1), make(chan struct{})); n != 0 {
		t.Fatalf("recvFromContext n=%d, want 0", n)
	} else {
		assertUnsupported("recvFromContext", err)
	}
	if got := d.nextHandle.Load(); got != 0 {
		t.Fatalf("unsupported UDP allocated %d virtual handles", got)
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
