package vclpoll

import (
	"sync/atomic"
	"testing"
	"time"
)

func newTestMode2Workers(n int) (*mode2Dispatcher, []*worker) {
	d := newMode2Dispatcher(n)
	workers := make([]*worker, n)
	for i := 0; i < n; i++ {
		w := newWorker(i, i == 0, d, make(chan struct{}))
		w.epVLSH = VLSH(900 + i)
		w.skipOwnershipCheck = true
		w.epollAdd = func(_ VLSH, _ VLSH, _ VLSH, _ uint32) error { return nil }
		w.epollMod = func(_ VLSH, _ VLSH, _ VLSH, _ uint32) error { return nil }
		w.epollDel = func(_ VLSH, _ VLSH) {}
		w.epollWait = func(_ VLSH, _ []pollEvent, _ float64) int { return 0 }
		workers[i] = w
	}
	d.workers = workers
	return d, workers
}

// startTestWorkerLoop runs a minimal worker loop for testing purposes (no VLS
// registration). It processes ops from opCh until stopCh is closed.
func startTestWorkerLoop(w *worker) {
	go func() {
		for {
			select {
			case <-w.stopCh:
				for {
					select {
					case op := <-w.opCh:
						op.resp <- workerResult{err: ErrClosed}
					default:
						close(w.quiesced)
						return
					}
				}
			case op := <-w.opCh:
				value, err := op.run(w)
				op.resp <- workerResult{value: value, err: err}
			}
		}
	}()
}

// newTestShardedListener constructs a shardedListener for test purposes
// without starting accept loops (which require CGo). Tests can then
// exercise the fan-in/close/lookup logic by injecting results directly
// into acceptC.
func newTestShardedListener(d *mode2Dispatcher) (*shardedListener, VLSH) {
	shards := make([]listenerShard, len(d.workers))
	for i, w := range d.workers {
		shards[i] = listenerShard{worker: w, raw: VLSH(500 + i)}
	}
	id := d.nextHandle.Add(1)
	public := VLSH(id)
	sl := &shardedListener{
		d:       d,
		public:  public,
		shards:  shards,
		acceptC: make(chan shardAcceptResult, len(shards)*4),
		stopCh:  make(chan struct{}),
	}
	d.owners.Store(public, sl)
	return sl, public
}

func TestShardedListenerCreatesOneShardPerWorker(t *testing.T) {
	d, workers := newTestMode2Workers(3)
	sl, _ := newTestShardedListener(d)
	if len(sl.shards) != 3 {
		t.Fatalf("expected 3 shards, got %d", len(sl.shards))
	}
	for i, shard := range sl.shards {
		if shard.worker != workers[i] {
			t.Errorf("shard %d worker mismatch", i)
		}
	}
}

func TestShardedListenerNewShardedListenerViaSubmit(t *testing.T) {
	oldLive := appLive.Load()
	appLive.Store(true)
	t.Cleanup(func() { appLive.Store(oldLive) })

	d, workers := newTestMode2Workers(3)
	for _, w := range workers {
		startTestWorkerLoop(w)
	}
	t.Cleanup(func() {
		d.stopping.Store(true)
		for _, w := range workers {
			close(w.stopCh)
		}
		for _, w := range workers {
			<-w.quiesced
		}
	})

	// Verify that newShardedListener submits create calls to each worker.
	var createCount atomic.Int32
	vlsh, err := d.newShardedListener(func() (VLSH, error) {
		idx := createCount.Add(1)
		return VLSH(idx), nil
	})
	if err != nil {
		t.Fatalf("newShardedListener: %v", err)
	}

	if createCount.Load() != 3 {
		t.Fatalf("expected 3 listener creates, got %d", createCount.Load())
	}
	sl := d.lookupShardedListener(vlsh)
	if sl == nil {
		t.Fatal("lookupShardedListener returned nil")
	}
	if len(sl.shards) != 3 {
		t.Fatalf("expected 3 shards, got %d", len(sl.shards))
	}
	for i, shard := range sl.shards {
		if shard.worker != workers[i] {
			t.Errorf("shard %d worker mismatch", i)
		}
		if shard.raw != VLSH(i+1) {
			t.Errorf("shard %d raw=%d, want %d", i, shard.raw, i+1)
		}
	}

	// Stop the accept loops that started. In test mode without VPP, the
	// loops will get errors from rawAcceptFull and exit — we just need
	// to signal stop and wait for them.
	close(sl.stopCh)
	sl.wg.Wait()
}

func TestShardedListenerAcceptFanIn(t *testing.T) {
	oldLive := appLive.Load()
	appLive.Store(true)
	t.Cleanup(func() { appLive.Store(oldLive) })

	d, _ := newTestMode2Workers(2)
	sl, vlsh := newTestShardedListener(d)

	// Inject accepted connections directly into the fan-in channel.
	sl.acceptC <- shardAcceptResult{
		handle: VLSH(1001),
		addr:   AddrInfo{Port: 1234, IsV4: true},
	}
	sl.acceptC <- shardAcceptResult{
		handle: VLSH(1002),
		addr:   AddrInfo{Port: 5678, IsV4: true},
	}

	h1, info1, err := d.acceptFullContext(vlsh, nil)
	if err != nil {
		t.Fatalf("first accept: %v", err)
	}
	if h1 != 1001 || info1.Port != 1234 {
		t.Errorf("first accept: handle=%d port=%d", h1, info1.Port)
	}

	h2, info2, err := d.acceptFullContext(vlsh, nil)
	if err != nil {
		t.Fatalf("second accept: %v", err)
	}
	if h2 != 1002 || info2.Port != 5678 {
		t.Errorf("second accept: handle=%d port=%d", h2, info2.Port)
	}
}

func TestShardedListenerAcceptRespectsContext(t *testing.T) {
	d, _ := newTestMode2Workers(1)
	_, vlsh := newTestShardedListener(d)

	done := make(chan struct{})
	close(done)

	_, _, err := d.acceptFullContext(vlsh, done)
	if err != ErrClosed {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestShardedListenerCloseStopsAcceptLoops(t *testing.T) {
	d, _ := newTestMode2Workers(2)
	sl, vlsh := newTestShardedListener(d)

	// Start fake accept loops that just block on stopCh.
	for range sl.shards {
		sl.wg.Add(1)
		go func() {
			defer sl.wg.Done()
			<-sl.stopCh
		}()
	}

	// Close the stopCh directly to simulate closeShardedListener's first step.
	close(sl.stopCh)
	sl.wg.Wait()

	// Verify channel is drained.
	select {
	case <-sl.acceptC:
		t.Error("unexpected accept result after stop")
	default:
	}

	// Remove from owners map.
	d.owners.Delete(sl.public)
	if d.lookupShardedListener(vlsh) != nil {
		t.Error("listener still registered after cleanup")
	}

	// Accept after cleanup should fail: it goes through the non-sharded
	// fallback which calls d.lookup and gets ErrClosed.
	done := make(chan struct{})
	time.AfterFunc(50*time.Millisecond, func() { close(done) })
	_, _, err := d.acceptFullContext(vlsh, done)
	if err == nil {
		t.Error("accept after close should fail")
	}
}

func TestShardedListenerLookupDistinguishesTypes(t *testing.T) {
	d, workers := newTestMode2Workers(2)
	_, vlsh := newTestShardedListener(d)

	if d.lookupShardedListener(vlsh) == nil {
		t.Fatal("expected sharded listener lookup to succeed")
	}

	// Register a normal session.
	w := workers[0]
	normalPublic, err := d.registerSession(w, VLSH(50))
	if err != nil {
		t.Fatalf("registerSession: %v", err)
	}
	if d.lookupShardedListener(normalPublic) != nil {
		t.Error("normal session incorrectly identified as sharded listener")
	}

	// Normal lookup should work for the normal session.
	ref, err := d.lookup(normalPublic)
	if err != nil {
		t.Fatalf("lookup normal session: %v", err)
	}
	if ref.worker != w {
		t.Error("normal session lookup returned wrong worker")
	}
}

func TestShardedListenerAcceptBlocksUntilResult(t *testing.T) {
	d, _ := newTestMode2Workers(2)
	sl, vlsh := newTestShardedListener(d)

	// Accept should block until a result appears or context fires.
	resultCh := make(chan error, 1)
	go func() {
		_, _, err := d.acceptFullContext(vlsh, nil)
		resultCh <- err
	}()

	// Should not complete immediately.
	select {
	case err := <-resultCh:
		t.Fatalf("accept returned immediately with: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	// Inject a result.
	sl.acceptC <- shardAcceptResult{handle: VLSH(999), addr: AddrInfo{Port: 42}}

	select {
	case err := <-resultCh:
		if err != nil {
			t.Fatalf("accept returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("accept didn't return after injecting result")
	}
}
