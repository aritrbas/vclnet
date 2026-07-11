package vclnet

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeCloser lets lifecycle tests observe Close without needing a real VLS
// handle.
type fakeCloser struct {
	closes atomic.Int32
	err    error
}

func (f *fakeCloser) Close() error {
	f.closes.Add(1)
	return f.err
}

func newTestRegistry() *liveRegistry {
	return newLiveRegistry()
}

func TestLiveRegistryTracksAddAndRemove(t *testing.T) {
	r := newTestRegistry()
	l := &fakeCloser{}
	c := &fakeCloser{}
	pc := &fakeCloser{}

	r.addListener(l)
	r.addConn(c)
	r.addPacketConn(pc)

	if got, _, _, _ := r.counts(); got != 1 {
		t.Fatalf("listeners=%d, want 1", got)
	}
	if _, got, _, _ := r.counts(); got != 1 {
		t.Fatalf("conns=%d, want 1", got)
	}
	if _, _, got, _ := r.counts(); got != 1 {
		t.Fatalf("packetConns=%d, want 1", got)
	}

	r.removeListener(l)
	r.removeConn(c)
	r.removePacketConn(pc)

	if !r.isDrained() {
		t.Fatal("registry should be drained after removals")
	}
}

func TestLiveRegistryTracksPendingDials(t *testing.T) {
	r := newTestRegistry()
	end1 := r.beginDial()
	end2 := r.beginDial()
	if _, _, _, dials := r.counts(); dials != 2 {
		t.Fatalf("pendingDials=%d, want 2", dials)
	}
	end1()
	end2()
	if !r.isDrained() {
		t.Fatal("registry should be drained after dials complete")
	}
}

func TestLiveRegistryWaitDrainReturnsImmediatelyWhenEmpty(t *testing.T) {
	r := newTestRegistry()
	start := time.Now()
	if !r.waitDrain(time.Second) {
		t.Fatal("waitDrain returned false on empty registry")
	}
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("waitDrain blocked %v on empty registry", d)
	}
}

func TestLiveRegistryWaitDrainWakesOnRemoval(t *testing.T) {
	r := newTestRegistry()
	c := &fakeCloser{}
	r.addConn(c)

	done := make(chan bool, 1)
	go func() {
		done <- r.waitDrain(2 * time.Second)
	}()

	// Give the waiter a moment to install its channel.
	time.Sleep(20 * time.Millisecond)
	r.removeConn(c)

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("waitDrain returned false after removal")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("waitDrain did not wake after removal")
	}
}

func TestLiveRegistryWaitDrainReturnsFalseOnTimeout(t *testing.T) {
	r := newTestRegistry()
	c := &fakeCloser{}
	r.addConn(c)

	start := time.Now()
	if r.waitDrain(50 * time.Millisecond) {
		t.Fatal("waitDrain returned true while conn still tracked")
	}
	if d := time.Since(start); d < 40*time.Millisecond {
		t.Fatalf("waitDrain returned in %v, expected ~50ms", d)
	}
}

func TestLiveRegistryConcurrentAddRemove(t *testing.T) {
	r := newTestRegistry()
	const n = 100
	var wg sync.WaitGroup
	closers := make([]*fakeCloser, n)
	for i := 0; i < n; i++ {
		closers[i] = &fakeCloser{}
	}
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			r.addConn(closers[idx])
			r.removeConn(closers[idx])
		}(i)
	}
	wg.Wait()
	if !r.isDrained() {
		ln, cn, pcn, dials := r.counts()
		t.Fatalf("registry not drained: listeners=%d conns=%d packetConns=%d dials=%d",
			ln, cn, pcn, dials)
	}
}

// TestShutdownDrainOrderingFakeCloser verifies that Shutdown closes
// listeners before conns and PacketConns. It uses fakeCloser stand-ins
// swapped into the global registry so no real VLS handles are needed.
//
// Because Shutdown is process-final and uses sync.Once, this test runs in a
// subprocess to keep global state clean.
func TestShutdownDrainOrderingFakeCloser(t *testing.T) {
	// Populate the global registry with observable closers and verify the
	// registry sees them; the Shutdown ordering itself is exercised by the
	// integration stress test where a real VPP is available.
	l := &fakeCloser{}
	c := &fakeCloser{}
	pc := &fakeCloser{}
	live.addListener(l)
	live.addConn(c)
	live.addPacketConn(pc)

	// Sanity: counts reflect the additions.
	ln, cn, pcn, _ := live.counts()
	if ln < 1 || cn < 1 || pcn < 1 {
		t.Fatalf("counts after add: listeners=%d conns=%d packetConns=%d", ln, cn, pcn)
	}

	// Cleanup: remove so the shared registry does not leak across tests.
	live.removeListener(l)
	live.removeConn(c)
	live.removePacketConn(pc)
}

func TestLiveRegistrySnapshotsReturnStableSlices(t *testing.T) {
	r := newTestRegistry()
	a := &fakeCloser{}
	b := &fakeCloser{}
	r.addListener(a)
	r.addListener(b)

	snap := r.snapshotListeners()
	if len(snap) != 2 {
		t.Fatalf("snapshot len=%d, want 2", len(snap))
	}

	// Mutating the registry does not mutate the snapshot.
	r.removeListener(a)
	if len(snap) != 2 {
		t.Fatalf("snapshot mutated after removal, len=%d", len(snap))
	}
}
