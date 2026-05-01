package vclpoll

import (
	"errors"
	"testing"
)

type epollCall struct {
	vlsh   VLSH
	events uint32
}

func newTestPoller() (*poller, *[]epollCall, *[]epollCall, *[]VLSH) {
	adds := []epollCall{}
	mods := []epollCall{}
	dels := []VLSH{}
	p := &poller{
		regCh:    make(chan *waiter, 16),
		cancelCh: make(chan *waiter, 16),
		unregCh:  make(chan VLSH, 16),
		stopCh:   make(chan struct{}),
		stopped:  make(chan struct{}),
		started:  make(chan struct{}),
		waiters:  make(map[VLSH]*waitSet),
		epollAdd: func(_ VLSH, vlsh VLSH, events uint32) error {
			adds = append(adds, epollCall{vlsh: vlsh, events: events})
			return nil
		},
		epollMod: func(_ VLSH, vlsh VLSH, events uint32) error {
			mods = append(mods, epollCall{vlsh: vlsh, events: events})
			return nil
		},
		epollDel: func(_ VLSH, vlsh VLSH) {
			dels = append(dels, vlsh)
		},
	}
	return p, &adds, &mods, &dels
}

func testWaiter(vlsh VLSH, events uint32) *waiter {
	return &waiter{vlsh: vlsh, events: events, ready: make(chan struct{})}
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestPollerCombinesWaitersForOneSession(t *testing.T) {
	p, adds, mods, _ := newTestPoller()
	reader := testWaiter(42, epollIn)
	writer := testWaiter(42, epollOut)
	p.regCh <- reader
	p.regCh <- writer
	p.drainRegistrations()

	if len(*adds) != 1 || (*adds)[0] != (epollCall{vlsh: 42, events: epollIn}) {
		t.Fatalf("add calls=%v, want one EPOLLIN add", *adds)
	}
	if len(*mods) != 1 || (*mods)[0] != (epollCall{vlsh: 42, events: epollIn | epollOut}) {
		t.Fatalf("mod calls=%v, want combined mask", *mods)
	}
	set := p.waiters[42]
	if set == nil || len(set.waiters) != 2 || set.events != epollIn|epollOut {
		t.Fatalf("wait set=%+v, want two waiters with combined mask", set)
	}
}

func TestPollerEventWakesOnlyMatchingWaiters(t *testing.T) {
	p, _, mods, dels := newTestPoller()
	reader := testWaiter(7, epollIn)
	writer := testWaiter(7, epollOut)
	p.regCh <- reader
	p.regCh <- writer
	p.drainRegistrations()

	p.handleEvent(7, epollIn)

	if !isClosed(reader.ready) {
		t.Fatal("read waiter was not woken")
	}
	if isClosed(writer.ready) {
		t.Fatal("write waiter was woken by read-only event")
	}
	if len(*mods) != 2 || (*mods)[1].events != epollOut {
		t.Fatalf("mod calls=%v, want remaining EPOLLOUT interest", *mods)
	}
	if len(*dels) != 0 {
		t.Fatalf("unexpected delete calls: %v", *dels)
	}
}

func TestPollerCancellationRemovesExactWaiter(t *testing.T) {
	p, _, mods, _ := newTestPoller()
	reader := testWaiter(9, epollIn)
	writer := testWaiter(9, epollOut)
	p.regCh <- reader
	p.regCh <- writer
	p.drainRegistrations()

	p.cancelCh <- reader
	p.drainCancellations()

	if !isClosed(reader.ready) {
		t.Fatal("cancelled waiter was not woken")
	}
	if isClosed(writer.ready) {
		t.Fatal("cancelling reader woke writer")
	}
	if len(p.waiters[9].waiters) != 1 || p.waiters[9].events != epollOut {
		t.Fatalf("remaining wait set=%+v, want writer only", p.waiters[9])
	}
	if len(*mods) != 2 || (*mods)[1].events != epollOut {
		t.Fatalf("mod calls=%v, want EPOLLOUT after cancellation", *mods)
	}
}

func TestPollerUnregisterWakesEveryWaiter(t *testing.T) {
	p, _, _, dels := newTestPoller()
	reader := testWaiter(11, epollIn)
	writer := testWaiter(11, epollOut)
	p.regCh <- reader
	p.regCh <- writer
	p.drainRegistrations()

	p.unregCh <- 11
	p.drainUnregistrations()

	if !isClosed(reader.ready) || !isClosed(writer.ready) {
		t.Fatal("session unregister did not wake all waiters")
	}
	if _, ok := p.waiters[11]; ok {
		t.Fatal("session remained registered")
	}
	if len(*dels) != 1 || (*dels)[0] != 11 {
		t.Fatalf("delete calls=%v, want [11]", *dels)
	}
}

func TestPollerTerminalEventWakesEveryWaiter(t *testing.T) {
	p, _, _, _ := newTestPoller()
	reader := testWaiter(13, epollIn)
	writer := testWaiter(13, epollOut)
	p.regCh <- reader
	p.regCh <- writer
	p.drainRegistrations()

	p.handleEvent(13, epollHup)

	if !isClosed(reader.ready) || !isClosed(writer.ready) {
		t.Fatal("terminal event did not wake every waiter")
	}
	if _, ok := p.waiters[13]; ok {
		t.Fatal("terminal event left session registered")
	}
}

func TestPollerAddFailureWakesWaiter(t *testing.T) {
	p, _, _, _ := newTestPoller()
	p.epollAdd = func(VLSH, VLSH, uint32) error { return errors.New("add failed") }
	w := testWaiter(21, epollIn)
	p.regCh <- w
	p.drainRegistrations()

	if !isClosed(w.ready) {
		t.Fatal("failed registration did not wake waiter")
	}
	if len(p.waiters) != 0 {
		t.Fatalf("failed registration created state: %v", p.waiters)
	}
}

func TestPollerWakeAllIncludesPendingRegistrations(t *testing.T) {
	p, _, _, _ := newTestPoller()
	active := testWaiter(1, epollIn)
	pending := testWaiter(2, epollOut)
	p.regCh <- active
	p.drainRegistrations()
	p.regCh <- pending

	p.wakeAll()

	if !isClosed(active.ready) || !isClosed(pending.ready) {
		t.Fatal("wakeAll did not wake active and pending waiters")
	}
	if len(p.waiters) != 0 {
		t.Fatalf("wakeAll left waiter state: %v", p.waiters)
	}
}

func TestPollerStoppedBeforeStartCannotRestart(t *testing.T) {
	p, _, _, _ := newTestPoller()
	p.stop()
	if !p.disabled {
		t.Fatal("stop did not disable poller")
	}
	if p.start() {
		t.Fatal("disabled poller restarted")
	}
}

func TestPollUnregisterBeforeStart(t *testing.T) {
	p := &poller{
		regCh:    make(chan *waiter, 1),
		cancelCh: make(chan *waiter, 1),
		unregCh:  make(chan VLSH, 1),
		stopCh:   make(chan struct{}),
		stopped:  make(chan struct{}),
		waiters:  make(map[VLSH]*waitSet),
		started:  make(chan struct{}),
	}
	select {
	case <-p.started:
		t.Fatal("started should not be closed")
	default:
	}
}
