package vclpoll

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// waiter represents a goroutine parked waiting for a session event.
type waiter struct {
	vlsh   VLSH
	events uint32
	ready  chan struct{}
}

// waitSet contains every goroutine waiting on one VLS session. VLS epoll
// accepts a session only once, so the poller registers the union of the
// requested masks and fans returned events out to matching waiters.
type waitSet struct {
	waiters map[*waiter]struct{}
	events  uint32
}

// unregisterRequest is a close barrier. The caller must not destroy or reuse
// the VLS handle until the poller has removed its epoll interest and woken all
// waiters. VLS handles are small pool indexes and can be reused immediately.
type unregisterRequest struct {
	vlsh VLSH
	done chan struct{}
}

// poller is the shared vls_epoll goroutine. It owns a single persistent
// vls_epoll handle and multiplexes readiness notifications for all sessions.
type poller struct {
	epVLSH   VLSH
	regCh    chan *waiter
	cancelCh chan *waiter
	unregCh  chan unregisterRequest
	stopCh   chan struct{}
	stopped  chan struct{}
	waiters  map[VLSH]*waitSet
	once     sync.Once
	startMu  sync.Mutex
	disabled bool
	started  chan struct{}
	running  atomic.Bool

	// Tests inject these operations so waiter bookkeeping can be verified
	// without starting VPP. Nil uses the real VLS epoll bridge.
	epollAdd func(VLSH, VLSH, uint32) error
	epollMod func(VLSH, VLSH, uint32) error
	epollDel func(VLSH, VLSH)
}

var defaultPoller = &poller{
	regCh:    make(chan *waiter, 256),
	cancelCh: make(chan *waiter, 256),
	unregCh:  make(chan unregisterRequest, 256),
	stopCh:   make(chan struct{}),
	stopped:  make(chan struct{}),
	waiters:  make(map[VLSH]*waitSet),
	started:  make(chan struct{}),
}

func (p *poller) start() bool {
	p.startMu.Lock()
	defer p.startMu.Unlock()
	if p.disabled {
		return false
	}
	p.once.Do(func() {
		go p.loop()
		<-p.started
	})
	return p.running.Load()
}

func (p *poller) loop() {
	runtime.LockOSThread()
	registerThisThread()

	ep, err := pollerEpollCreate()
	if err != nil {
		panic("vclpoll: poller: " + err.Error())
	}
	p.epVLSH = ep
	p.running.Store(true)
	close(p.started)

	eventBuf := make([]pollEvent, 64)

	for {
		select {
		case <-p.stopCh:
			p.wakeAll()
			p.running.Store(false)
			close(p.stopped)
			return
		default:
		}

		p.drainRegistrations()
		p.drainCancellations()
		p.drainUnregistrations()

		n := pollerEpollWait(p.epVLSH, eventBuf)
		for i := 0; i < n; i++ {
			p.handleEvent(eventBuf[i].Vlsh, eventBuf[i].Events)
		}
	}
}

func (p *poller) wakeAll() {
	for vlsh, set := range p.waiters {
		p.del(p.epVLSH, vlsh)
		wakeWaitSet(set)
	}
	p.waiters = make(map[VLSH]*waitSet)
	for {
		select {
		case w := <-p.regCh:
			wakeWaiter(w)
		default:
			return
		}
	}
}

func (p *poller) drainRegistrations() {
	for {
		select {
		case w := <-p.regCh:
			set, exists := p.waiters[w.vlsh]
			if !exists {
				if err := p.add(p.epVLSH, w.vlsh, w.events); err != nil {
					wakeWaiter(w)
					continue
				}
				p.waiters[w.vlsh] = &waitSet{
					waiters: map[*waiter]struct{}{w: {}},
					events:  w.events,
				}
				continue
			}

			newEvents := set.events | w.events
			if newEvents != set.events {
				if err := p.mod(p.epVLSH, w.vlsh, newEvents); err != nil {
					wakeWaiter(w)
					continue
				}
			}
			set.waiters[w] = struct{}{}
			set.events = newEvents
		default:
			return
		}
	}
}

// drainCancellations removes one precise waiter. Cancelling a read deadline
// must not remove a concurrent writer waiting on the same VLSH.
func (p *poller) drainCancellations() {
	for {
		select {
		case w := <-p.cancelCh:
			set, ok := p.waiters[w.vlsh]
			if !ok {
				continue
			}
			if _, ok := set.waiters[w]; !ok {
				continue
			}

			oldEvents := set.events
			delete(set.waiters, w)
			wakeWaiter(w)
			if len(set.waiters) == 0 {
				p.del(p.epVLSH, w.vlsh)
				delete(p.waiters, w.vlsh)
				continue
			}

			set.events = combinedEvents(set)
			if set.events != oldEvents {
				if err := p.mod(p.epVLSH, w.vlsh, set.events); err != nil {
					p.dropWaitSet(w.vlsh, set)
				}
			}
		default:
			return
		}
	}
}

func (p *poller) drainUnregistrations() {
	for {
		select {
		case req := <-p.unregCh:
			if set, ok := p.waiters[req.vlsh]; ok {
				p.del(p.epVLSH, req.vlsh)
				delete(p.waiters, req.vlsh)
				wakeWaitSet(set)
			}
			close(req.done)
		default:
			return
		}
	}
}

func (p *poller) handleEvent(vlsh VLSH, events uint32) {
	set, ok := p.waiters[vlsh]
	if !ok {
		return
	}

	oldEvents := set.events
	terminal := events&(epollErr|epollHup|epollRDHup) != 0
	for w := range set.waiters {
		if terminal || events&w.events != 0 {
			delete(set.waiters, w)
			wakeWaiter(w)
		}
	}

	if len(set.waiters) == 0 {
		p.del(p.epVLSH, vlsh)
		delete(p.waiters, vlsh)
		return
	}

	set.events = combinedEvents(set)
	if set.events != oldEvents {
		if err := p.mod(p.epVLSH, vlsh, set.events); err != nil {
			p.dropWaitSet(vlsh, set)
		}
	}
}

func combinedEvents(set *waitSet) uint32 {
	var events uint32
	for w := range set.waiters {
		events |= w.events
	}
	return events
}

func wakeWaiter(w *waiter) {
	close(w.ready)
}

func wakeWaitSet(set *waitSet) {
	for w := range set.waiters {
		wakeWaiter(w)
	}
}

func (p *poller) dropWaitSet(vlsh VLSH, set *waitSet) {
	p.del(p.epVLSH, vlsh)
	delete(p.waiters, vlsh)
	wakeWaitSet(set)
}

func (p *poller) add(epVLSH, vlsh VLSH, events uint32) error {
	if p.epollAdd != nil {
		return p.epollAdd(epVLSH, vlsh, events)
	}
	return pollerEpollCtlAdd(epVLSH, vlsh, events)
}

func (p *poller) mod(epVLSH, vlsh VLSH, events uint32) error {
	if p.epollMod != nil {
		return p.epollMod(epVLSH, vlsh, events)
	}
	return pollerEpollCtlMod(epVLSH, vlsh, events)
}

func (p *poller) del(epVLSH, vlsh VLSH) {
	if p.epollDel != nil {
		p.epollDel(epVLSH, vlsh)
		return
	}
	pollerEpollCtlDel(epVLSH, vlsh)
}

// StopPoller signals the poller to exit and waits for it to stop.
// It must run before AppDestroy so no poller VLS call remains in flight.
func mode3StopPoller() {
	defaultPoller.stop()
}

func (p *poller) stop() {
	p.startMu.Lock()
	p.disabled = true
	select {
	case <-p.started:
		select {
		case <-p.stopCh:
		default:
			close(p.stopCh)
		}
		p.startMu.Unlock()
		<-p.stopped
	default:
		p.startMu.Unlock()
	}
}

// pollWait registers vlsh for the specified events with the shared poller and
// blocks until the event fires or the session is unregistered.
func pollWait(vlsh VLSH, events uint32) {
	mode3PollWaitContext(vlsh, events, nil)
}

// pollUnregister removes every waiter for vlsh, wakes parked goroutines, and
// waits for the poller to acknowledge that the handle is no longer in its
// epoll set. Callers may destroy/reuse the VLS handle only after this returns.
func pollUnregister(vlsh VLSH) {
	p := defaultPoller
	p.startMu.Lock()
	running := !p.disabled && p.running.Load()
	p.startMu.Unlock()
	if !running {
		return
	}
	req := unregisterRequest{vlsh: vlsh, done: make(chan struct{})}
	select {
	case p.unregCh <- req:
	case <-p.stopped:
		return
	}
	select {
	case <-req.done:
	case <-p.stopped:
	}
}

// PollWaitContext waits until an event fires or doneCh is closed. Cancellation
// removes only this waiter; other readers or writers on the same session stay
// registered.
func mode3PollWaitContext(vlsh VLSH, events uint32, doneCh <-chan struct{}) bool {
	if !defaultPoller.start() {
		return false
	}
	w := &waiter{
		vlsh:   vlsh,
		events: events,
		ready:  make(chan struct{}),
	}

	select {
	case defaultPoller.regCh <- w:
	case <-doneCh:
		return false
	case <-defaultPoller.stopped:
		return false
	}

	select {
	case <-w.ready:
		return true
	case <-doneCh:
		select {
		case defaultPoller.cancelCh <- w:
		case <-w.ready:
			return true
		case <-defaultPoller.stopped:
		}
		return false
	case <-defaultPoller.stopped:
		return false
	}
}
