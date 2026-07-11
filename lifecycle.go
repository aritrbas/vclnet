package vclnet

import (
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// liveCloser is the minimum surface the lifecycle registry needs from a
// tracked object. Every registered listener, connection, and PacketConn
// exposes Close so drain-then-force teardown can proceed uniformly.
type liveCloser interface {
	io.Closer
}

// liveRegistry tracks open listeners, connections, PacketConns, and in-flight
// dials so Shutdown can order teardown deterministically instead of racing
// application goroutines against VLS destruction.
//
// Ordering rules enforced elsewhere:
//   - Listeners close first (stop admitting new work at the process boundary).
//   - Pending dials must finish or be interrupted before the VCL app is
//     destroyed; a dial that finished but has not yet handed its conn to the
//     caller registers the conn before endDial so the object is not lost.
//   - Connections drain until either the drain window elapses or all conns
//     have been closed naturally.
//
// The registry stores raw pointers rather than typed slots so PacketConn
// (which is not a net.Conn) and TCPListener (not a net.Conn either) share the
// same code path.
type liveRegistry struct {
	mu           sync.Mutex
	listeners    map[liveCloser]struct{}
	conns        map[liveCloser]struct{}
	packetConns  map[liveCloser]struct{}
	pendingDials atomic.Int64
	drainedCh    chan struct{}
}

var live = newLiveRegistry()

func newLiveRegistry() *liveRegistry {
	return &liveRegistry{
		listeners:   make(map[liveCloser]struct{}),
		conns:       make(map[liveCloser]struct{}),
		packetConns: make(map[liveCloser]struct{}),
	}
}

func (r *liveRegistry) addListener(l liveCloser) {
	r.mu.Lock()
	r.listeners[l] = struct{}{}
	r.mu.Unlock()
}

func (r *liveRegistry) removeListener(l liveCloser) {
	r.mu.Lock()
	delete(r.listeners, l)
	r.mu.Unlock()
	r.notifyIfDrained()
}

func (r *liveRegistry) addConn(c liveCloser) {
	r.mu.Lock()
	r.conns[c] = struct{}{}
	r.mu.Unlock()
}

func (r *liveRegistry) removeConn(c liveCloser) {
	r.mu.Lock()
	delete(r.conns, c)
	r.mu.Unlock()
	r.notifyIfDrained()
}

func (r *liveRegistry) addPacketConn(pc liveCloser) {
	r.mu.Lock()
	r.packetConns[pc] = struct{}{}
	r.mu.Unlock()
}

func (r *liveRegistry) removePacketConn(pc liveCloser) {
	r.mu.Lock()
	delete(r.packetConns, pc)
	r.mu.Unlock()
	r.notifyIfDrained()
}

// beginDial records an in-flight dial. Callers must invoke the returned
// function exactly once; deferring it is the recommended pattern.
func (r *liveRegistry) beginDial() func() {
	r.pendingDials.Add(1)
	return func() {
		r.pendingDials.Add(-1)
		r.notifyIfDrained()
	}
}

// snapshotListeners returns a stable slice of currently tracked listeners.
func (r *liveRegistry) snapshotListeners() []liveCloser {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]liveCloser, 0, len(r.listeners))
	for l := range r.listeners {
		out = append(out, l)
	}
	return out
}

// snapshotConns returns a stable slice of currently tracked connections.
func (r *liveRegistry) snapshotConns() []liveCloser {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]liveCloser, 0, len(r.conns))
	for c := range r.conns {
		out = append(out, c)
	}
	return out
}

// snapshotPacketConns returns a stable slice of currently tracked PacketConns.
func (r *liveRegistry) snapshotPacketConns() []liveCloser {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]liveCloser, 0, len(r.packetConns))
	for pc := range r.packetConns {
		out = append(out, pc)
	}
	return out
}

// counts reports the number of live objects. Only useful for tests and the
// drain loop; callers must not race counts against add/remove without an
// additional synchronizer.
func (r *liveRegistry) counts() (listeners, conns, packetConns int, dials int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.listeners), len(r.conns), len(r.packetConns), r.pendingDials.Load()
}

// isDrained reports whether every tracked slot is empty and no dial is
// pending.
func (r *liveRegistry) isDrained() bool {
	ln, cn, pcn, dials := r.counts()
	return ln == 0 && cn == 0 && pcn == 0 && dials == 0
}

// waitDrain blocks until every tracked object is closed or the deadline
// passes. A zero timeout means wait indefinitely.
func (r *liveRegistry) waitDrain(timeout time.Duration) bool {
	// Install (or reuse) the drainedCh under the same lock that
	// notifyIfDrained inspects, then re-check drained state so we cannot
	// miss a concurrent last-removal that fired before the channel existed.
	r.mu.Lock()
	if r.drainedLocked() {
		r.mu.Unlock()
		return true
	}
	if r.drainedCh == nil {
		r.drainedCh = make(chan struct{})
	}
	ch := r.drainedCh
	r.mu.Unlock()

	if timeout <= 0 {
		<-ch
		return r.isDrained()
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
		return r.isDrained()
	case <-timer.C:
		return r.isDrained()
	}
}

// notifyIfDrained wakes any waitDrain callers once the registry hits zero
// across all slots.
func (r *liveRegistry) notifyIfDrained() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.drainedLocked() {
		return
	}
	if r.drainedCh != nil {
		close(r.drainedCh)
		r.drainedCh = nil
	}
}

// drainedLocked reports whether the registry is drained. Caller must hold
// r.mu; pendingDials is atomic and safe to read either way.
func (r *liveRegistry) drainedLocked() bool {
	return len(r.listeners) == 0 &&
		len(r.conns) == 0 &&
		len(r.packetConns) == 0 &&
		r.pendingDials.Load() == 0
}
