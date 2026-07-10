package vclpoll

import "sync"

// shardedListener implements the mode-2 "one listener per worker" design.
// Each worker creates its own VLS listener on the same address:port and runs
// a per-worker accept loop. Accepted connections fan into a shared channel
// that acceptFullContext reads from.
//
// This avoids cross-worker VLS access: each worker only touches its own raw
// handles for both listening and accepted sessions.
type shardedListener struct {
	d       *mode2Dispatcher
	public  VLSH
	shards  []listenerShard
	acceptC chan shardAcceptResult
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

type listenerShard struct {
	worker *worker
	raw    VLSH
}

type shardAcceptResult struct {
	handle VLSH
	addr   AddrInfo
	err    error
}

// newShardedListener creates one VLS listener per worker using createFn,
// starts per-worker accept loops, and returns a single public handle that
// the caller uses for acceptFullContext/close.
func (d *mode2Dispatcher) newShardedListener(createFn func() (VLSH, error)) (VLSH, error) {
	shards := make([]listenerShard, 0, len(d.workers))

	for _, w := range d.workers {
		value, err := d.submit(w, func(w *worker) (any, error) {
			raw, err := createFn()
			if err != nil {
				return nil, err
			}
			if !w.skipOwnershipCheck {
				if err := w.checkOwnership(raw); err != nil {
					_ = rawClose(raw)
					return nil, err
				}
			}
			return raw, nil
		})
		if err != nil {
			for _, s := range shards {
				d.submit(s.worker, func(_ *worker) (any, error) {
					_ = rawClose(s.raw)
					return nil, nil
				})
			}
			return invalidVLSH, err
		}
		shards = append(shards, listenerShard{worker: w, raw: value.(VLSH)})
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
	sl.startAcceptLoops()
	return public, nil
}

func (sl *shardedListener) startAcceptLoops() {
	for i := range sl.shards {
		sl.wg.Add(1)
		shard := &sl.shards[i]
		go sl.acceptLoop(shard)
	}
}

// acceptLoop is the per-worker accept goroutine. It submits non-blocking
// accept calls to its worker and polls via the worker's epoll when EAGAIN.
func (sl *shardedListener) acceptLoop(shard *listenerShard) {
	defer sl.wg.Done()
	w := shard.worker
	raw := shard.raw

	for {
		select {
		case <-sl.stopCh:
			return
		case <-w.quiesced:
			return
		default:
		}

		value, err := sl.d.submit(w, func(w *worker) (any, error) {
			conn, addr, err := rawAcceptFull(raw)
			if err != nil {
				return nil, err
			}
			public, err := sl.d.registerSession(w, conn)
			if err != nil {
				return nil, err
			}
			return acceptReply{handle: public, addr: addr}, nil
		})
		if err == nil {
			reply := value.(acceptReply)
			select {
			case sl.acceptC <- shardAcceptResult{handle: reply.handle, addr: reply.addr}:
			case <-sl.stopCh:
				return
			}
			continue
		}

		if !isAgainError(err) {
			select {
			case sl.acceptC <- shardAcceptResult{err: err}:
			case <-sl.stopCh:
				return
			}
			return
		}

		// EAGAIN: wait for readiness on this worker's listener via its epoll.
		waiter := &waiter{vlsh: VLSH(raw), events: epollIn, ready: make(chan struct{})}
		_, addErr := sl.d.submit(w, func(w *worker) (any, error) {
			return nil, w.addWaiterRaw(raw, waiter)
		})
		if addErr != nil {
			select {
			case <-sl.stopCh:
				return
			case <-w.quiesced:
				return
			default:
			}
			continue
		}

		select {
		case <-waiter.ready:
		case <-sl.stopCh:
			sl.d.submit(w, func(w *worker) (any, error) {
				w.cancelWaiterRaw(raw, waiter)
				return nil, nil
			})
			return
		case <-w.quiesced:
			return
		}
	}
}

// lookupShardedListener returns the shardedListener for a public handle, or nil.
func (d *mode2Dispatcher) lookupShardedListener(public VLSH) *shardedListener {
	value, ok := d.owners.Load(public)
	if !ok {
		return nil
	}
	sl, ok := value.(*shardedListener)
	if !ok {
		return nil
	}
	return sl
}

// shardedAcceptFullContext reads the next accepted connection from the fan-in
// channel, respecting cancellation.
func (d *mode2Dispatcher) shardedAcceptFullContext(sl *shardedListener, done <-chan struct{}) (VLSH, AddrInfo, error) {
	if done == nil {
		select {
		case result := <-sl.acceptC:
			return result.handle, result.addr, result.err
		case <-sl.stopCh:
			return invalidVLSH, AddrInfo{}, ErrClosed
		}
	}
	select {
	case result := <-sl.acceptC:
		return result.handle, result.addr, result.err
	case <-done:
		return invalidVLSH, AddrInfo{}, ErrClosed
	case <-sl.stopCh:
		return invalidVLSH, AddrInfo{}, ErrClosed
	}
}

// closeShardedListener stops the accept loops and closes all raw listeners.
func (d *mode2Dispatcher) closeShardedListener(sl *shardedListener) error {
	close(sl.stopCh)
	sl.wg.Wait()

	// Drain any accepted-but-unconsumed connections.
	for {
		select {
		case result := <-sl.acceptC:
			if result.err == nil {
				d.closeVLSH(result.handle)
			}
		default:
			goto drained
		}
	}
drained:

	var firstErr error
	for _, shard := range sl.shards {
		_, err := d.submit(shard.worker, func(w *worker) (any, error) {
			w.removeWaitersRaw(shard.raw)
			return nil, rawClose(shard.raw)
		})
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	d.owners.Delete(sl.public)
	return firstErr
}
