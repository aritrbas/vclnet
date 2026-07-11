package vclnet

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"github.com/aritrbas/vclnet/internal/vclpoll"
)

// tcpListener implements net.Listener over a VLS listening session.
type tcpListener struct {
	vlsh    vclpoll.VLSH
	addr    *net.TCPAddr
	network string

	closed    atomic.Bool
	closeOnce sync.Once
	tracked   atomic.Bool
	doneCh    chan struct{}
}

var _ net.Listener = (*tcpListener)(nil)

func newTCPListener(vlsh vclpoll.VLSH, addr *net.TCPAddr, network string) *tcpListener {
	return &tcpListener{
		vlsh:    vlsh,
		addr:    addr,
		network: network,
		doneCh:  make(chan struct{}),
	}
}

// Accept waits for and returns the next connection to the listener.
func (l *tcpListener) Accept() (net.Conn, error) {
	return l.AcceptContext(context.Background())
}

// AcceptContext waits for the next connection, respecting context cancellation.
func (l *tcpListener) AcceptContext(ctx context.Context) (net.Conn, error) {
	if l.closed.Load() || shutdownStarted.Load() {
		return nil, opErrorAddr("accept", l.addr, ErrClosed)
	}
	if err := ctx.Err(); err != nil {
		return nil, opErrorAddr("accept", l.addr, err)
	}

	doneCh, stop := l.mergedDone(ctx)
	defer stop()

	connVLSH, peerInfo, err := vclpoll.AcceptFullContext(l.vlsh, doneCh)
	if err != nil {
		if l.closed.Load() || shutdownStarted.Load() {
			return nil, opErrorAddr("accept", l.addr, ErrClosed)
		}
		if ctx.Err() != nil {
			return nil, opErrorAddr("accept", l.addr, ctx.Err())
		}
		if errors.Is(err, vclpoll.ErrClosed) {
			return nil, opErrorAddr("accept", l.addr, ErrClosed)
		}
		return nil, opErrorAddr("accept", l.addr, err)
	}

	conn := newTCPConn(connVLSH)
	conn.peerAddr = addrFromInfo(peerInfo)
	live.addConn(conn)
	conn.tracked.Store(true)
	return conn, nil
}

// Close closes the listener and wakes blocked Accept operations.
func (l *tcpListener) Close() error {
	var err error
	l.closeOnce.Do(func() {
		l.closed.Store(true)
		close(l.doneCh)
		if !shutdownStarted.Load() {
			err = vclpoll.Close(l.vlsh)
		}
		if l.tracked.Load() {
			live.removeListener(l)
		}
	})
	if err != nil {
		return opErrorAddr("close", l.addr, err)
	}
	return nil
}

func (l *tcpListener) Addr() net.Addr {
	return l.addr
}

// mergedDone returns a channel that closes when the context or listener is
// done. The stop function prevents a goroutine leak when Accept succeeds first.
func (l *tcpListener) mergedDone(ctx context.Context) (<-chan struct{}, func()) {
	if ctx.Done() == nil {
		return l.doneCh, func() {}
	}

	merged := make(chan struct{})
	stopCh := make(chan struct{})
	var stopOnce sync.Once
	go func() {
		select {
		case <-ctx.Done():
			close(merged)
		case <-l.doneCh:
			close(merged)
		case <-stopCh:
		}
	}()
	return merged, func() {
		stopOnce.Do(func() { close(stopCh) })
	}
}
