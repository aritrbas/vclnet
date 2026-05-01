package vclnet

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aritrbas/vclnet/internal/vclpoll"
)

// errUnexpectedZeroWrite reports that a non-blocking VLS write returned 0
// bytes without EAGAIN or another error. This should not happen — it is
// distinct from io.ErrShortWrite, which means the writer accepted fewer
// bytes than requested; here VLS accepted none at all on a call that
// otherwise reported success.
var errUnexpectedZeroWrite = errors.New("vclnet: VLS write returned 0 bytes unexpectedly")

// deadlineState provides resettable deadline notification. Every Set call
// wakes current waiters; they then observe the new deadline and either time
// out or wait again on the replacement channel.
type deadlineState struct {
	mu     sync.Mutex
	when   time.Time
	ch     chan struct{}
	closed bool
	timer  *time.Timer
}

func newDeadlineState() *deadlineState {
	return &deadlineState{ch: make(chan struct{})}
}

func (d *deadlineState) set(t time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	d.closeCurrentLocked()

	d.when = t
	d.ch = make(chan struct{})
	d.closed = false
	if t.IsZero() {
		return
	}
	if delay := time.Until(t); delay <= 0 {
		d.closeCurrentLocked()
	} else {
		ch := d.ch
		d.timer = time.AfterFunc(delay, func() {
			d.mu.Lock()
			defer d.mu.Unlock()
			if d.ch == ch {
				d.closeCurrentLocked()
				d.timer = nil
			}
		})
	}
}

func (d *deadlineState) waitChannel() <-chan struct{} {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ch
}

func (d *deadlineState) expired() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return !d.when.IsZero() && !time.Now().Before(d.when)
}

func (d *deadlineState) value() time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.when
}

func (d *deadlineState) interrupt() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	d.closeCurrentLocked()
}

func (d *deadlineState) closeCurrentLocked() {
	if !d.closed {
		close(d.ch)
		d.closed = true
	}
}

// tcpConn implements net.Conn over a VLS session.
type tcpConn struct {
	vlsh      vclpoll.VLSH
	localAddr *net.TCPAddr
	peerAddr  *net.TCPAddr
	addrMu    sync.Mutex

	closed    atomic.Bool
	closeOnce sync.Once
	// tracked records whether this conn was registered with the lifecycle
	// registry. Tests that construct bare tcpConn via newTCPConn(0) skip
	// registration, so Close must not attempt to unregister them.
	tracked atomic.Bool

	// readShutdown and writeShutdown record half-close state independently
	// of closed. vls_shutdown does not emit an epoll event for a locally
	// issued SHUT_RD/SHUT_WR, so once set we short-circuit further I/O and
	// wake any parked reader/writer through the corresponding deadline
	// channel (interrupt() is idempotent with a subsequent SetDeadline).
	readShutdown  atomic.Bool
	writeShutdown atomic.Bool
	readShutOnce  sync.Once
	writeShutOnce sync.Once

	readDeadline  *deadlineState
	writeDeadline *deadlineState
}

var _ net.Conn = (*tcpConn)(nil)

func newTCPConn(vlsh vclpoll.VLSH) *tcpConn {
	return &tcpConn{
		vlsh:          vlsh,
		readDeadline:  newDeadlineState(),
		writeDeadline: newDeadlineState(),
	}
}

func (c *tcpConn) Read(b []byte) (int, error) {
	// Half-close: Read after CloseRead returns io.EOF unwrapped, matching
	// net.TCPConn. This check precedes readStateError so a pending read
	// deadline does not shadow the half-close result.
	if c.readShutdown.Load() {
		return 0, io.EOF
	}
	if err := c.readStateError(); err != nil {
		return 0, opErrorAddr("read", c.RemoteAddr(), err)
	}
	if len(b) == 0 {
		return 0, nil
	}
	for {
		if c.readShutdown.Load() {
			return 0, io.EOF
		}
		if err := c.readStateError(); err != nil {
			return 0, opErrorAddr("read", c.RemoteAddr(), err)
		}

		n, err := vclpoll.ReadContext(c.vlsh, b, c.readDeadline.waitChannel())
		if retry, mapped := mapIOError(err); retry {
			continue
		} else if mapped != nil {
			return 0, opErrorAddr("read", c.RemoteAddr(), mapped)
		}
		if n == 0 {
			return 0, io.EOF
		}
		return n, nil
	}
}

func (c *tcpConn) Write(b []byte) (int, error) {
	if c.writeShutdown.Load() {
		return 0, opErrorAddr("write", c.RemoteAddr(), net.ErrClosed)
	}
	if err := c.writeStateError(); err != nil {
		return 0, opErrorAddr("write", c.RemoteAddr(), err)
	}
	if len(b) == 0 {
		return 0, nil
	}
	written := 0
	for written < len(b) {
		if c.writeShutdown.Load() {
			return written, opErrorAddr("write", c.RemoteAddr(), net.ErrClosed)
		}
		if err := c.writeStateError(); err != nil {
			return written, opErrorAddr("write", c.RemoteAddr(), err)
		}

		n, err := vclpoll.WriteContext(c.vlsh, b[written:], c.writeDeadline.waitChannel())
		if retry, mapped := mapIOError(err); retry {
			continue
		} else if mapped != nil {
			return written, opErrorAddr("write", c.RemoteAddr(), mapped)
		}
		if n == 0 {
			return written, opErrorAddr("write", c.RemoteAddr(), errUnexpectedZeroWrite)
		}
		written += n
	}
	return written, nil
}

// CloseRead shuts down the reading side of the TCP connection using VCL
// shutdown semantics (vls_shutdown → vppcom_session_shutdown with SHUT_RD).
// After CloseRead, subsequent Read calls return io.EOF and any parked reader
// is woken. CloseRead is idempotent; calling it again returns nil without
// re-issuing the VLS shutdown. Once Close has been called, CloseRead instead
// returns an error wrapping net.ErrClosed, matching net.TCPConn. It does not
// affect the write half; callers can still Write until CloseWrite or Close
// is invoked.
//
// Implements the ambient interface { CloseRead() error } that packages such
// as net/http detect on a net.Conn to signal half-close.
func (c *tcpConn) CloseRead() error {
	if c.closed.Load() {
		return opErrorAddr("close", c.RemoteAddr(), net.ErrClosed)
	}
	var err error
	c.readShutOnce.Do(func() {
		c.readShutdown.Store(true)
		// Wake any parked reader; the retry-loop observes readShutdown and
		// returns io.EOF instead of retrying the VLS call.
		c.readDeadline.interrupt()
		// Skip the VLS call if the app is torn down — vlsh may already be
		// invalid and rawShutdown would race the AppDestroy path.
		if shutdownStarted.Load() {
			return
		}
		err = vclpoll.Shutdown(c.vlsh, vclpoll.ShutRD)
	})
	if err != nil {
		return opErrorAddr("close", c.RemoteAddr(), err)
	}
	return nil
}

// CloseWrite shuts down the writing side of the TCP connection using VCL
// shutdown semantics (vls_shutdown → vppcom_session_shutdown with SHUT_WR).
// VPP sends the shutdown message to the peer so it observes EOF on its Read.
// After CloseWrite, subsequent Write calls return an error wrapping
// net.ErrClosed (matching net.TCPConn semantics) and any parked writer is
// woken. CloseWrite is idempotent; calling it again returns nil without
// re-issuing the VLS shutdown. Once Close has been called, CloseWrite
// instead returns an error wrapping net.ErrClosed, matching net.TCPConn. It
// does not affect the read half; callers can still Read until Close or the
// peer finishes writing.
//
// Implements the ambient interface { CloseWrite() error } that HTTP request
// bodies and other protocols detect for graceful half-close.
func (c *tcpConn) CloseWrite() error {
	if c.closed.Load() {
		return opErrorAddr("close", c.RemoteAddr(), net.ErrClosed)
	}
	var err error
	c.writeShutOnce.Do(func() {
		c.writeShutdown.Store(true)
		c.writeDeadline.interrupt()
		if shutdownStarted.Load() {
			return
		}
		err = vclpoll.Shutdown(c.vlsh, vclpoll.ShutWR)
	})
	if err != nil {
		return opErrorAddr("close", c.RemoteAddr(), err)
	}
	return nil
}

func (c *tcpConn) readStateError() error  { return ioStateError(&c.closed, c.readDeadline) }
func (c *tcpConn) writeStateError() error { return ioStateError(&c.closed, c.writeDeadline) }

func (c *tcpConn) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.readDeadline.interrupt()
		c.writeDeadline.interrupt()
		if !shutdownStarted.Load() {
			err = vclpoll.Close(c.vlsh)
		}
		if c.tracked.Load() {
			live.removeConn(c)
		}
	})
	if err != nil {
		return opErrorAddr("close", c.RemoteAddr(), err)
	}
	return nil
}

func (c *tcpConn) LocalAddr() net.Addr {
	c.addrMu.Lock()
	defer c.addrMu.Unlock()
	if c.localAddr != nil {
		return c.localAddr
	}
	if c.closed.Load() || shutdownStarted.Load() {
		return &net.TCPAddr{}
	}
	info, err := vclpoll.GetLocalAddr(c.vlsh)
	if err != nil {
		return &net.TCPAddr{}
	}
	c.localAddr = addrFromInfo(info)
	return c.localAddr
}

func (c *tcpConn) RemoteAddr() net.Addr {
	c.addrMu.Lock()
	defer c.addrMu.Unlock()
	if c.peerAddr != nil {
		return c.peerAddr
	}
	if c.closed.Load() || shutdownStarted.Load() {
		return &net.TCPAddr{}
	}
	info, err := vclpoll.GetPeerAddr(c.vlsh)
	if err != nil {
		return &net.TCPAddr{}
	}
	c.peerAddr = addrFromInfo(info)
	return c.peerAddr
}

func (c *tcpConn) SetDeadline(t time.Time) error {
	if err := c.deadlineStateError(); err != nil {
		return opErrorAddr("set", c.RemoteAddr(), err)
	}
	c.readDeadline.set(t)
	c.writeDeadline.set(t)
	return nil
}

func (c *tcpConn) SetReadDeadline(t time.Time) error {
	if err := c.deadlineStateError(); err != nil {
		return opErrorAddr("set", c.RemoteAddr(), err)
	}
	c.readDeadline.set(t)
	return nil
}

func (c *tcpConn) SetWriteDeadline(t time.Time) error {
	if err := c.deadlineStateError(); err != nil {
		return opErrorAddr("set", c.RemoteAddr(), err)
	}
	c.writeDeadline.set(t)
	return nil
}

func (c *tcpConn) deadlineStateError() error { return closedStateError(&c.closed) }

// mapIOError translates vclpoll sentinel errors into vclnet-layer errors.
// Returns (true, nil) for ErrWaitCanceled (caller should retry), or
// (false, mapped) with ErrClosed substituted for vclpoll.ErrClosed.
func mapIOError(err error) (retry bool, mapped error) {
	if errors.Is(err, vclpoll.ErrWaitCanceled) {
		return true, nil
	}
	if errors.Is(err, vclpoll.ErrClosed) {
		return false, ErrClosed
	}
	return false, err
}

// ioStateError checks if the connection is closed or shutdown, or if the
// deadline has expired. Shared by tcpConn and udpConn.
func ioStateError(closed *atomic.Bool, dl *deadlineState) error {
	if closed.Load() || shutdownStarted.Load() {
		return ErrClosed
	}
	if dl.expired() {
		return &timeoutError{}
	}
	return nil
}

// closedStateError checks only the closed/shutdown condition without a
// deadline. Used by SetDeadline/SetReadDeadline/SetWriteDeadline.
func closedStateError(closed *atomic.Bool) error {
	if closed.Load() || shutdownStarted.Load() {
		return ErrClosed
	}
	return nil
}

// timeoutError implements net.Error with Timeout() == true.
type timeoutError struct{}

func (e *timeoutError) Error() string   { return "i/o timeout" }
func (e *timeoutError) Timeout() bool   { return true }
func (e *timeoutError) Temporary() bool { return true }
