package vclnet

import (
	"context"
	"net"
	"time"

	"github.com/aritrbas/vclnet/internal/vclpoll"
)

const defaultFallbackDelay = 250 * time.Millisecond

// Dialer provides options for connecting to an address via VPP.
type Dialer struct {
	// Timeout is the maximum time to wait for a connect to complete.
	// Zero means no timeout.
	Timeout time.Duration

	// FallbackDelay is the delay before starting the next address family
	// in happy-eyeballs mode. Default is 250ms per RFC 8305.
	FallbackDelay time.Duration
}

// DialContext connects to the address on the named network, respecting the
// context's deadline and cancellation.
func (d *Dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if shutdownStarted.Load() {
		return nil, opError("dial", network, address, ErrClosed)
	}
	_, _, err := parseNetwork(network)
	if err != nil {
		return nil, opError("dial", network, address, err)
	}

	if ctx.Err() != nil {
		return nil, opError("dial", network, address, ctx.Err())
	}

	if d.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, d.Timeout)
		defer cancel()
	}

	if isUDP(network) {
		return d.dialUDP(ctx, network, address)
	}

	return d.dialTCP(ctx, network, address)
}

func (d *Dialer) dialTCP(ctx context.Context, network, address string) (net.Conn, error) {
	// For "tcp" (no suffix), use happy eyeballs with both address families.
	if network == "tcp" {
		addrs, err := resolveAddrs(ctx, network, address)
		if err != nil {
			return nil, opError("dial", network, address, err)
		}
		if len(addrs) > 1 {
			return d.dialHappyEyeballs(ctx, network, address, addrs)
		}
		// Single address — fall through to direct connect.
		return d.dialSingleTCP(ctx, network, address, addrs[0])
	}

	// "tcp4" or "tcp6" — single-family connect.
	addr, err := resolveAddrContext(ctx, network, address)
	if err != nil {
		return nil, opError("dial", network, address, err)
	}
	return d.dialSingleTCP(ctx, network, address, addr)
}

func (d *Dialer) dialSingleTCP(ctx context.Context, network, address string, addr *net.TCPAddr) (net.Conn, error) {
	endDial := live.beginDial()
	defer endDial()

	vlsh, immediate, err := connectStart(addr)
	if err != nil {
		return nil, opError("dial", network, address, err)
	}

	if !immediate {
		// Wait on the union of EPOLLOUT (write-ready = connect completed
		// successfully) and EPOLLERR|EPOLLHUP (connect failed). Both wake
		// the same waiter; sessionConnectError disambiguates.
		ok := vclpoll.PollWaitContext(vlsh, connectReadyEvents, ctx.Done())
		if !ok {
			vclpoll.CloseVLSH(vlsh)
			return nil, opError("dial", network, address, interruptedConnectError(ctx))
		}
		// EPOLLOUT alone does not mean the connect succeeded — a refused
		// SYN or unreachable route still surfaces via
		// SESSION_CTRL_EVT_CONNECTED with a non-zero retval. Query VPP
		// for the session's post-connect error before handing the vlsh
		// back to the caller.
		if err := vclpoll.SessionConnectError(vlsh); err != nil {
			vclpoll.CloseVLSH(vlsh)
			return nil, opError("dial", network, address, err)
		}
	}

	if err := ctx.Err(); err != nil {
		vclpoll.CloseVLSH(vlsh)
		return nil, opError("dial", network, address, err)
	}
	if shutdownStarted.Load() {
		vclpoll.CloseVLSH(vlsh)
		return nil, opError("dial", network, address, ErrClosed)
	}

	conn := newTCPConn(vlsh)
	conn.peerAddr = addr
	live.addConn(conn)
	conn.tracked.Store(true)
	return conn, nil
}

// connectReadyEvents is the epoll event mask a connect waits on. EPOLLOUT
// fires on successful handshake; EPOLLERR / EPOLLHUP fire on refused,
// unreachable, and other terminal failures. VPP delivers both through the
// same session-event path, so waiting on their union lets one waiter cover
// both outcomes; sessionConnectError() then disambiguates.
const connectReadyEvents = 0x004 | 0x008 | 0x010 // EPOLLOUT | EPOLLERR | EPOLLHUP

// dialHappyEyeballs implements RFC 6555/8305 for concurrent dual-stack connect.
func (d *Dialer) dialHappyEyeballs(ctx context.Context, network, address string, addrs []*net.TCPAddr) (net.Conn, error) {
	sorted := interleaveAddrs(addrs)
	fallbackDelay := d.FallbackDelay
	if fallbackDelay == 0 {
		fallbackDelay = defaultFallbackDelay
	}

	type result struct {
		conn net.Conn
		err  error
	}

	ctx, cancel := context.WithCancel(ctx)
	results := make(chan result, len(sorted))
	start := func(addr *net.TCPAddr) {
		go func() {
			conn, err := d.dialSingleTCP(ctx, network, address, addr)
			results <- result{conn: conn, err: err}
		}()
	}
	cleanup := func(outstanding int) {
		cancel()
		if outstanding == 0 {
			return
		}
		go func() {
			for i := 0; i < outstanding; i++ {
				r := <-results
				if r.conn != nil {
					_ = r.conn.Close()
				}
			}
		}()
	}
	defer cancel()

	start(sorted[0])
	timer := time.NewTimer(fallbackDelay)
	defer timer.Stop()

	outstanding := 1
	nextIdx := 1
	for outstanding > 0 {
		select {
		case r := <-results:
			outstanding--
			if r.err == nil {
				cleanup(outstanding)
				return r.conn, nil
			}
			if outstanding == 0 && nextIdx >= len(sorted) {
				return nil, r.err
			}
			if nextIdx < len(sorted) {
				start(sorted[nextIdx])
				nextIdx++
				outstanding++
			}
		case <-timer.C:
			if nextIdx < len(sorted) {
				start(sorted[nextIdx])
				nextIdx++
				outstanding++
				timer.Reset(fallbackDelay)
			}
		case <-ctx.Done():
			cleanup(outstanding)
			return nil, opError("dial", network, address, ctx.Err())
		}
	}
	return nil, opError("dial", network, address, &net.DNSError{Err: "no address succeeded", Name: address})
}

func interruptedConnectError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return ErrClosed
}

func (d *Dialer) dialUDP(ctx context.Context, network, address string) (net.Conn, error) {
	endDial := live.beginDial()
	defer endDial()

	addr, err := resolveUDPAddr(ctx, network, address)
	if err != nil {
		return nil, opError("dial", network, address, err)
	}

	vlsh, immediate, err := connectUDPStart(addr)
	if err != nil {
		return nil, opError("dial", network, address, err)
	}
	if !immediate {
		if ok := vclpoll.PollWaitContext(vlsh, connectReadyEvents, ctx.Done()); !ok {
			vclpoll.CloseVLSH(vlsh)
			return nil, opError("dial", network, address, interruptedConnectError(ctx))
		}
		if err := vclpoll.SessionConnectError(vlsh); err != nil {
			vclpoll.CloseVLSH(vlsh)
			return nil, opError("dial", network, address, err)
		}
	}
	if err := ctx.Err(); err != nil {
		vclpoll.CloseVLSH(vlsh)
		return nil, opError("dial", network, address, err)
	}
	if shutdownStarted.Load() {
		vclpoll.CloseVLSH(vlsh)
		return nil, opError("dial", network, address, ErrClosed)
	}

	conn := newUDPConn(vlsh, nil, true)
	conn.peerAddr = addr
	live.addConn(conn)
	conn.tracked.Store(true)
	return conn, nil
}

// connectStart initiates a non-blocking TCP connect to addr.
func connectStart(addr *net.TCPAddr) (vclpoll.VLSH, bool, error) {
	if addr.IP.To4() != nil {
		var ip4 [4]byte
		copy(ip4[:], addr.IP.To4())
		return vclpoll.ConnectTCP4Start(ip4, uint16(addr.Port))
	}
	var ip6 [16]byte
	copy(ip6[:], addr.IP.To16())
	return vclpoll.ConnectTCP6Start(ip6, uint16(addr.Port))
}

// connectUDPStart initiates a non-blocking UDP connect to addr.
func connectUDPStart(addr *net.UDPAddr) (vclpoll.VLSH, bool, error) {
	if addr.IP.To4() != nil {
		var ip4 [4]byte
		copy(ip4[:], addr.IP.To4())
		return vclpoll.ConnectUDP4Start(ip4, uint16(addr.Port))
	}
	var ip6 [16]byte
	copy(ip6[:], addr.IP.To16())
	return vclpoll.ConnectUDP6Start(ip6, uint16(addr.Port))
}

// interleaveAddrs sorts addresses per RFC 8305: alternate between IPv6 and IPv4,
// starting with IPv6 (preferred).
func interleaveAddrs(addrs []*net.TCPAddr) []*net.TCPAddr {
	var v4, v6 []*net.TCPAddr
	for _, a := range addrs {
		if a.IP.To4() != nil {
			v4 = append(v4, a)
		} else {
			v6 = append(v6, a)
		}
	}

	result := make([]*net.TCPAddr, 0, len(addrs))
	i, j := 0, 0
	preferV6 := true
	for i < len(v6) || j < len(v4) {
		if preferV6 && i < len(v6) {
			result = append(result, v6[i])
			i++
		} else if j < len(v4) {
			result = append(result, v4[j])
			j++
		} else if i < len(v6) {
			result = append(result, v6[i])
			i++
		}
		preferV6 = !preferV6
	}
	return result
}
