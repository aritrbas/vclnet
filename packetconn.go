package vclnet

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aritrbas/vclnet/internal/vclpoll"
)

// ErrUnknownPeer is returned by WriteTo when the destination address has not
// been seen via ReadFrom. VPP's UDP model is session-based: each peer gets
// its own VLS session only after it sends data to this listener.
var ErrUnknownPeer = errors.New("vclnet: unknown peer; WriteTo requires prior ReadFrom from that address")

// packetConn implements net.PacketConn over VPP's session-based UDP model.
// A background accept loop creates per-peer sessions. ReadFrom fans in data
// from all peers; WriteTo routes to a known peer's session.
type packetConn struct {
	listenerVLSH vclpoll.VLSH
	localAddr    *net.UDPAddr

	closed    atomic.Bool
	closeOnce sync.Once
	tracked   atomic.Bool
	stopCh    chan struct{}
	wg        sync.WaitGroup

	// Per-peer session tracking.
	peerMu   sync.RWMutex
	peers    map[peerKey]*peerSession
	incoming chan incomingDatagram

	readDeadline  *deadlineState
	writeDeadline *deadlineState
}

type peerKey struct {
	ip   [16]byte
	port uint16
}

type peerSession struct {
	vlsh vclpoll.VLSH
	addr *net.UDPAddr
}

type incomingDatagram struct {
	data []byte
	addr *net.UDPAddr
	err  error
}

var _ net.PacketConn = (*packetConn)(nil)

func newPacketConn(listenerVLSH vclpoll.VLSH, localAddr *net.UDPAddr) *packetConn {
	pc := &packetConn{
		listenerVLSH:  listenerVLSH,
		localAddr:     localAddr,
		stopCh:        make(chan struct{}),
		peers:         make(map[peerKey]*peerSession),
		incoming:      make(chan incomingDatagram, 64),
		readDeadline:  newDeadlineState(),
		writeDeadline: newDeadlineState(),
	}
	pc.wg.Add(1)
	go pc.acceptLoop()
	return pc
}

func (pc *packetConn) acceptLoop() {
	defer pc.wg.Done()
	for {
		select {
		case <-pc.stopCh:
			return
		default:
		}

		connVLSH, addrInfo, err := vclpoll.AcceptFullContext(pc.listenerVLSH, pc.stopCh)
		if err != nil {
			select {
			case <-pc.stopCh:
			default:
				if !errors.Is(err, vclpoll.ErrClosed) {
					pc.incoming <- incomingDatagram{err: err}
				}
			}
			return
		}

		peerAddr := udpAddrFromInfo(addrInfo)
		key := addrToKey(peerAddr)

		pc.peerMu.Lock()
		ps := &peerSession{vlsh: connVLSH, addr: peerAddr}
		pc.peers[key] = ps
		pc.peerMu.Unlock()

		pc.wg.Add(1)
		go pc.peerReadLoop(ps)
	}
}

func (pc *packetConn) peerReadLoop(ps *peerSession) {
	defer pc.wg.Done()
	buf := make([]byte, 65536)
	for {
		select {
		case <-pc.stopCh:
			return
		default:
		}

		n, err := vclpoll.ReadContext(ps.vlsh, buf, pc.stopCh)
		if err != nil {
			select {
			case <-pc.stopCh:
			default:
				if !errors.Is(err, vclpoll.ErrClosed) {
					pc.incoming <- incomingDatagram{err: err}
				}
			}
			return
		}
		if n == 0 {
			return
		}

		data := make([]byte, n)
		copy(data, buf[:n])

		select {
		case pc.incoming <- incomingDatagram{data: data, addr: ps.addr}:
		case <-pc.stopCh:
			return
		}
	}
}

func (pc *packetConn) ReadFrom(p []byte) (int, net.Addr, error) {
	for {
		if pc.closed.Load() || shutdownStarted.Load() {
			return 0, nil, opErrorAddr("read", pc.localAddr, ErrClosed)
		}
		if pc.readDeadline.expired() {
			return 0, nil, opErrorAddr("read", pc.localAddr, &timeoutError{})
		}

		waitCh := pc.readDeadline.waitChannel()
		if waitCh == nil {
			select {
			case dg := <-pc.incoming:
				if dg.err != nil {
					return 0, nil, opErrorAddr("read", pc.localAddr, dg.err)
				}
				n := copy(p, dg.data)
				return n, dg.addr, nil
			case <-pc.stopCh:
				return 0, nil, opErrorAddr("read", pc.localAddr, ErrClosed)
			}
		}

		select {
		case dg := <-pc.incoming:
			if dg.err != nil {
				return 0, nil, opErrorAddr("read", pc.localAddr, dg.err)
			}
			n := copy(p, dg.data)
			return n, dg.addr, nil
		case <-waitCh:
			if pc.readDeadline.expired() {
				return 0, nil, opErrorAddr("read", pc.localAddr, &timeoutError{})
			}
			continue
		case <-pc.stopCh:
			return 0, nil, opErrorAddr("read", pc.localAddr, ErrClosed)
		}
	}
}

func (pc *packetConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	if pc.closed.Load() || shutdownStarted.Load() {
		return 0, opErrorAddr("write", pc.localAddr, ErrClosed)
	}
	if pc.writeDeadline.expired() {
		return 0, opErrorAddr("write", pc.localAddr, &timeoutError{})
	}
	if addr == nil {
		return 0, opErrorAddr("write", pc.localAddr, &net.AddrError{
			Err:  "nil UDP address",
			Addr: "<nil>",
		})
	}
	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok {
		return 0, opErrorAddr("write", pc.localAddr, &net.AddrError{
			Err:  "non-UDP address",
			Addr: addr.String(),
		})
	}

	key := addrToKey(udpAddr)
	pc.peerMu.RLock()
	ps, found := pc.peers[key]
	pc.peerMu.RUnlock()

	if !found {
		return 0, opErrorAddr("write", pc.localAddr, ErrUnknownPeer)
	}

	n, err := vclpoll.WriteContext(ps.vlsh, p, pc.writeDeadline.waitChannel())
	if errors.Is(err, vclpoll.ErrWaitCanceled) {
		if pc.closed.Load() || shutdownStarted.Load() {
			return 0, opErrorAddr("write", pc.localAddr, ErrClosed)
		}
		if pc.writeDeadline.expired() {
			return 0, opErrorAddr("write", pc.localAddr, &timeoutError{})
		}
	}
	if errors.Is(err, vclpoll.ErrClosed) {
		err = ErrClosed
	}
	if err != nil {
		return 0, opErrorAddr("write", pc.localAddr, err)
	}
	return n, nil
}

func (pc *packetConn) Close() error {
	var closeErr error
	pc.closeOnce.Do(func() {
		pc.closed.Store(true)
		close(pc.stopCh)
		pc.readDeadline.interrupt()
		pc.writeDeadline.interrupt()

		pc.wg.Wait()

		pc.peerMu.Lock()
		for _, ps := range pc.peers {
			_ = vclpoll.Close(ps.vlsh)
		}
		pc.peers = nil
		pc.peerMu.Unlock()

		if !shutdownStarted.Load() {
			closeErr = vclpoll.Close(pc.listenerVLSH)
		}
		if pc.tracked.Load() {
			live.removePacketConn(pc)
		}
	})
	if closeErr != nil {
		return opErrorAddr("close", pc.localAddr, closeErr)
	}
	return nil
}

func (pc *packetConn) LocalAddr() net.Addr {
	return pc.localAddr
}

func (pc *packetConn) SetDeadline(t time.Time) error {
	if pc.closed.Load() || shutdownStarted.Load() {
		return opErrorAddr("set", pc.localAddr, ErrClosed)
	}
	pc.readDeadline.set(t)
	pc.writeDeadline.set(t)
	return nil
}

func (pc *packetConn) SetReadDeadline(t time.Time) error {
	if pc.closed.Load() || shutdownStarted.Load() {
		return opErrorAddr("set", pc.localAddr, ErrClosed)
	}
	pc.readDeadline.set(t)
	return nil
}

func (pc *packetConn) SetWriteDeadline(t time.Time) error {
	if pc.closed.Load() || shutdownStarted.Load() {
		return opErrorAddr("set", pc.localAddr, ErrClosed)
	}
	pc.writeDeadline.set(t)
	return nil
}

func addrToKey(addr *net.UDPAddr) peerKey {
	var key peerKey
	if ip4 := addr.IP.To4(); ip4 != nil {
		copy(key.ip[:], ip4)
	} else if ip6 := addr.IP.To16(); ip6 != nil {
		copy(key.ip[:], ip6)
	}
	key.port = uint16(addr.Port)
	return key
}
