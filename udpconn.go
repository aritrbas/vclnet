package vclnet

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aritrbas/vclnet/internal/vclpoll"
)

var (
	errWriteToConnected = errors.New("vclnet: use of WriteTo with pre-connected UDP connection")
	errMissingPeer      = errors.New("vclnet: destination address required")
)

// udpConn implements net.PacketConn for unconnected UDP and net.Conn for
// connected UDP (created via Dial).
type udpConn struct {
	vlsh      vclpoll.VLSH
	localAddr *net.UDPAddr
	peerAddr  *net.UDPAddr
	connected bool
	addrMu    sync.Mutex

	closed    atomic.Bool
	closeOnce sync.Once
	tracked   atomic.Bool

	readDeadline  *deadlineState
	writeDeadline *deadlineState
}

var (
	_ net.PacketConn = (*udpConn)(nil)
	_ net.Conn       = (*udpConn)(nil)
)

func newUDPConn(vlsh vclpoll.VLSH, localAddr *net.UDPAddr, connected bool) *udpConn {
	return &udpConn{
		vlsh:          vlsh,
		localAddr:     localAddr,
		connected:     connected,
		readDeadline:  newDeadlineState(),
		writeDeadline: newDeadlineState(),
	}
}

// --- net.PacketConn methods ---

func (c *udpConn) ReadFrom(p []byte) (int, net.Addr, error) {
	for {
		if err := c.readStateError(); err != nil {
			return 0, nil, opErrorAddr("read", c.localAddrSafe(), err)
		}
		n, info, err := vclpoll.RecvFromContext(c.vlsh, p, c.readDeadline.waitChannel())
		if retry, mapped := mapIOError(err); retry {
			continue
		} else if mapped != nil {
			return 0, nil, opErrorAddr("read", c.localAddrSafe(), mapped)
		}
		return n, udpAddrFromInfo(info), nil
	}
}

func (c *udpConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	if err := c.writeStateError(); err != nil {
		return 0, opErrorAddr("write", c.localAddrSafe(), err)
	}
	if c.connected {
		return 0, opErrorAddr("write", c.localAddrSafe(), errWriteToConnected)
	}
	if addr == nil {
		return 0, opErrorAddr("write", c.localAddrSafe(), &net.AddrError{
			Err:  "nil UDP address",
			Addr: "<nil>",
		})
	}
	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok {
		return 0, opErrorAddr("write", c.localAddrSafe(), &net.AddrError{
			Err:  "non-UDP address",
			Addr: addr.String(),
		})
	}
	if udpAddr.Port < 0 || udpAddr.Port > 65535 {
		return 0, opErrorAddr("write", c.localAddrSafe(), &net.AddrError{
			Err:  "invalid port",
			Addr: udpAddr.String(),
		})
	}

	info := vclpoll.AddrInfo{Port: uint16(udpAddr.Port)}
	if ip4 := udpAddr.IP.To4(); ip4 != nil {
		info.IsV4 = true
		copy(info.IP[:], ip4)
	} else if ip6 := udpAddr.IP.To16(); ip6 != nil {
		copy(info.IP[:], ip6)
	} else {
		return 0, opErrorAddr("write", c.localAddrSafe(), &net.AddrError{
			Err:  "invalid IP address",
			Addr: udpAddr.String(),
		})
	}

	for {
		n, err := vclpoll.SendToContext(c.vlsh, p, info, c.writeDeadline.waitChannel())
		if retry, mapped := mapIOError(err); retry {
			if stateErr := c.writeStateError(); stateErr != nil {
				return 0, opErrorAddr("write", c.localAddrSafe(), stateErr)
			}
			continue
		} else if mapped != nil {
			return 0, opErrorAddr("write", c.localAddrSafe(), mapped)
		}
		return n, nil
	}
}

// --- net.Conn methods (for connected UDP) ---

func (c *udpConn) Read(p []byte) (int, error) {
	for {
		if err := c.readStateError(); err != nil {
			return 0, opErrorAddr("read", c.remoteAddrSafe(), err)
		}
		n, err := vclpoll.ReadContext(c.vlsh, p, c.readDeadline.waitChannel())
		if retry, mapped := mapIOError(err); retry {
			continue
		} else if mapped != nil {
			return 0, opErrorAddr("read", c.remoteAddrSafe(), mapped)
		}
		return n, nil
	}
}

func (c *udpConn) Write(p []byte) (int, error) {
	if err := c.writeStateError(); err != nil {
		return 0, opErrorAddr("write", c.remoteAddrSafe(), err)
	}
	if !c.connected {
		return 0, opErrorAddr("write", c.localAddrSafe(), errMissingPeer)
	}
	for {
		if err := c.writeStateError(); err != nil {
			return 0, opErrorAddr("write", c.remoteAddrSafe(), err)
		}
		n, err := vclpoll.WriteContext(c.vlsh, p, c.writeDeadline.waitChannel())
		if retry, mapped := mapIOError(err); retry {
			continue
		} else if mapped != nil {
			return 0, opErrorAddr("write", c.remoteAddrSafe(), mapped)
		}
		return n, nil
	}
}

func (c *udpConn) readStateError() error  { return ioStateError(&c.closed, c.readDeadline) }
func (c *udpConn) writeStateError() error { return ioStateError(&c.closed, c.writeDeadline) }

func (c *udpConn) Close() error {
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
		return opErrorAddr("close", c.localAddrSafe(), err)
	}
	return nil
}

func (c *udpConn) LocalAddr() net.Addr {
	c.addrMu.Lock()
	defer c.addrMu.Unlock()
	if c.localAddr != nil {
		return c.localAddr
	}
	if c.closed.Load() || shutdownStarted.Load() {
		return &net.UDPAddr{}
	}
	info, err := vclpoll.GetLocalAddr(c.vlsh)
	if err != nil {
		return &net.UDPAddr{}
	}
	c.localAddr = udpAddrFromInfo(info)
	return c.localAddr
}

func (c *udpConn) RemoteAddr() net.Addr {
	c.addrMu.Lock()
	defer c.addrMu.Unlock()
	if c.peerAddr != nil {
		return c.peerAddr
	}
	return nil
}

func (c *udpConn) SetDeadline(t time.Time) error {
	if err := c.deadlineStateError(); err != nil {
		return opErrorAddr("set", c.remoteAddrSafe(), err)
	}
	c.readDeadline.set(t)
	c.writeDeadline.set(t)
	return nil
}

func (c *udpConn) SetReadDeadline(t time.Time) error {
	if err := c.deadlineStateError(); err != nil {
		return opErrorAddr("set", c.remoteAddrSafe(), err)
	}
	c.readDeadline.set(t)
	return nil
}

func (c *udpConn) SetWriteDeadline(t time.Time) error {
	if err := c.deadlineStateError(); err != nil {
		return opErrorAddr("set", c.remoteAddrSafe(), err)
	}
	c.writeDeadline.set(t)
	return nil
}

func (c *udpConn) deadlineStateError() error { return closedStateError(&c.closed) }

func (c *udpConn) localAddrSafe() net.Addr {
	c.addrMu.Lock()
	defer c.addrMu.Unlock()
	if c.localAddr != nil {
		return c.localAddr
	}
	return &net.UDPAddr{}
}

func (c *udpConn) remoteAddrSafe() net.Addr {
	c.addrMu.Lock()
	defer c.addrMu.Unlock()
	if c.peerAddr != nil {
		return c.peerAddr
	}
	return &net.UDPAddr{}
}
