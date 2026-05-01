package vclnet

import (
	"errors"
	"net"
	"syscall"
)

var (
	// ErrClosed is returned when an operation is attempted after a connection,
	// listener, or the package VCL application has been closed.
	ErrClosed = errors.New("vclnet: use of closed connection")

	// ErrMPTCP is retained for source compatibility. vclnet never asks the
	// kernel for MPTCP, so current code paths do not return it.
	ErrMPTCP = errors.New("vclnet: MPTCP not supported by VPP")
)

type unresolvedAddr struct {
	network string
	address string
}

func (a *unresolvedAddr) Network() string { return a.network }
func (a *unresolvedAddr) String() string  { return a.address }

// opError wraps an error into a *net.OpError for net-compatible callers.
func opError(op, network, addr string, err error) error {
	var target net.Addr
	if addr != "" {
		target = &unresolvedAddr{network: network, address: addr}
	}
	return &net.OpError{
		Op:   op,
		Net:  network,
		Addr: target,
		Err:  err,
	}
}

func opErrorAddr(op string, addr net.Addr, err error) error {
	network := ""
	if addr != nil {
		network = addr.Network()
	}
	return &net.OpError{
		Op:   op,
		Net:  network,
		Addr: addr,
		Err:  err,
	}
}

func IsTimeout(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	return errors.Is(err, syscall.ETIMEDOUT)
}

func IsConnectionRefused(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED)
}

func IsConnectionReset(err error) bool {
	return errors.Is(err, syscall.ECONNRESET)
}
