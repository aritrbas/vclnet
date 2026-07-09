package vclnet

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aritrbas/vclnet/internal/vclpoll"
)

// --- parseNetwork tests ---

func TestParseNetwork(t *testing.T) {
	tests := []struct {
		input   string
		wantV4  bool
		wantV6  bool
		wantErr bool
	}{
		{"tcp", false, false, false},
		{"tcp4", true, false, false},
		{"tcp6", false, true, false},
		{"udp", false, false, false},
		{"udp4", true, false, false},
		{"udp6", false, true, false},
		{"unix", false, false, true},
		{"", false, false, true},
		{"TCP", false, false, true}, // case-sensitive
		{"tcp7", false, false, true},
		{"ip4", false, false, true},
	}
	for _, tt := range tests {
		v4, v6, err := parseNetwork(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseNetwork(%q): err=%v, wantErr=%v", tt.input, err, tt.wantErr)
		}
		if v4 != tt.wantV4 {
			t.Errorf("parseNetwork(%q): ipv4Only=%v, want %v", tt.input, v4, tt.wantV4)
		}
		if v6 != tt.wantV6 {
			t.Errorf("parseNetwork(%q): ipv6Only=%v, want %v", tt.input, v6, tt.wantV6)
		}
		if tt.wantErr && err != nil {
			var unk net.UnknownNetworkError
			if !errors.As(err, &unk) {
				t.Errorf("parseNetwork(%q): error type=%T, want UnknownNetworkError", tt.input, err)
			}
		}
	}
}

// --- resolveAddr tests ---

func TestResolveAddrLiteralIPv4(t *testing.T) {
	addr, err := resolveAddr("tcp", "127.0.0.1:8080")
	if err != nil {
		t.Fatalf("resolveAddr: %v", err)
	}
	if !addr.IP.Equal(net.ParseIP("127.0.0.1")) {
		t.Errorf("IP=%v, want 127.0.0.1", addr.IP)
	}
	if addr.Port != 8080 {
		t.Errorf("Port=%d, want 8080", addr.Port)
	}
}

func TestResolveAddrLiteralIPv6(t *testing.T) {
	addr, err := resolveAddr("tcp6", "[::1]:443")
	if err != nil {
		t.Fatalf("resolveAddr: %v", err)
	}
	if !addr.IP.Equal(net.ParseIP("::1")) {
		t.Errorf("IP=%v, want ::1", addr.IP)
	}
	if addr.Port != 443 {
		t.Errorf("Port=%d, want 443", addr.Port)
	}
}

func TestResolveAddrEmptyHost(t *testing.T) {
	// Empty host on tcp → IPv4 unspecified.
	addr, err := resolveAddr("tcp", ":9876")
	if err != nil {
		t.Fatalf("resolveAddr(tcp, :9876): %v", err)
	}
	if !addr.IP.Equal(net.IPv4zero) {
		t.Errorf("IP=%v, want 0.0.0.0", addr.IP)
	}
	if addr.Port != 9876 {
		t.Errorf("Port=%d, want 9876", addr.Port)
	}

	// Empty host on tcp6 → IPv6 unspecified.
	addr, err = resolveAddr("tcp6", ":9876")
	if err != nil {
		t.Fatalf("resolveAddr(tcp6, :9876): %v", err)
	}
	if !addr.IP.Equal(net.IPv6zero) {
		t.Errorf("IP=%v, want [::]", addr.IP)
	}
}

func TestResolveAddrIPv4OnTcp6Rejected(t *testing.T) {
	_, err := resolveAddr("tcp6", "192.168.1.1:80")
	if err == nil {
		t.Fatal("expected error for IPv4 on tcp6")
	}
	var addrErr *net.AddrError
	if !errors.As(err, &addrErr) {
		t.Errorf("error type=%T, want *net.AddrError", err)
	}
}

func TestResolveAddrIPv6OnTcp4Rejected(t *testing.T) {
	_, err := resolveAddr("tcp4", "[::1]:80")
	if err == nil {
		t.Fatal("expected error for IPv6 on tcp4")
	}
	var addrErr *net.AddrError
	if !errors.As(err, &addrErr) {
		t.Errorf("error type=%T, want *net.AddrError", err)
	}
}

func TestResolveAddrBadFormat(t *testing.T) {
	// net.SplitHostPort fails without a colon separator.
	_, err := resolveAddr("tcp", "127.0.0.1")
	if err == nil {
		t.Fatal("expected error for missing port separator")
	}
	// Multiple colons without brackets.
	_, err = resolveAddr("tcp", "::1:80")
	if err == nil {
		t.Fatal("expected error for ambiguous IPv6 without brackets")
	}
}

func TestResolveAddrMissingPort(t *testing.T) {
	_, err := resolveAddr("tcp", "127.0.0.1")
	if err == nil {
		t.Fatal("expected error for missing port")
	}
}

func TestResolveAddrInvalidHost(t *testing.T) {
	_, err := resolveAddr("tcp", "not-a-valid-!!!-host:80")
	if err == nil {
		t.Fatal("expected error for invalid host")
	}
}

func TestResolveAddrZeroAddr(t *testing.T) {
	addr, err := resolveAddr("tcp4", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("resolveAddr: %v", err)
	}
	if addr.Port != 0 {
		t.Errorf("Port=%d, want 0", addr.Port)
	}
	if !addr.IP.Equal(net.IPv4zero) {
		t.Errorf("IP=%v, want 0.0.0.0", addr.IP)
	}
}

func TestResolveAddrIPv6Full(t *testing.T) {
	addr, err := resolveAddr("tcp6", "[2001:db8::1]:8443")
	if err != nil {
		t.Fatalf("resolveAddr: %v", err)
	}
	expected := net.ParseIP("2001:db8::1")
	if !addr.IP.Equal(expected) {
		t.Errorf("IP=%v, want %v", addr.IP, expected)
	}
	if addr.Port != 8443 {
		t.Errorf("Port=%d, want 8443", addr.Port)
	}
}

// --- addrFromInfo tests ---

func TestAddrFromInfoIPv4(t *testing.T) {
	info := vclpoll.AddrInfo{
		IP:   [16]byte{10, 0, 0, 1},
		Port: 8080,
		IsV4: true,
	}
	addr := addrFromInfo(info)
	if !addr.IP.Equal(net.ParseIP("10.0.0.1")) {
		t.Errorf("IP=%v, want 10.0.0.1", addr.IP)
	}
	if addr.Port != 8080 {
		t.Errorf("Port=%d, want 8080", addr.Port)
	}
}

func TestAddrFromInfoIPv6(t *testing.T) {
	info := vclpoll.AddrInfo{
		IP:   [16]byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		Port: 443,
		IsV4: false,
	}
	addr := addrFromInfo(info)
	expected := net.ParseIP("2001:db8::1")
	if !addr.IP.Equal(expected) {
		t.Errorf("IP=%v, want %v", addr.IP, expected)
	}
	if addr.Port != 443 {
		t.Errorf("Port=%d, want 443", addr.Port)
	}
}

func TestAddrFromInfoZero(t *testing.T) {
	info := vclpoll.AddrInfo{
		IP:   [16]byte{},
		Port: 0,
		IsV4: true,
	}
	addr := addrFromInfo(info)
	if !addr.IP.Equal(net.IPv4zero) {
		t.Errorf("IP=%v, want 0.0.0.0", addr.IP)
	}
	if addr.Port != 0 {
		t.Errorf("Port=%d, want 0", addr.Port)
	}
}

// --- error wrapping tests ---

func TestOpError(t *testing.T) {
	err := opError("listen", "tcp", ":8080", ErrClosed)
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("type=%T, want *net.OpError", err)
	}
	if opErr.Op != "listen" {
		t.Errorf("Op=%q, want \"listen\"", opErr.Op)
	}
	if opErr.Net != "tcp" {
		t.Errorf("Net=%q, want \"tcp\"", opErr.Net)
	}
	if !errors.Is(opErr.Err, ErrClosed) {
		t.Errorf("Err=%v, want ErrClosed", opErr.Err)
	}
}

func TestOpErrorAddr(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("10.0.0.1"), Port: 80}
	err := opErrorAddr("read", addr, &timeoutError{})
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("type=%T, want *net.OpError", err)
	}
	if opErr.Op != "read" {
		t.Errorf("Op=%q, want \"read\"", opErr.Op)
	}
	if opErr.Addr.String() != "10.0.0.1:80" {
		t.Errorf("Addr=%v, want 10.0.0.1:80", opErr.Addr)
	}
}

// --- timeoutError tests ---

func TestTimeoutError(t *testing.T) {
	var err net.Error = &timeoutError{}
	if !err.Timeout() {
		t.Error("Timeout() should be true")
	}
	if err.Error() != "i/o timeout" {
		t.Errorf("Error()=%q, want \"i/o timeout\"", err.Error())
	}
	if !err.Temporary() {
		t.Error("Temporary() should be true for a timeout (parity with stdlib *net.OpError)")
	}
}

// --- tcpConn state tests (no VPP needed — only testing state logic) ---

func TestTCPConnZeroLengthIO(t *testing.T) {
	c := newTCPConn(0)
	c.localAddr = &net.TCPAddr{}
	c.peerAddr = &net.TCPAddr{}
	if n, err := c.Read(nil); n != 0 || err != nil {
		t.Fatalf("Read(nil) = (%d, %v), want (0, nil)", n, err)
	}
	if n, err := c.Write(nil); n != 0 || err != nil {
		t.Fatalf("Write(nil) = (%d, %v), want (0, nil)", n, err)
	}
}

func TestTcpConnDeadlines(t *testing.T) {
	c := newTCPConn(0) // vlsh=0, won't be used for I/O in these tests
	c.peerAddr = &net.TCPAddr{IP: net.ParseIP("1.2.3.4"), Port: 80}

	// Initially no deadline.
	dl := c.readDeadline.value()
	if !dl.IsZero() {
		t.Error("initial readDeadline should be zero")
	}

	// Set read deadline.
	future := time.Now().Add(time.Hour)
	if err := c.SetReadDeadline(future); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	dl = c.readDeadline.value()
	if !dl.Equal(future) {
		t.Errorf("readDeadline=%v, want %v", dl, future)
	}

	// Set write deadline.
	if err := c.SetWriteDeadline(future); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	dl = c.writeDeadline.value()
	if !dl.Equal(future) {
		t.Errorf("writeDeadline=%v, want %v", dl, future)
	}

	// Set both via SetDeadline.
	past := time.Now().Add(-time.Hour)
	if err := c.SetDeadline(past); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
	dl = c.readDeadline.value()
	if !dl.Equal(past) {
		t.Errorf("after SetDeadline: readDeadline=%v, want %v", dl, past)
	}
	dl = c.writeDeadline.value()
	if !dl.Equal(past) {
		t.Errorf("after SetDeadline: writeDeadline=%v, want %v", dl, past)
	}
}

func TestTcpConnReadAfterClose(t *testing.T) {
	c := newTCPConn(0)
	c.peerAddr = &net.TCPAddr{IP: net.ParseIP("1.2.3.4"), Port: 80}
	c.closed.Store(true)

	_, err := c.Read(make([]byte, 10))
	if err == nil {
		t.Fatal("Read on closed conn should return error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("type=%T, want *net.OpError", err)
	}
	if !errors.Is(opErr.Err, ErrClosed) {
		t.Errorf("inner err=%v, want ErrClosed", opErr.Err)
	}
}

func TestTcpConnWriteAfterClose(t *testing.T) {
	c := newTCPConn(0)
	c.peerAddr = &net.TCPAddr{IP: net.ParseIP("1.2.3.4"), Port: 80}
	c.closed.Store(true)

	_, err := c.Write([]byte("hello"))
	if err == nil {
		t.Fatal("Write on closed conn should return error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("type=%T, want *net.OpError", err)
	}
	if !errors.Is(opErr.Err, ErrClosed) {
		t.Errorf("inner err=%v, want ErrClosed", opErr.Err)
	}
}

func TestTcpConnReadDeadlineExpired(t *testing.T) {
	c := newTCPConn(0)
	c.peerAddr = &net.TCPAddr{IP: net.ParseIP("1.2.3.4"), Port: 80}
	c.SetReadDeadline(time.Now().Add(-time.Second)) // already expired

	_, err := c.Read(make([]byte, 10))
	if err == nil {
		t.Fatal("Read with expired deadline should return error")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) {
		t.Fatalf("type=%T, want net.Error", err)
	}
	if !netErr.Timeout() {
		t.Error("error should be a timeout")
	}
}

func TestTcpConnWriteDeadlineExpired(t *testing.T) {
	c := newTCPConn(0)
	c.peerAddr = &net.TCPAddr{IP: net.ParseIP("1.2.3.4"), Port: 80}
	c.SetWriteDeadline(time.Now().Add(-time.Second)) // already expired

	_, err := c.Write([]byte("hello"))
	if err == nil {
		t.Fatal("Write with expired deadline should return error")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) {
		t.Fatalf("type=%T, want net.Error", err)
	}
	if !netErr.Timeout() {
		t.Error("error should be a timeout")
	}
}

func TestTcpConnRemoteAddrCached(t *testing.T) {
	c := newTCPConn(0)
	expected := &net.TCPAddr{IP: net.ParseIP("10.0.0.1"), Port: 9999}
	c.peerAddr = expected

	// Should return cached value.
	got := c.RemoteAddr()
	if got.String() != "10.0.0.1:9999" {
		t.Errorf("RemoteAddr()=%v, want 10.0.0.1:9999", got)
	}
}

func TestTcpConnLocalAddrCached(t *testing.T) {
	c := newTCPConn(0)
	expected := &net.TCPAddr{IP: net.ParseIP("192.168.1.1"), Port: 5555}
	c.localAddr = expected

	got := c.LocalAddr()
	if got.String() != "192.168.1.1:5555" {
		t.Errorf("LocalAddr()=%v, want 192.168.1.1:5555", got)
	}
}

// TestTcpConnClosedStateBlocksIO verifies the publicly observable closed-state
// contract: once c.closed is true (which Close() sets atomically inside its
// sync.Once), every subsequent Read/Write fails with *net.OpError(ErrClosed).
// This is the only piece of the Close path testable without VPP, because
// vclpoll.Close is a cgo call into vls_close and crashes on an invalid handle.
// Idempotency of Close itself is exercised by the integration tests.
func TestTcpConnClosedStateBlocksIO(t *testing.T) {
	c := newTCPConn(0)
	c.peerAddr = &net.TCPAddr{IP: net.ParseIP("1.2.3.4"), Port: 80}
	c.closed.Store(true)

	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Error("Read on closed conn returned no error")
	} else {
		var opErr *net.OpError
		if !errors.As(err, &opErr) || !errors.Is(opErr.Err, ErrClosed) {
			t.Errorf("Read err=%v, want *net.OpError wrapping ErrClosed", err)
		}
	}
	if _, err := c.Write([]byte("x")); err == nil {
		t.Error("Write on closed conn returned no error")
	} else {
		var opErr *net.OpError
		if !errors.As(err, &opErr) || !errors.Is(opErr.Err, ErrClosed) {
			t.Errorf("Write err=%v, want *net.OpError wrapping ErrClosed", err)
		}
	}
}

// --- tcpListener state tests ---

func TestTcpListenerAddr(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("0.0.0.0"), Port: 8080}
	l := newTCPListener(0, addr, "tcp")
	got := l.Addr()
	if got.String() != "0.0.0.0:8080" {
		t.Errorf("Addr()=%v, want 0.0.0.0:8080", got)
	}
	if got.Network() != "tcp" {
		t.Errorf("Network()=%v, want tcp", got.Network())
	}
}

func TestTcpListenerAcceptAfterClose(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("0.0.0.0"), Port: 8080}
	l := newTCPListener(0, addr, "tcp")
	l.closed.Store(true)

	_, err := l.Accept()
	if err == nil {
		t.Fatal("Accept on closed listener should return error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("type=%T, want *net.OpError", err)
	}
	if !errors.Is(opErr.Err, ErrClosed) {
		t.Errorf("inner err=%v, want ErrClosed", opErr.Err)
	}
}

// --- sentinel error tests ---

func TestErrClosedMessage(t *testing.T) {
	if ErrClosed.Error() != "vclnet: use of closed connection" {
		t.Errorf("ErrClosed.Error()=%q", ErrClosed.Error())
	}
}

func TestErrMPTCPMessage(t *testing.T) {
	if ErrMPTCP.Error() != "vclnet: MPTCP not supported by VPP" {
		t.Errorf("ErrMPTCP.Error()=%q", ErrMPTCP.Error())
	}
}

// --- interface compliance tests ---

func TestNetConnInterface(t *testing.T) {
	var _ net.Conn = (*tcpConn)(nil)
}

func TestNetListenerInterface(t *testing.T) {
	var _ net.Listener = (*tcpListener)(nil)
}

func TestNetErrorInterface(t *testing.T) {
	var _ net.Error = (*timeoutError)(nil)
}

func TestNetPacketConnInterface(t *testing.T) {
	var _ net.PacketConn = (*udpConn)(nil)
}

func TestUDPConnAsNetConn(t *testing.T) {
	var _ net.Conn = (*udpConn)(nil)
}

// --- Listen / Dial / DialTimeout validation tests ---
//
// These exercise the early-return error paths of the public Listen, Dial,
// and DialTimeout functions. All failure cases here are caught before any
// cgo call into VLS, so they require neither VPP nor VCL_CONFIG.

func TestListenInvalidNetwork(t *testing.T) {
	_, err := Listen("udp", "127.0.0.1:0")
	if err == nil {
		t.Fatal("Listen with invalid network returned no error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("err type=%T, want *net.OpError", err)
	}
	if opErr.Op != "listen" || opErr.Net != "udp" {
		t.Errorf("Op=%q Net=%q, want listen/udp", opErr.Op, opErr.Net)
	}
	var unk net.UnknownNetworkError
	if !errors.As(err, &unk) {
		t.Errorf("inner err type=%T, want net.UnknownNetworkError", opErr.Err)
	}
}

func TestListenInvalidAddress(t *testing.T) {
	_, err := Listen("tcp4", "127.0.0.1") // missing port
	if err == nil {
		t.Fatal("Listen with bad address returned no error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("err type=%T, want *net.OpError", err)
	}
	if opErr.Op != "listen" {
		t.Errorf("Op=%q, want listen", opErr.Op)
	}
}

func TestListenIPv4OnTcp6(t *testing.T) {
	_, err := Listen("tcp6", "127.0.0.1:0")
	if err == nil {
		t.Fatal("Listen tcp6 with IPv4 literal returned no error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("err type=%T, want *net.OpError", err)
	}
	var addrErr *net.AddrError
	if !errors.As(err, &addrErr) {
		t.Errorf("inner err type=%T, want *net.AddrError", opErr.Err)
	}
}

func TestDialInvalidNetwork(t *testing.T) {
	_, err := Dial("unix", "/tmp/x")
	if err == nil {
		t.Fatal("Dial with invalid network returned no error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("err type=%T, want *net.OpError", err)
	}
	if opErr.Op != "dial" || opErr.Net != "unix" {
		t.Errorf("Op=%q Net=%q, want dial/unix", opErr.Op, opErr.Net)
	}
	var unk net.UnknownNetworkError
	if !errors.As(err, &unk) {
		t.Errorf("inner err type=%T, want net.UnknownNetworkError", opErr.Err)
	}
}

func TestDialInvalidAddress(t *testing.T) {
	_, err := Dial("tcp4", "no-port-here")
	if err == nil {
		t.Fatal("Dial with bad address returned no error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("err type=%T, want *net.OpError", err)
	}
	if opErr.Op != "dial" {
		t.Errorf("Op=%q, want dial", opErr.Op)
	}
}

func TestDialTimeoutInvalidNetwork(t *testing.T) {
	// DialTimeout delegates to DialContext with a derived context deadline;
	// we lock in the contract that validation errors still propagate as
	// *net.OpError with Op=="dial".
	_, err := DialTimeout("bogus", "127.0.0.1:0", time.Second)
	if err == nil {
		t.Fatal("DialTimeout with invalid network returned no error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("err type=%T, want *net.OpError", err)
	}
	if opErr.Op != "dial" || opErr.Net != "bogus" {
		t.Errorf("Op=%q Net=%q, want dial/bogus", opErr.Op, opErr.Net)
	}
}

func TestDialTimeoutInvalidAddress(t *testing.T) {
	_, err := DialTimeout("tcp6", "10.0.0.1:80", time.Second) // IPv4 lit on tcp6
	if err == nil {
		t.Fatal("DialTimeout with mismatched network returned no error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("err type=%T, want *net.OpError", err)
	}
	var addrErr *net.AddrError
	if !errors.As(err, &addrErr) {
		t.Errorf("inner err type=%T, want *net.AddrError", opErr.Err)
	}
}

// --- DialContext validation tests ---

func TestDialContextInvalidNetwork(t *testing.T) {
	_, err := DialContext(context.Background(), "unix", "/tmp/x")
	if err == nil {
		t.Fatal("DialContext with invalid network returned no error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("err type=%T, want *net.OpError", err)
	}
	if opErr.Op != "dial" || opErr.Net != "unix" {
		t.Errorf("Op=%q Net=%q, want dial/unix", opErr.Op, opErr.Net)
	}
}

func TestDialContextCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled
	_, err := DialContext(ctx, "tcp", "127.0.0.1:80")
	if err == nil {
		t.Fatal("DialContext with cancelled ctx returned no error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("err type=%T, want *net.OpError", err)
	}
	if !errors.Is(opErr.Err, context.Canceled) {
		t.Errorf("inner err=%v, want context.Canceled", opErr.Err)
	}
}

func TestDialContextUDPInvalidAddress(t *testing.T) {
	_, err := DialContext(context.Background(), "udp4", "[::1]:53")
	if err == nil {
		t.Fatal("DialContext udp4 with IPv6 address returned no error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("err type=%T, want *net.OpError", err)
	}
}

// --- ListenPacket validation tests ---

func TestListenPacketInvalidNetwork(t *testing.T) {
	_, err := ListenPacket("tcp", ":0")
	if err == nil {
		t.Fatal("ListenPacket with tcp returned no error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("err type=%T, want *net.OpError", err)
	}
}

func TestListenPacketInvalidNetworkString(t *testing.T) {
	_, err := ListenPacket("bogus", ":0")
	if err == nil {
		t.Fatal("ListenPacket with bogus returned no error")
	}
}

func TestListenPacketBadAddress(t *testing.T) {
	_, err := ListenPacket("udp4", "[::1]:0") // IPv6 on udp4
	if err == nil {
		t.Fatal("ListenPacket udp4 with IPv6 returned no error")
	}
}

// --- Happy Eyeballs interleave tests ---

func TestInterleaveAddrsV6Preferred(t *testing.T) {
	addrs := []*net.TCPAddr{
		{IP: net.ParseIP("10.0.0.1"), Port: 80},
		{IP: net.ParseIP("10.0.0.2"), Port: 80},
		{IP: net.ParseIP("2001:db8::1"), Port: 80},
		{IP: net.ParseIP("2001:db8::2"), Port: 80},
	}
	result := interleaveAddrs(addrs)
	if len(result) != 4 {
		t.Fatalf("len=%d, want 4", len(result))
	}
	// First should be IPv6 (preferred)
	if result[0].IP.To4() != nil {
		t.Errorf("result[0]=%v, want IPv6", result[0].IP)
	}
	// Second should be IPv4
	if result[1].IP.To4() == nil {
		t.Errorf("result[1]=%v, want IPv4", result[1].IP)
	}
	// Third should be IPv6
	if result[2].IP.To4() != nil {
		t.Errorf("result[2]=%v, want IPv6", result[2].IP)
	}
	// Fourth should be IPv4
	if result[3].IP.To4() == nil {
		t.Errorf("result[3]=%v, want IPv4", result[3].IP)
	}
}

func TestInterleaveAddrsOnlyV4(t *testing.T) {
	addrs := []*net.TCPAddr{
		{IP: net.ParseIP("10.0.0.1"), Port: 80},
		{IP: net.ParseIP("10.0.0.2"), Port: 80},
	}
	result := interleaveAddrs(addrs)
	if len(result) != 2 {
		t.Fatalf("len=%d, want 2", len(result))
	}
	for i, a := range result {
		if a.IP.To4() == nil {
			t.Errorf("result[%d]=%v, want IPv4", i, a.IP)
		}
	}
}

func TestInterleaveAddrsSingle(t *testing.T) {
	addrs := []*net.TCPAddr{
		{IP: net.ParseIP("::1"), Port: 443},
	}
	result := interleaveAddrs(addrs)
	if len(result) != 1 {
		t.Fatalf("len=%d, want 1", len(result))
	}
}

// --- UDP connection state tests ---

func TestUDPConnDeadlines(t *testing.T) {
	c := newUDPConn(0, &net.UDPAddr{IP: net.ParseIP("0.0.0.0"), Port: 9000}, false)

	dl := c.readDeadline.value()
	if !dl.IsZero() {
		t.Error("initial readDeadline should be zero")
	}

	future := time.Now().Add(time.Hour)
	c.SetReadDeadline(future)
	dl = c.readDeadline.value()
	if !dl.Equal(future) {
		t.Errorf("readDeadline=%v, want %v", dl, future)
	}

	c.SetWriteDeadline(future)
	dl = c.writeDeadline.value()
	if !dl.Equal(future) {
		t.Errorf("writeDeadline=%v, want %v", dl, future)
	}

	past := time.Now().Add(-time.Hour)
	c.SetDeadline(past)
	dl = c.readDeadline.value()
	if !dl.Equal(past) {
		t.Errorf("after SetDeadline: readDeadline=%v, want %v", dl, past)
	}
}

func TestUDPConnReadFromAfterClose(t *testing.T) {
	c := newUDPConn(0, &net.UDPAddr{IP: net.ParseIP("0.0.0.0"), Port: 9000}, false)
	c.closed.Store(true)

	_, _, err := c.ReadFrom(make([]byte, 10))
	if err == nil {
		t.Fatal("ReadFrom on closed conn should return error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("type=%T, want *net.OpError", err)
	}
	if !errors.Is(opErr.Err, ErrClosed) {
		t.Errorf("inner err=%v, want ErrClosed", opErr.Err)
	}
}

func TestUDPConnWriteToAfterClose(t *testing.T) {
	c := newUDPConn(0, &net.UDPAddr{IP: net.ParseIP("0.0.0.0"), Port: 9000}, false)
	c.closed.Store(true)

	_, err := c.WriteTo([]byte("hello"), &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 53})
	if err == nil {
		t.Fatal("WriteTo on closed conn should return error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("type=%T, want *net.OpError", err)
	}
	if !errors.Is(opErr.Err, ErrClosed) {
		t.Errorf("inner err=%v, want ErrClosed", opErr.Err)
	}
}

func TestUDPConnReadDeadlineExpired(t *testing.T) {
	c := newUDPConn(0, &net.UDPAddr{IP: net.ParseIP("0.0.0.0"), Port: 9000}, false)
	c.SetReadDeadline(time.Now().Add(-time.Second))

	_, _, err := c.ReadFrom(make([]byte, 10))
	if err == nil {
		t.Fatal("ReadFrom with expired deadline should return error")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) {
		t.Fatalf("type=%T, want net.Error", err)
	}
	if !netErr.Timeout() {
		t.Error("error should be a timeout")
	}
}

func TestUDPConnLocalAddr(t *testing.T) {
	addr := &net.UDPAddr{IP: net.ParseIP("0.0.0.0"), Port: 9000}
	c := newUDPConn(0, addr, false)
	got := c.LocalAddr()
	if got.String() != "0.0.0.0:9000" {
		t.Errorf("LocalAddr()=%v, want 0.0.0.0:9000", got)
	}
}

// --- resolveAddrs tests ---

func TestResolveAddrsLiteralIPv4(t *testing.T) {
	addrs, err := resolveAddrs(context.Background(), "tcp", "127.0.0.1:80")
	if err != nil {
		t.Fatalf("resolveAddrs: %v", err)
	}
	if len(addrs) != 1 {
		t.Fatalf("len=%d, want 1", len(addrs))
	}
	if !addrs[0].IP.Equal(net.ParseIP("127.0.0.1")) {
		t.Errorf("IP=%v, want 127.0.0.1", addrs[0].IP)
	}
}

func TestResolveAddrsLiteralIPv6(t *testing.T) {
	addrs, err := resolveAddrs(context.Background(), "tcp6", "[::1]:443")
	if err != nil {
		t.Fatalf("resolveAddrs: %v", err)
	}
	if len(addrs) != 1 {
		t.Fatalf("len=%d, want 1", len(addrs))
	}
	if !addrs[0].IP.Equal(net.ParseIP("::1")) {
		t.Errorf("IP=%v, want ::1", addrs[0].IP)
	}
}

// --- resolveUDPAddr tests ---

func TestResolveUDPAddrIPv4(t *testing.T) {
	addr, err := resolveUDPAddr(context.Background(), "udp4", "10.0.0.1:53")
	if err != nil {
		t.Fatalf("resolveUDPAddr: %v", err)
	}
	if !addr.IP.Equal(net.ParseIP("10.0.0.1")) {
		t.Errorf("IP=%v, want 10.0.0.1", addr.IP)
	}
	if addr.Port != 53 {
		t.Errorf("Port=%d, want 53", addr.Port)
	}
}

func TestResolveUDPAddrIPv6OnUdp4Rejected(t *testing.T) {
	_, err := resolveUDPAddr(context.Background(), "udp4", "[::1]:53")
	if err == nil {
		t.Fatal("expected error for IPv6 on udp4")
	}
}

// --- Graceful shutdown tests ---

func TestShutdownDoneChannel(t *testing.T) {
	ch := ShutdownDone()
	if ch == nil {
		t.Fatal("ShutdownDone() returned nil")
	}
	// Channel should not be closed yet (we haven't called Shutdown in tests).
	select {
	case <-ch:
		t.Fatal("ShutdownDone channel should not be closed before Shutdown is called")
	default:
	}
}

func TestInstallSignalHandlerIdempotent(t *testing.T) {
	// Should not panic when called multiple times.
	InstallSignalHandler()
	InstallSignalHandler()
	InstallSignalHandler()
}

func TestListenContextReturnsPublicType(t *testing.T) {
	// Test that ListenContext returns the right type (validation only, no VPP).
	_, err := ListenContext("bogus", ":0")
	if err == nil {
		t.Fatal("ListenContext with bogus network returned no error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("err type=%T, want *net.OpError", err)
	}
}

func TestTCPListenerAcceptContextCancelled(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("0.0.0.0"), Port: 8080}
	l := newTCPListener(0, addr, "tcp")

	// With a pre-cancelled context, AcceptContext should return immediately.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Close the listener first so AcceptContext can proceed to the doneCh path.
	l.closed.Store(true)
	close(l.doneCh)

	_, err := l.AcceptContext(ctx)
	if err == nil {
		t.Fatal("AcceptContext on closed listener should return error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("type=%T, want *net.OpError", err)
	}
	if !errors.Is(opErr.Err, ErrClosed) {
		t.Errorf("inner err=%v, want ErrClosed", opErr.Err)
	}
}

func TestTCPListenerCloseWakesAccept(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("0.0.0.0"), Port: 8080}
	l := newTCPListener(0, addr, "tcp")

	// Verify doneCh is open.
	select {
	case <-l.doneCh:
		t.Fatal("doneCh should be open before Close")
	default:
	}

	// Close should close doneCh.
	l.closed.Store(true)
	close(l.doneCh)

	select {
	case <-l.doneCh:
		// expected
	default:
		t.Fatal("doneCh should be closed after Close")
	}
}

// --- Error classification tests ---

func TestVCLErrorIsErrno(t *testing.T) {
	// Simulate what vclpoll returns for ECONNREFUSED.
	err := opError("dial", "tcp", "10.0.0.1:80",
		&vclpoll.VCLError{Op: "connect", Errno: syscall.ECONNREFUSED})

	if !errors.Is(err, syscall.ECONNREFUSED) {
		t.Error("errors.Is(err, ECONNREFUSED) should be true")
	}
	if errors.Is(err, syscall.ECONNRESET) {
		t.Error("errors.Is(err, ECONNRESET) should be false")
	}
}

func TestVCLErrorIsTimeout(t *testing.T) {
	err := &vclpoll.VCLError{Op: "connect_timeout", Errno: syscall.ETIMEDOUT}
	var netErr net.Error
	if !errors.As(err, &netErr) {
		t.Fatal("VCLError should implement net.Error")
	}
	if !netErr.Timeout() {
		t.Error("ETIMEDOUT should report Timeout() == true")
	}
}

func TestVCLErrorMessage(t *testing.T) {
	err := &vclpoll.VCLError{Op: "read", Errno: syscall.ECONNRESET}
	msg := err.Error()
	if msg == "" {
		t.Fatal("Error() should not be empty")
	}
	if !strings.Contains(msg, "read") {
		t.Errorf("Error()=%q, should contain op name", msg)
	}
}

func TestIsConnectionRefused(t *testing.T) {
	err := opError("dial", "tcp", "10.0.0.1:80",
		&vclpoll.VCLError{Op: "connect", Errno: syscall.ECONNREFUSED})
	if !IsConnectionRefused(err) {
		t.Error("IsConnectionRefused should be true")
	}
	if IsConnectionReset(err) {
		t.Error("IsConnectionReset should be false for ECONNREFUSED")
	}
}

func TestIsConnectionReset(t *testing.T) {
	err := opError("dial", "tcp", "10.0.0.1:80",
		&vclpoll.VCLError{Op: "read", Errno: syscall.ECONNRESET})
	if !IsConnectionReset(err) {
		t.Error("IsConnectionReset should be true")
	}
	if IsConnectionRefused(err) {
		t.Error("IsConnectionRefused should be false for ECONNRESET")
	}
}

func TestIsTimeoutHelper(t *testing.T) {
	err := opError("dial", "tcp", "10.0.0.1:80",
		&vclpoll.VCLError{Op: "connect", Errno: syscall.ETIMEDOUT})
	if !IsTimeout(err) {
		t.Error("IsTimeout should be true for ETIMEDOUT")
	}
}

// --- Transport tests ---

func TestTransportReturnsHTTPTransport(t *testing.T) {
	tr := Transport(nil)
	if tr == nil {
		t.Fatal("Transport returned nil")
	}
	if tr.MaxIdleConns != 100 {
		t.Errorf("MaxIdleConns=%d, want 100", tr.MaxIdleConns)
	}
	if tr.MaxIdleConnsPerHost != 10 {
		t.Errorf("MaxIdleConnsPerHost=%d, want 10", tr.MaxIdleConnsPerHost)
	}
	if tr.DisableKeepAlives {
		t.Error("DisableKeepAlives should be false (keep-alive enabled)")
	}
}

func TestNewHTTPClient(t *testing.T) {
	client := NewHTTPClient()
	if client == nil {
		t.Fatal("NewHTTPClient returned nil")
	}
	if client.Transport == nil {
		t.Fatal("client.Transport is nil")
	}
}

func TestDefaultTransportNotNil(t *testing.T) {
	if DefaultTransport == nil {
		t.Fatal("DefaultTransport is nil")
	}
}

// --- isUDP tests ---

func TestIsUDP(t *testing.T) {
	if !isUDP("udp") {
		t.Error("isUDP(\"udp\") should be true")
	}
	if !isUDP("udp4") {
		t.Error("isUDP(\"udp4\") should be true")
	}
	if !isUDP("udp6") {
		t.Error("isUDP(\"udp6\") should be true")
	}
	if isUDP("tcp") {
		t.Error("isUDP(\"tcp\") should be false")
	}
	if isUDP("tcp4") {
		t.Error("isUDP(\"tcp4\") should be false")
	}
}

// --- udpAddrFromInfo tests ---

func TestUDPAddrFromInfoIPv4(t *testing.T) {
	info := vclpoll.AddrInfo{
		IP:   [16]byte{10, 0, 0, 1},
		Port: 5353,
		IsV4: true,
	}
	addr := udpAddrFromInfo(info)
	if !addr.IP.Equal(net.ParseIP("10.0.0.1")) {
		t.Errorf("IP=%v, want 10.0.0.1", addr.IP)
	}
	if addr.Port != 5353 {
		t.Errorf("Port=%d, want 5353", addr.Port)
	}
	if addr.Network() != "udp" {
		t.Errorf("Network()=%q, want udp", addr.Network())
	}
}

func TestUDPAddrFromInfoIPv6(t *testing.T) {
	info := vclpoll.AddrInfo{
		IP:   [16]byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		Port: 53,
		IsV4: false,
	}
	addr := udpAddrFromInfo(info)
	expected := net.ParseIP("2001:db8::1")
	if !addr.IP.Equal(expected) {
		t.Errorf("IP=%v, want %v", addr.IP, expected)
	}
	if addr.Port != 53 {
		t.Errorf("Port=%d, want 53", addr.Port)
	}
}

func TestUDPAddrFromInfoZero(t *testing.T) {
	info := vclpoll.AddrInfo{IsV4: true}
	addr := udpAddrFromInfo(info)
	if !addr.IP.Equal(net.IPv4zero) {
		t.Errorf("IP=%v, want 0.0.0.0", addr.IP)
	}
	if addr.Port != 0 {
		t.Errorf("Port=%d, want 0", addr.Port)
	}
}

// --- Additional interleaveAddrs coverage ---

func TestInterleaveAddrsOnlyV6(t *testing.T) {
	addrs := []*net.TCPAddr{
		{IP: net.ParseIP("2001:db8::1"), Port: 80},
		{IP: net.ParseIP("2001:db8::2"), Port: 80},
	}
	result := interleaveAddrs(addrs)
	if len(result) != 2 {
		t.Fatalf("len=%d, want 2", len(result))
	}
	for i, a := range result {
		if a.IP.To4() != nil {
			t.Errorf("result[%d]=%v, want IPv6", i, a.IP)
		}
	}
}

func TestInterleaveAddrsEmpty(t *testing.T) {
	result := interleaveAddrs(nil)
	if len(result) != 0 {
		t.Errorf("len=%d, want 0", len(result))
	}
}

// --- UDPConn state / error paths ---

// TestUDPConnWriteToRejectsNonUDPAddr verifies WriteTo returns a *net.AddrError
// when passed something other than a *net.UDPAddr.
func TestUDPConnWriteToRejectsNonUDPAddr(t *testing.T) {
	c := newUDPConn(0, &net.UDPAddr{IP: net.ParseIP("0.0.0.0"), Port: 9000}, false)
	tcpAddr := &net.TCPAddr{IP: net.ParseIP("10.0.0.1"), Port: 80}
	_, err := c.WriteTo([]byte("hi"), tcpAddr)
	if err == nil {
		t.Fatal("WriteTo with non-UDP addr should fail")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("err type=%T, want *net.OpError", err)
	}
	var addrErr *net.AddrError
	if !errors.As(err, &addrErr) {
		t.Errorf("inner err type=%T, want *net.AddrError", opErr.Err)
	}
}

// TestUDPConnRemoteAddrDefaultsWhenUnconnected matches net.UDPConn: an
// unconnected socket has no remote address.
func TestUDPConnRemoteAddrDefaultsWhenUnconnected(t *testing.T) {
	c := newUDPConn(0, &net.UDPAddr{IP: net.ParseIP("0.0.0.0"), Port: 9000}, false)
	if got := c.RemoteAddr(); got != nil {
		t.Fatalf("RemoteAddr()=%v, want nil", got)
	}
}

// TestUDPConnRemoteAddrCached verifies that a peer set at construction is
// returned by RemoteAddr without a VCL round-trip.
func TestUDPConnRemoteAddrCached(t *testing.T) {
	c := newUDPConn(0, &net.UDPAddr{IP: net.ParseIP("0.0.0.0"), Port: 9000}, true)
	c.peerAddr = &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 53}
	got := c.RemoteAddr()
	if got.String() != "10.0.0.1:53" {
		t.Errorf("RemoteAddr()=%v, want 10.0.0.1:53", got)
	}
}

// TestUDPConnReadDeadlineExpiredConnected exercises the connected-UDP Read
// deadline path (as opposed to ReadFrom).
func TestUDPConnReadDeadlineExpiredConnected(t *testing.T) {
	c := newUDPConn(0, nil, true)
	c.peerAddr = &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 53}
	c.SetReadDeadline(time.Now().Add(-time.Second))

	if _, err := c.Read(make([]byte, 10)); err == nil {
		t.Fatal("Read with expired deadline should return error")
	} else {
		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {
			t.Errorf("err=%v, want timeout", err)
		}
	}
}

// TestUDPConnWriteDeadlineExpiredConnected exercises the connected-UDP Write
// deadline path (as opposed to WriteTo).
func TestUDPConnWriteDeadlineExpiredConnected(t *testing.T) {
	c := newUDPConn(0, nil, true)
	c.peerAddr = &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 53}
	c.SetWriteDeadline(time.Now().Add(-time.Second))

	if _, err := c.Write([]byte("x")); err == nil {
		t.Fatal("Write with expired deadline should return error")
	} else {
		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {
			t.Errorf("err=%v, want timeout", err)
		}
	}
}

// TestUDPConnReadAfterClose exercises the closed-state check on connected Read.
func TestUDPConnReadAfterClose(t *testing.T) {
	c := newUDPConn(0, nil, true)
	c.peerAddr = &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 53}
	c.closed.Store(true)

	_, err := c.Read(make([]byte, 10))
	if err == nil {
		t.Fatal("Read on closed conn should return error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("type=%T, want *net.OpError", err)
	}
	if !errors.Is(opErr.Err, ErrClosed) {
		t.Errorf("inner err=%v, want ErrClosed", opErr.Err)
	}
}

// TestUDPConnWriteAfterClose exercises the closed-state check on connected Write.
func TestUDPConnWriteAfterClose(t *testing.T) {
	c := newUDPConn(0, nil, true)
	c.peerAddr = &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 53}
	c.closed.Store(true)

	_, err := c.Write([]byte("hello"))
	if err == nil {
		t.Fatal("Write on closed conn should return error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("type=%T, want *net.OpError", err)
	}
	if !errors.Is(opErr.Err, ErrClosed) {
		t.Errorf("inner err=%v, want ErrClosed", opErr.Err)
	}
}

// --- Public TCPListener wrapper tests ---

func TestPublicTCPListenerAddr(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("0.0.0.0"), Port: 8080}
	inner := newTCPListener(0, addr, "tcp")
	pub := &TCPListener{inner: inner}
	if pub.Addr().String() != "0.0.0.0:8080" {
		t.Errorf("Addr()=%v, want 0.0.0.0:8080", pub.Addr())
	}
}

func TestPublicTCPListenerAcceptAfterClose(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("0.0.0.0"), Port: 8080}
	inner := newTCPListener(0, addr, "tcp")
	inner.closed.Store(true)
	pub := &TCPListener{inner: inner}
	_, err := pub.Accept()
	if err == nil {
		t.Fatal("Accept on closed listener should return error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) || !errors.Is(opErr.Err, ErrClosed) {
		t.Errorf("err=%v, want *net.OpError wrapping ErrClosed", err)
	}
}

func TestPublicTCPListenerAcceptContextCancelledFast(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("0.0.0.0"), Port: 8080}
	inner := newTCPListener(0, addr, "tcp")
	inner.closed.Store(true)
	close(inner.doneCh)
	pub := &TCPListener{inner: inner}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := pub.AcceptContext(ctx)
	if err == nil {
		t.Fatal("AcceptContext on closed listener should return error")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) || !errors.Is(opErr.Err, ErrClosed) {
		t.Errorf("err=%v, want *net.OpError wrapping ErrClosed", err)
	}
}

// --- Dialer defaults ---

// TestDialerFallbackDelayDefault verifies that a zero FallbackDelay picks up
// the package default (250ms per RFC 8305).
func TestInterruptedConnectError(t *testing.T) {
	if err := interruptedConnectError(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("background context error=%v, want ErrClosed", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := interruptedConnectError(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled context error=%v, want context.Canceled", err)
	}
}

func TestDialerFallbackDelayDefault(t *testing.T) {
	if defaultFallbackDelay != 250*time.Millisecond {
		t.Errorf("defaultFallbackDelay=%v, want 250ms", defaultFallbackDelay)
	}
}

// TestDialerTimeoutValidation verifies a Dialer with an explicit Timeout still
// propagates validation errors through opError with Op="dial".
func TestDialerTimeoutValidation(t *testing.T) {
	d := &Dialer{Timeout: 100 * time.Millisecond}
	_, err := d.DialContext(context.Background(), "bogus", "127.0.0.1:0")
	if err == nil {
		t.Fatal("expected error from bogus network")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("err type=%T, want *net.OpError", err)
	}
	if opErr.Op != "dial" {
		t.Errorf("Op=%q, want dial", opErr.Op)
	}
}

// TestDialerFallbackDelayCustom verifies that a caller-provided FallbackDelay
// is retained on the struct (used by Happy Eyeballs when dialing "tcp").
func TestDialerFallbackDelayCustom(t *testing.T) {
	d := &Dialer{FallbackDelay: 500 * time.Millisecond}
	if d.FallbackDelay != 500*time.Millisecond {
		t.Errorf("FallbackDelay=%v, want 500ms", d.FallbackDelay)
	}
}

// --- Transport dial validation ---

// TestTransportDialContextForHTTPValidation verifies that Transport()'s
// DialContext plumbing forwards validation errors correctly for an unknown
// network. This is the code path net/http hits before any I/O occurs.
func TestTransportDialContextForHTTPValidation(t *testing.T) {
	tr := Transport(nil)
	_, err := tr.DialContext(context.Background(), "bogus", "127.0.0.1:80")
	if err == nil {
		t.Fatal("Transport DialContext with bogus network should fail")
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("err type=%T, want *net.OpError", err)
	}
	if opErr.Op != "dial" {
		t.Errorf("Op=%q, want dial", opErr.Op)
	}
}

// --- Shutdown state tests ---

// TestShutdownDoneReturnsStableChannel verifies that ShutdownDone always
// returns the same channel so callers can safely cache it.
func TestShutdownDoneReturnsStableChannel(t *testing.T) {
	ch1 := ShutdownDone()
	ch2 := ShutdownDone()
	if ch1 != ch2 {
		t.Error("ShutdownDone returned different channels on successive calls")
	}
}

// --- resolveAddrs edge cases ---

func TestResolveAddrsEmptyHost(t *testing.T) {
	addrs, err := resolveAddrs(context.Background(), "tcp", ":8080")
	if err != nil {
		t.Fatalf("resolveAddrs: %v", err)
	}
	if len(addrs) != 1 {
		t.Fatalf("len=%d, want 1", len(addrs))
	}
	if !addrs[0].IP.Equal(net.IPv4zero) {
		t.Errorf("IP=%v, want 0.0.0.0", addrs[0].IP)
	}
	if addrs[0].Port != 8080 {
		t.Errorf("Port=%d, want 8080", addrs[0].Port)
	}
}

func TestResolveAddrsBadPort(t *testing.T) {
	_, err := resolveAddrs(context.Background(), "tcp", "127.0.0.1:not-a-port")
	if err == nil {
		t.Fatal("expected error for invalid port")
	}
}

func TestResolveAddrsBadFormat(t *testing.T) {
	_, err := resolveAddrs(context.Background(), "tcp", "127.0.0.1")
	if err == nil {
		t.Fatal("expected error for missing port separator")
	}
}

// --- parseNetwork error type check ---

func TestParseNetworkReturnsUnknownNetworkError(t *testing.T) {
	_, _, err := parseNetwork("sctp")
	if err == nil {
		t.Fatal("expected error for unknown network")
	}
	var unk net.UnknownNetworkError
	if !errors.As(err, &unk) {
		t.Errorf("err type=%T, want net.UnknownNetworkError", err)
	}
	if string(unk) != "sctp" {
		t.Errorf("unk=%q, want sctp", string(unk))
	}
}

// --- VCLError classification: additional errno paths ---

func TestVCLErrorTemporary(t *testing.T) {
	// EAGAIN, EWOULDBLOCK, and EINTR should be reported as temporary.
	if !(&vclpoll.VCLError{Op: "read", Errno: syscall.EAGAIN}).Temporary() {
		t.Error("EAGAIN should be temporary")
	}
	if !(&vclpoll.VCLError{Op: "read", Errno: syscall.EINTR}).Temporary() {
		t.Error("EINTR should be temporary")
	}
	if (&vclpoll.VCLError{Op: "read", Errno: syscall.ECONNRESET}).Temporary() {
		t.Error("ECONNRESET should not be temporary")
	}
}

// TestVCLErrorUnwrap verifies the wrapped syscall.Errno is accessible via
// errors.As so callers can do stdlib-style errno inspection.
func TestVCLErrorUnwrap(t *testing.T) {
	err := &vclpoll.VCLError{Op: "connect", Errno: syscall.ECONNREFUSED}
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		t.Fatal("errors.As did not extract syscall.Errno")
	}
	if errno != syscall.ECONNREFUSED {
		t.Errorf("errno=%v, want ECONNREFUSED", errno)
	}
}

// --- Contract and regression coverage added by the repository audit ---

func TestResolveAddrRejectsInvalidServiceOnResolvableHost(t *testing.T) {
	_, err := resolveAddr("tcp", "localhost:definitely-not-a-service")
	if err == nil {
		t.Fatal("expected invalid service error")
	}
}

func TestResolveUDPAddrNamedService(t *testing.T) {
	addr, err := resolveUDPAddr(context.Background(), "udp", "127.0.0.1:domain")
	if err != nil {
		t.Fatalf("resolveUDPAddr: %v", err)
	}
	if addr.Port != 53 {
		t.Fatalf("Port=%d, want 53", addr.Port)
	}
}

func TestOpErrorIncludesUnresolvedAddress(t *testing.T) {
	err := opError("dial", "tcp", "example.invalid:443", ErrClosed)
	var opErr *net.OpError
	if !errors.As(err, &opErr) {
		t.Fatalf("type=%T, want *net.OpError", err)
	}
	if opErr.Addr == nil || opErr.Addr.String() != "example.invalid:443" {
		t.Fatalf("Addr=%v, want example.invalid:443", opErr.Addr)
	}
	if opErr.Addr.Network() != "tcp" {
		t.Fatalf("Addr.Network()=%q, want tcp", opErr.Addr.Network())
	}
}

func TestDeadlineStateExpiresAndCanBeCleared(t *testing.T) {
	d := newDeadlineState()
	d.set(time.Now().Add(20 * time.Millisecond))
	select {
	case <-d.waitChannel():
	case <-time.After(time.Second):
		t.Fatal("deadline did not expire")
	}
	if !d.expired() {
		t.Fatal("expired deadline reported active")
	}

	d.set(time.Time{})
	if d.expired() {
		t.Fatal("cleared deadline still reported expired")
	}
	select {
	case <-d.waitChannel():
		t.Fatal("cleared deadline channel is closed")
	default:
	}
}

func TestDeadlineStateUpdateWakesExistingWaiter(t *testing.T) {
	d := newDeadlineState()
	old := d.waitChannel()
	d.set(time.Now().Add(time.Hour))
	select {
	case <-old:
	default:
		t.Fatal("deadline update did not wake existing waiter")
	}
	if d.expired() {
		t.Fatal("future deadline reported expired")
	}
	d.interrupt()
}

func TestTCPDeadlineSetAfterClose(t *testing.T) {
	c := newTCPConn(0)
	c.peerAddr = &net.TCPAddr{}
	c.closed.Store(true)
	if err := c.SetDeadline(time.Now()); !errors.Is(err, ErrClosed) {
		t.Fatalf("SetDeadline error=%v, want ErrClosed", err)
	}
}

func TestUDPDeadlineSetAfterClose(t *testing.T) {
	c := newUDPConn(0, &net.UDPAddr{}, false)
	c.closed.Store(true)
	if err := c.SetDeadline(time.Now()); !errors.Is(err, ErrClosed) {
		t.Fatalf("SetDeadline error=%v, want ErrClosed", err)
	}
}

func TestUDPConnWriteToNilAddress(t *testing.T) {
	c := newUDPConn(0, &net.UDPAddr{IP: net.IPv4zero, Port: 9000}, false)
	if _, err := c.WriteTo([]byte("x"), nil); err == nil {
		t.Fatal("WriteTo(nil) returned no error")
	} else {
		var addrErr *net.AddrError
		if !errors.As(err, &addrErr) {
			t.Fatalf("error=%T, want *net.AddrError", err)
		}
	}
}

func TestUDPConnWriteToConnectedRejected(t *testing.T) {
	c := newUDPConn(0, nil, true)
	c.peerAddr = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 53}
	_, err := c.WriteTo([]byte("x"), c.peerAddr)
	if !errors.Is(err, errWriteToConnected) {
		t.Fatalf("WriteTo error=%v, want errWriteToConnected", err)
	}
}

func TestUDPConnWriteRequiresPeer(t *testing.T) {
	c := newUDPConn(0, &net.UDPAddr{IP: net.IPv4zero, Port: 9000}, false)
	if _, err := c.Write([]byte("x")); !errors.Is(err, errMissingPeer) {
		t.Fatalf("Write error=%v, want errMissingPeer", err)
	}
}

func TestUDPConnWriteAfterClosePrecedesMissingPeer(t *testing.T) {
	c := newUDPConn(0, &net.UDPAddr{IP: net.IPv4zero, Port: 9000}, false)
	c.closed.Store(true)
	if _, err := c.Write([]byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Write error=%v, want ErrClosed", err)
	}
}

func TestTCPListenerAcceptContextPreCancelled(t *testing.T) {
	l := newTCPListener(0, &net.TCPAddr{IP: net.IPv4zero, Port: 9000}, "tcp")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.AcceptContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("AcceptContext error=%v, want context.Canceled", err)
	}
}

func TestShutdownBeforeInit(t *testing.T) {
	const helperEnv = "VCLNET_SHUTDOWN_HELPER"
	if os.Getenv(helperEnv) == "1" {
		Shutdown()
		select {
		case <-ShutdownDone():
		default:
			t.Fatal("ShutdownDone was not closed")
		}
		if err := Init("after-shutdown"); !errors.Is(err, ErrClosed) {
			t.Fatalf("Init after Shutdown error=%v, want ErrClosed", err)
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestShutdownBeforeInit$")
	cmd.Env = append(os.Environ(), helperEnv+"=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("shutdown helper failed: %v\n%s", err, out)
	}
}

func TestListenPortZeroRejected(t *testing.T) {
	if _, err := Listen("tcp4", "127.0.0.1:0"); err == nil {
		t.Fatal("Listen port zero returned no error")
	} else {
		var addrErr *net.AddrError
		if !errors.As(err, &addrErr) {
			t.Fatalf("error=%T, want *net.AddrError", err)
		}
	}
}

func TestListenPacketPortZeroRejected(t *testing.T) {
	if _, err := ListenPacket("udp4", "127.0.0.1:0"); err == nil {
		t.Fatal("ListenPacket port zero returned no error")
	} else {
		var addrErr *net.AddrError
		if !errors.As(err, &addrErr) {
			t.Fatalf("error=%T, want *net.AddrError", err)
		}
	}
}
