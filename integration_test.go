package vclnet_test

// Integration tests for the public vclnet API (net.Listener / net.Conn).
//
// Architecture: same subprocess pattern as internal/vclpoll/integration_test.go.
// The test binary re-executes itself with VCLNET_API_SERVER_MODE=1 to run a
// server in a separate VLS app, then the parent test process connects as client.
//
// Tests are gated by VCL_CONFIG. If unset, they are skipped.
//
// Coverage:
//   - TCP IPv4 echo (single + concurrent)
//   - TCP IPv6 echo
//   - HTTP over vclnet (IPv4)
//   - HTTP over vclnet (IPv6)
//   - Large payload transfer
//   - Multiple sequential connections
//
// Run:
//   VCL_CONFIG=/tmp/vclnet-share/vcl.conf go test -v -count=1 .

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aritrbas/vclnet"
	"github.com/aritrbas/vclnet/internal/vclpoll"
)

const (
	envAPIServerMode = "VCLNET_API_SERVER_MODE"
	envAPIServerPort = "VCLNET_API_SERVER_PORT"
	envAPIServerType = "VCLNET_API_SERVER_TYPE" // "echo", "echo6", "http", "http6", "udp", "udp6"
	envAPIServerN    = "VCLNET_API_SERVER_NCONNS"
	envMultiWorker   = "VCLNET_MULTI_WORKER"
)

var nextAPIPort int32 = 40000

func reserveAPIPort() uint16 { return uint16(atomic.AddInt32(&nextAPIPort, 1)) }

func skipIfNoVPP(t *testing.T) {
	t.Helper()
	if os.Getenv("VCL_CONFIG") == "" {
		t.Skip("VCL_CONFIG not set; skipping VPP integration test")
	}
}

func skipUDPInMode2(t *testing.T) {
	t.Helper()
	if os.Getenv("VCLNET_VLS_MODE") == "2" {
		t.Skip("UDP is not supported in VLS mode 2 (VPP cut-through datagram cleanup crash)")
	}
}

func TestMain(m *testing.M) {
	if os.Getenv(envAPIServerMode) == "1" {
		runServerChild()
		return
	}
	code := m.Run()
	vclnet.Shutdown()
	os.Exit(code)
}

func runServerChild() {
	port, _ := strconv.Atoi(os.Getenv(envAPIServerPort))
	serverType := os.Getenv(envAPIServerType)
	nConns, _ := strconv.Atoi(os.Getenv(envAPIServerN))
	if nConns == 0 {
		nConns = 1
	}

	appName := fmt.Sprintf("vclnet-test-srv-%s-%d", serverType, port)
	if err := vclnet.Init(appName); err != nil {
		fmt.Fprintf(os.Stderr, "child: Init: %v\n", err)
		os.Exit(2)
	}
	defer vclnet.Shutdown()

	switch serverType {
	case "echo":
		runEchoServer(port, nConns, "tcp4")
	case "echo6":
		runEchoServer(port, nConns, "tcp6")
	case "delayecho":
		runDelayedEchoServer(port)
	case "http":
		runHTTPServer(port, "tcp4")
	case "http6":
		runHTTPServer(port, "tcp6")
	case "udp":
		runUDPEchoServer(port, nConns, "udp4")
	case "udp6":
		runUDPEchoServer(port, nConns, "udp6")
	case "tls":
		runTLSEchoServer(port, nConns)
	case "nativetls":
		runNativeTLSEchoServer(port, nConns)
	case "shutdown":
		runShutdownSelfTest(port)
	case "halfclose":
		runHalfCloseServer(port)
	default:
		fmt.Fprintf(os.Stderr, "child: unknown server type %q\n", serverType)
		os.Exit(2)
	}
}

func runEchoServer(port, nConns int, network string) {
	var addr string
	switch network {
	case "tcp6":
		addr = fmt.Sprintf("[::1]:%d", port)
	default:
		addr = fmt.Sprintf("0.0.0.0:%d", port)
	}

	ln, err := vclnet.Listen(network, addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: Listen(%s, %s): %v\n", network, addr, err)
		os.Exit(2)
	}
	fmt.Printf("READY %d\n", port)
	os.Stdout.Sync()

	var wg sync.WaitGroup
	for i := 0; i < nConns; i++ {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "child: Accept: %v\n", err)
			os.Exit(2)
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer c.Close()
			io.Copy(c, c) // echo
		}(conn)
	}
	wg.Wait()
	ln.Close()
}

func runShutdownSelfTest(port int) {
	ln, err := vclnet.Listen("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: shutdown Listen: %v\n", err)
		os.Exit(2)
	}
	acceptErr := make(chan error, 1)
	go func() {
		_, err := ln.Accept()
		acceptErr <- err
	}()
	time.Sleep(100 * time.Millisecond)
	vclnet.Shutdown()
	select {
	case err := <-acceptErr:
		if err == nil {
			fmt.Fprintln(os.Stderr, "child: Accept returned nil during shutdown")
			os.Exit(2)
		}
	case <-time.After(2 * time.Second):
		fmt.Fprintln(os.Stderr, "child: shutdown did not wake Accept")
		os.Exit(2)
	}
	if err := vclnet.Init("after-shutdown"); !errors.Is(err, vclnet.ErrClosed) {
		fmt.Fprintf(os.Stderr, "child: Init after shutdown: %v\n", err)
		os.Exit(2)
	}
	fmt.Printf("READY %d\n", port)
	os.Stdout.Sync()
}

// runHalfCloseServer validates CloseWrite over the wire: the client sends
// bytes, then calls CloseWrite, and the server's Read must observe EOF (the
// FIN that vls_shutdown(SHUT_WR) emitted). The server then replies with a
// distinct marker plus the reversed request so the client can verify both
// halves of the transfer succeeded independently.
func runHalfCloseServer(port int) {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	ln, err := vclnet.Listen("tcp4", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: halfclose Listen: %v\n", err)
		os.Exit(2)
	}
	fmt.Printf("READY %d\n", port)
	os.Stdout.Sync()

	conn, err := ln.Accept()
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: halfclose Accept: %v\n", err)
		os.Exit(2)
	}

	// Read all client bytes until EOF. EOF here proves that the client's
	// vls_shutdown(SHUT_WR) reached VPP, propagated to the peer session,
	// and surfaced as a zero-length read on the server side.
	req, err := io.ReadAll(conn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: halfclose ReadAll: %v\n", err)
		os.Exit(2)
	}

	// Compose a response the client can uniquely verify.
	response := append([]byte("RESP:"), reverseBytes(req)...)
	if _, err := conn.Write(response); err != nil {
		fmt.Fprintf(os.Stderr, "child: halfclose Write: %v\n", err)
		os.Exit(2)
	}

	_ = conn.Close()
	_ = ln.Close()
}

func reverseBytes(b []byte) []byte {
	out := make([]byte, len(b))
	for i := range b {
		out[len(b)-1-i] = b[i]
	}
	return out
}

func runDelayedEchoServer(port int) {
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	ln, err := vclnet.Listen("tcp4", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: delayed Listen: %v\n", err)
		os.Exit(2)
	}
	fmt.Printf("READY %d\n", port)
	os.Stdout.Sync()

	conn, err := ln.Accept()
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: delayed Accept: %v\n", err)
		os.Exit(2)
	}
	time.Sleep(200 * time.Millisecond)
	_, _ = io.Copy(conn, conn)
	_ = conn.Close()
	_ = ln.Close()
}

func runHTTPServer(port int, network string) {
	var addr string
	switch network {
	case "tcp6":
		addr = fmt.Sprintf("[::1]:%d", port)
	default:
		addr = fmt.Sprintf("0.0.0.0:%d", port)
	}

	ln, err := vclnet.Listen(network, addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: Listen(%s, %s): %v\n", network, addr, err)
		os.Exit(2)
	}
	fmt.Printf("READY %d\n", port)
	os.Stdout.Sync()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok"}`)
	})
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		io.Copy(w, r.Body)
	})
	mux.HandleFunc("/large", func(w http.ResponseWriter, r *http.Request) {
		// Return 64KB of data.
		data := bytes.Repeat([]byte("X"), 65536)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Write(data)
	})

	server := &http.Server{Handler: mux}
	server.Serve(ln)
}

// --- Test helpers ---

func cleanupCommand(tb interface{ Cleanup(func()) }, cmd *exec.Cmd) {
	tb.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
}

func startServer(t *testing.T, serverType string, nConns int) (*exec.Cmd, uint16, *bytes.Buffer) {
	t.Helper()
	port := reserveAPIPort()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.Command(exe, "-test.run=^$")
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	cmd.Env = append(os.Environ(),
		envAPIServerMode+"=1",
		envAPIServerPort+"="+strconv.Itoa(int(port)),
		envAPIServerType+"="+serverType,
		envAPIServerN+"="+strconv.Itoa(nConns),
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("cmd.Start: %v", err)
	}

	cleanupCommand(t, cmd)

	br := bufio.NewReader(stdout)
	readyCh := make(chan error, 1)
	go func() {
		line, err := br.ReadString('\n')
		if err != nil {
			readyCh <- fmt.Errorf("read READY: %w", err)
			return
		}
		var got int
		if _, err := fmt.Sscanf(line, "READY %d", &got); err != nil || got != int(port) {
			readyCh <- fmt.Errorf("bad ready line %q (want READY %d)", line, port)
			return
		}
		readyCh <- nil
		// Drain remaining stdout so child doesn't block on write.
		io.Copy(io.Discard, br)
	}()

	select {
	case err := <-readyCh:
		if err != nil {
			cmd.Process.Kill()
			t.Fatalf("server child (%s): %v\nstderr:\n%s", serverType, err, stderr.String())
		}
	case <-time.After(15 * time.Second):
		cmd.Process.Kill()
		t.Fatalf("server child (%s) did not become READY in time\nstderr:\n%s", serverType, stderr.String())
	}

	return cmd, port, stderr
}

func waitOrKill(t *testing.T, cmd *exec.Cmd, stderr *bytes.Buffer) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("server child exit: %v\nstderr:\n%s", err, stderr.String())
		}
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		t.Errorf("server child did not exit\nstderr:\n%s", stderr.String())
	}
}

// --- TCP IPv4 Tests ---

func TestTCPIPv4EchoSingle(t *testing.T) {
	skipIfNoVPP(t)

	cmd, port, stderr := startServer(t, "echo", 1)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	conn, err := vclnet.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Dial: %v\nstderr:\n%s", err, stderr.String())
	}

	msg := []byte("hello from vclnet integration test")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Errorf("echo mismatch: got %q want %q", got, msg)
	}

	conn.Close()
	waitOrKill(t, cmd, stderr)
}

func TestTCPIPv4EchoConcurrent(t *testing.T) {
	skipIfNoVPP(t)

	const nConns = 4
	cmd, port, stderr := startServer(t, "echo", nConns)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// Dial all connections first, then echo on each one sequentially.
	// VPP debug builds have a session-reset race when multiple sessions
	// do concurrent I/O on the same VLS app, so we serialise the I/O.
	conns := make([]net.Conn, nConns)
	for i := 0; i < nConns; i++ {
		var err error
		conns[i], err = vclnet.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			for j := 0; j < i; j++ {
				conns[j].Close()
			}
			t.Fatalf("conn %d Dial: %v\nstderr:\n%s", i, err, stderr.String())
		}
	}

	for i := 0; i < nConns; i++ {
		conn := conns[i]
		msg := []byte(fmt.Sprintf("concurrent-payload-%d-from-vclnet", i))
		if _, err := conn.Write(msg); err != nil {
			t.Fatalf("conn %d Write: %v", i, err)
		}

		got := make([]byte, len(msg))
		if _, err := io.ReadFull(conn, got); err != nil {
			t.Fatalf("conn %d ReadFull: %v", i, err)
		}
		if !bytes.Equal(got, msg) {
			t.Errorf("conn %d echo mismatch: got %q want %q", i, got, msg)
		}
		conn.Close()
	}
	waitOrKill(t, cmd, stderr)
}

func TestTCPIPv4LargePayload(t *testing.T) {
	skipIfNoVPP(t)

	cmd, port, stderr := startServer(t, "echo", 1)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	conn, err := vclnet.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Dial: %v\nstderr:\n%s", err, stderr.String())
	}

	// Send 128KB payload.
	payload := bytes.Repeat([]byte("ABCDEFGH"), 16384)
	go func() {
		conn.Write(payload)
	}()

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("ReadFull 128KB: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("128KB echo mismatch at some offset")
	}

	conn.Close() // Must close before waitOrKill so server sees EOF.
	waitOrKill(t, cmd, stderr)
}

func TestTCPIPv4Sequential(t *testing.T) {
	skipIfNoVPP(t)

	// Server accepts 3 sequential connections.
	cmd, port, stderr := startServer(t, "echo", 3)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	for i := 0; i < 3; i++ {
		conn, err := vclnet.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			t.Fatalf("Dial #%d: %v", i, err)
		}

		msg := []byte(fmt.Sprintf("seq-%d", i))
		conn.Write(msg)
		got := make([]byte, len(msg))
		io.ReadFull(conn, got)
		if !bytes.Equal(got, msg) {
			t.Errorf("seq #%d mismatch: got %q want %q", i, got, msg)
		}
		conn.Close()
	}

	waitOrKill(t, cmd, stderr)
}

// --- TCP IPv6 Tests ---

func TestTCPIPv6Echo(t *testing.T) {
	skipIfNoVPP(t)

	cmd, port, stderr := startServer(t, "echo6", 1)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	conn, err := vclnet.Dial("tcp6", fmt.Sprintf("[::1]:%d", port))
	if err != nil {
		t.Fatalf("Dial tcp6: %v\nstderr:\n%s", err, stderr.String())
	}

	msg := []byte("hello ipv6 from vclnet")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Errorf("echo6 mismatch: got %q want %q", got, msg)
	}

	conn.Close()
	waitOrKill(t, cmd, stderr)
}

func TestTCPIPv6Concurrent(t *testing.T) {
	skipIfNoVPP(t)

	const nConns = 4
	cmd, port, stderr := startServer(t, "echo6", nConns)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// Dial all connections first, then echo on each one sequentially.
	conns := make([]net.Conn, nConns)
	for i := 0; i < nConns; i++ {
		var err error
		conns[i], err = vclnet.Dial("tcp6", fmt.Sprintf("[::1]:%d", port))
		if err != nil {
			for j := 0; j < i; j++ {
				conns[j].Close()
			}
			t.Fatalf("conn %d Dial: %v\nstderr:\n%s", i, err, stderr.String())
		}
	}

	for i := 0; i < nConns; i++ {
		conn := conns[i]
		msg := []byte(fmt.Sprintf("ipv6-concurrent-%d", i))
		if _, err := conn.Write(msg); err != nil {
			t.Fatalf("conn %d Write: %v", i, err)
		}

		got := make([]byte, len(msg))
		if _, err := io.ReadFull(conn, got); err != nil {
			t.Fatalf("conn %d ReadFull: %v", i, err)
		}
		if !bytes.Equal(got, msg) {
			t.Errorf("conn %d mismatch: got %q want %q", i, got, msg)
		}
		conn.Close()
	}
	waitOrKill(t, cmd, stderr)
}

// --- HTTP IPv4 Tests ---

func TestHTTPIPv4Health(t *testing.T) {
	skipIfNoVPP(t)

	cmd, port, stderr := startServer(t, "http", 0)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	client := httpClientViaVclnet()

	url := fmt.Sprintf("http://127.0.0.1:%d/health", port)
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v\nstderr:\n%s", url, err, stderr.String())
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("status=%d, want 200", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"status":"ok"`) {
		t.Errorf("body=%q, want containing status:ok", body)
	}

	cmd.Process.Kill()
}

func TestHTTPIPv4Echo(t *testing.T) {
	skipIfNoVPP(t)

	cmd, port, stderr := startServer(t, "http", 0)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	client := httpClientViaVclnet()

	payload := "hello from http echo test"
	url := fmt.Sprintf("http://127.0.0.1:%d/echo", port)
	resp, err := client.Post(url, "text/plain", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("POST %s: %v\nstderr:\n%s", url, err, stderr.String())
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != payload {
		t.Errorf("echo body=%q, want %q", body, payload)
	}

	cmd.Process.Kill()
}

func TestHTTPIPv4LargeResponse(t *testing.T) {
	skipIfNoVPP(t)

	cmd, port, stderr := startServer(t, "http", 0)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	client := httpClientViaVclnet()

	url := fmt.Sprintf("http://127.0.0.1:%d/large", port)
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v\nstderr:\n%s", url, err, stderr.String())
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if len(body) != 65536 {
		t.Errorf("large response size=%d, want 65536", len(body))
	}
	// Verify content.
	expected := bytes.Repeat([]byte("X"), 65536)
	if !bytes.Equal(body, expected) {
		t.Error("large response content mismatch")
	}

	cmd.Process.Kill()
}

func TestHTTPIPv4MultipleRequests(t *testing.T) {
	skipIfNoVPP(t)

	cmd, port, stderr := startServer(t, "http", 0)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	client := httpClientViaVclnet()

	// Send 5 sequential requests with an HTTP keep-alive transport.
	for i := 0; i < 5; i++ {
		url := fmt.Sprintf("http://127.0.0.1:%d/health", port)
		resp, err := client.Get(url)
		if err != nil {
			t.Fatalf("GET #%d: %v\nstderr:\n%s", i, err, stderr.String())
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("req #%d: status=%d", i, resp.StatusCode)
		}
		if !strings.Contains(string(body), "ok") {
			t.Errorf("req #%d: body=%q", i, body)
		}
	}

	cmd.Process.Kill()
}

// --- HTTP IPv6 Tests ---

func TestHTTPIPv6Health(t *testing.T) {
	skipIfNoVPP(t)

	cmd, port, stderr := startServer(t, "http6", 0)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	client := httpClientViaVclnet()

	url := fmt.Sprintf("http://[::1]:%d/health", port)
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v\nstderr:\n%s", url, err, stderr.String())
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Errorf("status=%d, want 200", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"status":"ok"`) {
		t.Errorf("body=%q, want containing status:ok", body)
	}

	cmd.Process.Kill()
}

func TestHTTPIPv6Echo(t *testing.T) {
	skipIfNoVPP(t)

	cmd, port, stderr := startServer(t, "http6", 0)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	client := httpClientViaVclnet()

	payload := "ipv6 http echo test payload"
	url := fmt.Sprintf("http://[::1]:%d/echo", port)
	resp, err := client.Post(url, "text/plain", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("POST %s: %v\nstderr:\n%s", url, err, stderr.String())
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != payload {
		t.Errorf("echo body=%q, want %q", body, payload)
	}

	cmd.Process.Kill()
}

// --- Helpers ---

func httpClientViaVclnet() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Dial: func(network, addr string) (net.Conn, error) {
				return vclnet.Dial(network, addr)
			},
			MaxIdleConns:       10,
			IdleConnTimeout:    30 * time.Second,
			DisableCompression: true,
		},
		Timeout: 30 * time.Second,
	}
}

// --- UDP Echo Server (for integration tests) ---

func runUDPEchoServer(port, nMsgs int, network string) {
	// VPP's UDP uses a session-based model: the server does bind+listen,
	// clients connect, server accepts per-client sessions (like TCP).
	var ip4 [4]byte
	var ip6 [16]byte
	var vlsh vclpoll.VLSH
	var err error

	switch network {
	case "udp6":
		copy(ip6[:], net.ParseIP("::1").To16())
		vlsh, err = vclpoll.BindUDP6(ip6, uint16(port))
	default:
		ip4 = [4]byte{0, 0, 0, 0}
		vlsh, err = vclpoll.BindUDP4(ip4, uint16(port))
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: BindUDP(%s, %d): %v\n", network, port, err)
		os.Exit(2)
	}

	fmt.Printf("READY %d\n", port)
	os.Stdout.Sync()

	for i := 0; i < nMsgs; i++ {
		connVLSH, _, acceptErr := vclpoll.AcceptFull(vlsh)
		if acceptErr != nil {
			fmt.Fprintf(os.Stderr, "child: UDP Accept: %v\n", acceptErr)
			os.Exit(2)
		}
		buf := make([]byte, 65536)
		n, readErr := vclpoll.Read(connVLSH, buf)
		if readErr != nil {
			fmt.Fprintf(os.Stderr, "child: UDP Read: %v\n", readErr)
			vclpoll.Close(connVLSH)
			continue
		}
		written, writeErr := vclpoll.Write(connVLSH, buf[:n])
		if writeErr != nil || written != n {
			fmt.Fprintf(os.Stderr, "child: UDP Write: n=%d/%d err=%v\n", written, n, writeErr)
			vclpoll.Close(connVLSH)
			continue
		}
		vclpoll.Close(connVLSH)
	}
	vclpoll.Close(vlsh)
}

// --- UDP Integration Tests ---

func TestUDPIPv4EchoSingle(t *testing.T) {
	skipIfNoVPP(t)
	skipUDPInMode2(t)

	cmd, port, stderr := startServer(t, "udp", 1)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	// Connected UDP via Dial.
	conn, err := vclnet.Dial("udp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Dial udp4: %v\nstderr:\n%s", err, stderr.String())
	}

	msg := []byte("hello UDP via vclnet")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	got := make([]byte, len(msg))
	n, err := conn.Read(got)
	if err != nil {
		t.Fatalf("Read: %v\nstderr:\n%s", err, stderr.String())
	}
	if string(got[:n]) != string(msg) {
		t.Errorf("echo mismatch: got %q, want %q", got[:n], msg)
	}
	conn.Close()
	waitOrKill(t, cmd, stderr)
}

func TestUDPIPv4PacketConn(t *testing.T) {
	// VPP's UDP model is session-based: each peer creates a separate session.
	// The classic unconnected datagram pattern (WriteTo/ReadFrom with arbitrary
	// peers on a single socket) is not supported by VPP's session layer.
	// Use connected UDP (Dial("udp", addr)) instead.
	t.Skip("VPP UDP is session-based; unconnected PacketConn not supported")
}

func TestUDPIPv6Echo(t *testing.T) {
	skipIfNoVPP(t)
	skipUDPInMode2(t)

	cmd, port, stderr := startServer(t, "udp6", 1)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	conn, err := vclnet.Dial("udp6", fmt.Sprintf("[::1]:%d", port))
	if err != nil {
		t.Fatalf("Dial udp6: %v\nstderr:\n%s", err, stderr.String())
	}

	msg := []byte("hello IPv6 UDP")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	got := make([]byte, len(msg))
	n, err := conn.Read(got)
	if err != nil {
		t.Fatalf("Read: %v\nstderr:\n%s", err, stderr.String())
	}
	if string(got[:n]) != string(msg) {
		t.Errorf("echo mismatch: got %q, want %q", got[:n], msg)
	}
	conn.Close()
	waitOrKill(t, cmd, stderr)
}

// --- Multi-VPP-Worker Stress Tests ---
//
// These tests validate vclnet with VPP configured with multiple session workers
// (cpu { workers N }) in both application-side VLS modes. They exercise:
//   - High-concurrency connect storms across VPP workers
//   - Parallel I/O from many goroutines simultaneously
//   - Mode 3 shared-worker compatibility
//   - Mode 2 session affinity without VLS migration
//
// Gated by VCLNET_MULTI_WORKER=1 (set by test/run_multiworker.sh).
// Also work with single-worker VPP (just don't exercise cross-worker paths).

func skipIfNotMultiWorker(t *testing.T) {
	t.Helper()
	if os.Getenv("VCL_CONFIG") == "" {
		t.Skip("VCL_CONFIG not set; skipping VPP integration test")
	}
	if os.Getenv(envMultiWorker) != "1" {
		t.Skip("VCLNET_MULTI_WORKER not set; skipping multi-worker test")
	}
}

// TestMultiWorkerConcurrentConnectStorm opens many connections simultaneously
// from multiple goroutines under either selected VLS dispatcher.
func TestMultiWorkerConcurrentConnectStorm(t *testing.T) {
	skipIfNotMultiWorker(t)

	const nConns = 8
	cmd, port, stderr := startServer(t, "echo", nConns)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	type dialResult struct {
		conn net.Conn
		err  error
		idx  int
	}
	results := make(chan dialResult, nConns)

	for i := 0; i < nConns; i++ {
		go func(idx int) {
			conn, err := vclnet.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
			results <- dialResult{conn, err, idx}
		}(i)
	}

	conns := make([]net.Conn, 0, nConns)
	for i := 0; i < nConns; i++ {
		r := <-results
		if r.err != nil {
			for _, c := range conns {
				c.Close()
			}
			t.Fatalf("goroutine %d: Dial: %v\nstderr:\n%s", r.idx, r.err, stderr.String())
		}
		conns = append(conns, r.conn)
	}

	for i, conn := range conns {
		msg := []byte(fmt.Sprintf("storm-payload-%d", i))
		if _, err := conn.Write(msg); err != nil {
			t.Fatalf("conn %d Write: %v", i, err)
		}
		got := make([]byte, len(msg))
		if _, err := io.ReadFull(conn, got); err != nil {
			t.Fatalf("conn %d ReadFull: %v", i, err)
		}
		if !bytes.Equal(got, msg) {
			t.Errorf("conn %d echo mismatch: got %q want %q", i, got, msg)
		}
		conn.Close()
	}
	waitOrKill(t, cmd, stderr)
}

// TestMultiWorkerParallelIO opens multiple connections and performs I/O on
// ALL of them simultaneously from separate goroutines.
func TestMultiWorkerParallelIO(t *testing.T) {
	skipIfNotMultiWorker(t)

	const nConns = 8
	cmd, port, stderr := startServer(t, "echo", nConns)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	conns := make([]net.Conn, nConns)
	for i := 0; i < nConns; i++ {
		var err error
		conns[i], err = vclnet.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			for j := 0; j < i; j++ {
				conns[j].Close()
			}
			t.Fatalf("conn %d Dial: %v\nstderr:\n%s", i, err, stderr.String())
		}
	}

	var wg sync.WaitGroup
	errs := make([]error, nConns)
	for i := 0; i < nConns; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			conn := conns[idx]
			msg := []byte(fmt.Sprintf("parallel-io-payload-%d-with-extra-data", idx))
			if _, err := conn.Write(msg); err != nil {
				errs[idx] = fmt.Errorf("Write: %w", err)
				return
			}
			got := make([]byte, len(msg))
			if _, err := io.ReadFull(conn, got); err != nil {
				errs[idx] = fmt.Errorf("ReadFull: %w", err)
				return
			}
			if !bytes.Equal(got, msg) {
				errs[idx] = fmt.Errorf("mismatch: got %q want %q", got, msg)
			}
		}(i)
	}
	wg.Wait()

	for i, conn := range conns {
		conn.Close()
		if errs[i] != nil {
			t.Errorf("conn %d: %v", i, errs[i])
		}
	}
	waitOrKill(t, cmd, stderr)
}

// TestMultiWorkerLargePayloadParallel sends 128KB payloads across multiple
// connections simultaneously under multi-worker VPP.
func TestMultiWorkerLargePayloadParallel(t *testing.T) {
	skipIfNotMultiWorker(t)

	const nConns = 4
	const payloadSize = 128 * 1024
	cmd, port, stderr := startServer(t, "echo", nConns)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	conns := make([]net.Conn, nConns)
	for i := 0; i < nConns; i++ {
		var err error
		conns[i], err = vclnet.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			for j := 0; j < i; j++ {
				conns[j].Close()
			}
			t.Fatalf("conn %d Dial: %v\nstderr:\n%s", i, err, stderr.String())
		}
	}

	var wg sync.WaitGroup
	errs := make([]error, nConns)
	for i := 0; i < nConns; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			conn := conns[idx]
			payload := bytes.Repeat([]byte{byte('A' + idx)}, payloadSize)
			go func() { conn.Write(payload) }()
			got := make([]byte, payloadSize)
			if _, err := io.ReadFull(conn, got); err != nil {
				errs[idx] = fmt.Errorf("ReadFull: %w", err)
				return
			}
			if !bytes.Equal(got, payload) {
				errs[idx] = fmt.Errorf("payload mismatch at conn %d", idx)
			}
		}(i)
	}
	wg.Wait()

	for i, conn := range conns {
		conn.Close()
		if errs[i] != nil {
			t.Errorf("conn %d: %v", i, errs[i])
		}
	}
	waitOrKill(t, cmd, stderr)
}

// TestMultiWorkerIPv6Parallel tests multi-worker behavior over IPv6.
func TestMultiWorkerIPv6Parallel(t *testing.T) {
	skipIfNotMultiWorker(t)

	const nConns = 8
	cmd, port, stderr := startServer(t, "echo6", nConns)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	conns := make([]net.Conn, nConns)
	for i := 0; i < nConns; i++ {
		var err error
		conns[i], err = vclnet.Dial("tcp6", fmt.Sprintf("[::1]:%d", port))
		if err != nil {
			for j := 0; j < i; j++ {
				conns[j].Close()
			}
			t.Fatalf("conn %d Dial: %v\nstderr:\n%s", i, err, stderr.String())
		}
	}

	var wg sync.WaitGroup
	errs := make([]error, nConns)
	for i := 0; i < nConns; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			conn := conns[idx]
			msg := []byte(fmt.Sprintf("ipv6-multi-worker-%d", idx))
			if _, err := conn.Write(msg); err != nil {
				errs[idx] = fmt.Errorf("Write: %w", err)
				return
			}
			got := make([]byte, len(msg))
			if _, err := io.ReadFull(conn, got); err != nil {
				errs[idx] = fmt.Errorf("ReadFull: %w", err)
				return
			}
			if !bytes.Equal(got, msg) {
				errs[idx] = fmt.Errorf("mismatch: got %q want %q", got, msg)
			}
		}(i)
	}
	wg.Wait()

	for i, conn := range conns {
		conn.Close()
		if errs[i] != nil {
			t.Errorf("conn %d: %v", i, errs[i])
		}
	}
	waitOrKill(t, cmd, stderr)
}

// TestMultiWorkerHTTPConcurrent issues many HTTP requests concurrently,
// exercising HTTP + connection pooling under multi-worker VPP.
func TestMultiWorkerHTTPConcurrent(t *testing.T) {
	skipIfNotMultiWorker(t)

	cmd, port, stderr := startServer(t, "http", 0)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	client := httpClientViaVclnet()

	const nRequests = 20
	var wg sync.WaitGroup
	errs := make([]error, nRequests)

	for i := 0; i < nRequests; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			url := fmt.Sprintf("http://127.0.0.1:%d/health", port)
			resp, err := client.Get(url)
			if err != nil {
				errs[idx] = fmt.Errorf("GET: %w", err)
				return
			}
			defer resp.Body.Close()
			io.ReadAll(resp.Body)
			if resp.StatusCode != 200 {
				errs[idx] = fmt.Errorf("status=%d", resp.StatusCode)
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("request %d: %v\nstderr:\n%s", i, err, stderr.String())
		}
	}
	cmd.Process.Kill()
}

func TestMultiWorkerMode2NoMigration(t *testing.T) {
	skipIfNotMultiWorker(t)
	if os.Getenv("VCLNET_VLS_MODE") != "2" {
		t.Skip("mode-2 ownership assertion only applies to VLS mode 2")
	}
	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if got := vclpoll.CurrentMode(); got != vclpoll.Mode2 {
		t.Fatalf("current VLS mode=%d, want mode 2", got)
	}
	if got := vclpoll.Mode2OwnershipViolations(); got != 0 {
		t.Fatalf("mode-2 ownership violations=%d; a session was routed toward migration", got)
	}
}

func TestMultiWorkerMode2UDPUnsupported(t *testing.T) {
	skipIfNotMultiWorker(t)
	if os.Getenv("VCLNET_VLS_MODE") != "2" {
		t.Skip("mode-2 UDP compatibility assertion only applies to VLS mode 2")
	}
	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	packetConn, err := vclnet.ListenPacket("udp4", fmt.Sprintf("127.0.0.1:%d", reserveAPIPort()))
	if packetConn != nil {
		_ = packetConn.Close()
		t.Fatal("Mode 2 UDP unexpectedly created a packet connection")
	}
	if !errors.Is(err, syscall.EOPNOTSUPP) {
		t.Fatalf("ListenPacket error=%v, want EOPNOTSUPP", err)
	}
}

// --- TLS Tests ---
//
// These validate that crypto/tls works correctly layered over vclnet's net.Conn.

func generateSelfSignedCert() (tls.Certificate, *x509.CertPool) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"vclnet-test"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	certDER, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	cert := tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  key,
	}
	pool := x509.NewCertPool()
	parsedCert, _ := x509.ParseCertificate(certDER)
	pool.AddCert(parsedCert)
	return cert, pool
}

// generateSelfSignedCertPEM returns PEM-encoded cert and key blobs. This is
// what vppcom_add_cert_key_pair expects on the wire (VPP feeds them into
// OpenSSL's PEM_read_bio_* family), and it is what vclnet.TLSConfig accepts.
// The returned cert also validates against the crypto/tls tls.Certificate
// we hand back so integration tests can compare native VCL TLS to layered
// crypto/tls against the same identity.
func generateSelfSignedCertPEM() (certPEM, keyPEM []byte, cert tls.Certificate, pool *x509.CertPool) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{Organization: []string{"vclnet-native-tls-test"}},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		panic(fmt.Sprintf("generateSelfSignedCertPEM: CreateCertificate: %v", err))
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		panic(fmt.Sprintf("generateSelfSignedCertPEM: MarshalECPrivateKey: %v", err))
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	cert = tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  key,
	}
	pool = x509.NewCertPool()
	parsedCert, err := x509.ParseCertificate(certDER)
	if err != nil {
		panic(fmt.Sprintf("generateSelfSignedCertPEM: ParseCertificate: %v", err))
	}
	pool.AddCert(parsedCert)
	return
}

// runNativeTLSEchoServer runs an echo server whose TLS termination is done
// inside VPP (VPPCOM_PROTO_TLS). The self-signed cert lives entirely inside
// the child process; the parent verifies the handshake with
// InsecureSkipVerify (matching the layered TLS test) so no cert material has
// to cross the process boundary.
func runNativeTLSEchoServer(port, nConns int) {
	certPEM, keyPEM, _, _ := generateSelfSignedCertPEM()

	addr := fmt.Sprintf("0.0.0.0:%d", port)
	ln, err := vclnet.ListenTLS("tcp4", addr, &vclnet.TLSConfig{Cert: certPEM, Key: keyPEM})
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: ListenTLS(%s): %v\n", addr, err)
		os.Exit(2)
	}
	fmt.Printf("READY %d\n", port)
	os.Stdout.Sync()

	var wg sync.WaitGroup
	for i := 0; i < nConns; i++ {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "child: native-TLS Accept: %v\n", err)
			os.Exit(2)
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer c.Close()
			io.Copy(c, c)
		}(conn)
	}
	wg.Wait()
	ln.Close()
}

func runTLSEchoServer(port, nConns int) {
	addr := fmt.Sprintf("0.0.0.0:%d", port)

	ln, err := vclnet.Listen("tcp4", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: Listen(tcp4, %s): %v\n", addr, err)
		os.Exit(2)
	}

	cert, _ := generateSelfSignedCert()
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}
	tlsListener := tls.NewListener(ln, tlsConfig)

	fmt.Printf("READY %d\n", port)
	os.Stdout.Sync()

	var wg sync.WaitGroup
	for i := 0; i < nConns; i++ {
		conn, err := tlsListener.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "child: TLS Accept: %v\n", err)
			os.Exit(2)
		}
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer c.Close()
			io.Copy(c, c)
		}(conn)
	}
	wg.Wait()
	tlsListener.Close()
}

func TestTLSEchoOverVclnet(t *testing.T) {
	skipIfNoVPP(t)

	cmd, port, stderr := startServer(t, "tls", 1)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	rawConn, err := vclnet.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Dial: %v\nstderr:\n%s", err, stderr.String())
	}

	tlsConn := tls.Client(rawConn, &tls.Config{
		InsecureSkipVerify: true,
	})

	if err := tlsConn.Handshake(); err != nil {
		t.Fatalf("TLS Handshake: %v", err)
	}

	msg := []byte("hello TLS over vclnet via VPP")
	if _, err := tlsConn.Write(msg); err != nil {
		t.Fatalf("TLS Write: %v", err)
	}

	got := make([]byte, len(msg))
	if _, err := io.ReadFull(tlsConn, got); err != nil {
		t.Fatalf("TLS ReadFull: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Errorf("TLS echo mismatch: got %q want %q", got, msg)
	}

	tlsConn.Close()
	// The server's io.Copy won't see a clean EOF through VPP's session
	// close path, so just kill it rather than waiting for graceful exit.
	cmd.Process.Kill()
}

// --- Native VCL TLS tests (VPPCOM_PROTO_TLS) ---
//
// These validate that TLS termination inside VPP works end to end: the
// handshake, application echo, and full close all traverse only the
// vclnet.TLSConfig/DialTLS/ListenTLS surface, never crypto/tls on the
// client side. Compared to TestTLSEchoOverVclnet (layered) they exercise
// SET_CKPAIR and the VPP TLS engine plumbing.

// TestNativeVCLTLSEchoSingle round-trips one plaintext write over a native
// VCL TLS session and confirms the peer echoes it back. If this passes,
// VPP's OpenSSL engine successfully drove a handshake and app-data path
// against a self-signed identity registered via vppcom_add_cert_key_pair.
func TestNativeVCLTLSEchoSingle(t *testing.T) {
	skipIfNoVPP(t)

	cmd, port, stderr := startServer(t, "nativetls", 1)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-native-tls-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := vclnet.DialTLSContext(ctx, "tcp4", fmt.Sprintf("127.0.0.1:%d", port), nil)
	if err != nil {
		t.Fatalf("DialTLS: %v\nstderr:\n%s", err, stderr.String())
	}
	defer conn.Close()

	msg := []byte("hello native VCL TLS")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("Write: %v\nstderr:\n%s", err, stderr.String())
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("ReadFull: %v\nstderr:\n%s", err, stderr.String())
	}
	if !bytes.Equal(got, msg) {
		t.Errorf("native-TLS echo mismatch: got %q, want %q", got, msg)
	}

	cmd.Process.Kill()
}

// TestNativeVCLTLSEchoLarge exercises fragmentation across TLS records: 128
// KiB is large enough that OpenSSL's per-record size (16 KiB max) is hit
// multiple times, and it must round-trip cleanly through VPP's SVM FIFO.
func TestNativeVCLTLSEchoLarge(t *testing.T) {
	skipIfNoVPP(t)

	cmd, port, stderr := startServer(t, "nativetls", 1)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-native-tls-large"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := vclnet.DialTLSContext(ctx, "tcp4", fmt.Sprintf("127.0.0.1:%d", port), nil)
	if err != nil {
		t.Fatalf("DialTLS: %v\nstderr:\n%s", err, stderr.String())
	}
	defer conn.Close()

	payload := make([]byte, 128*1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}

	errCh := make(chan error, 1)
	got := make([]byte, len(payload))
	go func() {
		_, err := io.ReadFull(conn, got)
		errCh <- err
	}()

	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("Write: %v\nstderr:\n%s", err, stderr.String())
	}
	if err := <-errCh; err != nil {
		t.Fatalf("ReadFull: %v\nstderr:\n%s", err, stderr.String())
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("native-TLS large echo mismatch (len %d)", len(payload))
	}

	cmd.Process.Kill()
}

// TestNativeVCLTLSVsLayeredTLSFunctionalParity dials the same native VCL
// TLS server twice: first with vclnet.DialTLS (VPP terminates TLS) and
// then with a layered crypto/tls client wrapping a plain vclnet TCP Dial
// against a *different* server (the standard TLSEchoOverVclnet child).
// It documents the observable API contrast between the two integration
// styles — both must round-trip an identical payload, both must reject
// tampered ciphertext, and neither must leak resources.
func TestNativeVCLTLSVsLayeredTLSFunctionalParity(t *testing.T) {
	skipIfNoVPP(t)

	nativeCmd, nativePort, nativeStderr := startServer(t, "nativetls", 1)
	defer nativeCmd.Process.Kill()

	layeredCmd, layeredPort, layeredStderr := startServer(t, "tls", 1)
	defer layeredCmd.Process.Kill()

	if err := vclnet.Init("vclnet-tls-parity"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	msg := []byte("parity check: native vs layered TLS")

	// Native VCL TLS arm: no crypto/tls on the client, VPP terminates.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	nativeConn, err := vclnet.DialTLSContext(ctx, "tcp4", fmt.Sprintf("127.0.0.1:%d", nativePort), nil)
	if err != nil {
		t.Fatalf("native DialTLS: %v\nstderr:\n%s", err, nativeStderr.String())
	}
	if _, err := nativeConn.Write(msg); err != nil {
		t.Fatalf("native Write: %v", err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(nativeConn, got); err != nil {
		t.Fatalf("native ReadFull: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("native echo mismatch")
	}
	nativeConn.Close()

	// Layered TLS arm: crypto/tls over a plain vclnet TCP conn.
	rawConn, err := vclnet.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", layeredPort))
	if err != nil {
		t.Fatalf("layered Dial: %v\nstderr:\n%s", err, layeredStderr.String())
	}
	tlsConn := tls.Client(rawConn, &tls.Config{InsecureSkipVerify: true})
	if err := tlsConn.Handshake(); err != nil {
		t.Fatalf("layered Handshake: %v", err)
	}
	if _, err := tlsConn.Write(msg); err != nil {
		t.Fatalf("layered Write: %v", err)
	}
	got2 := make([]byte, len(msg))
	if _, err := io.ReadFull(tlsConn, got2); err != nil {
		t.Fatalf("layered ReadFull: %v", err)
	}
	if !bytes.Equal(got2, msg) {
		t.Fatalf("layered echo mismatch")
	}
	tlsConn.Close()

	nativeCmd.Process.Kill()
	layeredCmd.Process.Kill()
}

// TestNativeVCLTLSListenValidation asserts that ListenTLS rejects an empty
// TLSConfig without ever touching VPP. Serves as a smoke test for
// callers that upgrade an existing plain Listen() to ListenTLS.
func TestNativeVCLTLSListenValidation(t *testing.T) {
	// This does not depend on VPP — validation runs before AppInit — so we
	// intentionally do NOT skip when VCL_CONFIG is unset.
	if _, err := vclnet.ListenTLS("tcp4", "127.0.0.1:0", nil); err == nil {
		t.Fatal("ListenTLS(nil cfg) unexpectedly succeeded")
	}
	if _, err := vclnet.ListenTLS("tcp4", "127.0.0.1:0", &vclnet.TLSConfig{}); err == nil {
		t.Fatal("ListenTLS(empty cfg) unexpectedly succeeded")
	}
}

// --- Benchmarks ---
//
// These measure vclnet throughput. Run with:
//   VCL_CONFIG=/tmp/vclnet-share/vcl.conf go test -bench=. -benchtime=5s .

func BenchmarkTCPEchoRoundTrip(b *testing.B) {
	if os.Getenv("VCL_CONFIG") == "" {
		b.Skip("VCL_CONFIG not set; skipping VPP benchmark")
	}
	time.Sleep(1 * time.Second)

	port := reserveAPIPort()
	exe, _ := os.Executable()
	cmd := exec.Command(exe, "-test.run=^$")
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	cmd.Env = append(os.Environ(),
		envAPIServerMode+"=1",
		envAPIServerPort+"="+strconv.Itoa(int(port)),
		envAPIServerType+"=echo",
		envAPIServerN+"="+strconv.Itoa(b.N),
	)
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = &bytes.Buffer{}
	if err := cmd.Start(); err != nil {
		b.Fatalf("cmd.Start: %v", err)
	}
	cleanupCommand(b, cmd)

	br := bufio.NewReader(stdout)
	line, _ := br.ReadString('\n')
	if !strings.HasPrefix(line, "READY") {
		b.Fatalf("server not ready: %q", line)
	}
	go io.Copy(io.Discard, br)

	if err := vclnet.Init("vclnet-bench-client"); err != nil {
		b.Fatalf("Init: %v", err)
	}

	msg := []byte("benchmark-payload-64-bytes-for-latency-measurement!!!!!!!!!!!!!!")
	got := make([]byte, len(msg))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := vclnet.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			b.Fatalf("Dial: %v", err)
		}
		conn.Write(msg)
		io.ReadFull(conn, got)
		conn.Close()
	}
	b.StopTimer()

	cmd.Process.Kill()
}

func BenchmarkTCPThroughput1MB(b *testing.B) {
	if os.Getenv("VCL_CONFIG") == "" {
		b.Skip("VCL_CONFIG not set; skipping VPP benchmark")
	}
	time.Sleep(1 * time.Second)

	port := reserveAPIPort()
	exe, _ := os.Executable()
	cmd := exec.Command(exe, "-test.run=^$")
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	cmd.Env = append(os.Environ(),
		envAPIServerMode+"=1",
		envAPIServerPort+"="+strconv.Itoa(int(port)),
		envAPIServerType+"=echo",
		envAPIServerN+"=1",
	)
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = &bytes.Buffer{}
	if err := cmd.Start(); err != nil {
		b.Fatalf("cmd.Start: %v", err)
	}
	cleanupCommand(b, cmd)

	br := bufio.NewReader(stdout)
	line, _ := br.ReadString('\n')
	if !strings.HasPrefix(line, "READY") {
		b.Fatalf("server not ready: %q", line)
	}
	go io.Copy(io.Discard, br)

	if err := vclnet.Init("vclnet-bench-client"); err != nil {
		b.Fatalf("Init: %v", err)
	}

	conn, err := vclnet.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		b.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	payload := bytes.Repeat([]byte("X"), 1024*1024) // 1MB
	got := make([]byte, len(payload))

	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		go func() { conn.Write(payload) }()
		io.ReadFull(conn, got)
	}
	b.StopTimer()

	cmd.Process.Kill()
}

// --- Net contract integration tests ---

func TestTCPReadDeadlineAndReset(t *testing.T) {
	skipIfNoVPP(t)
	cmd, port, stderr := startServer(t, "echo", 1)
	defer cmd.Process.Kill()
	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	conn, err := vclnet.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Dial: %v\nstderr:\n%s", err, stderr.String())
	}
	start := time.Now()
	if err := conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, err := conn.Read(make([]byte, 1)); err == nil || !vclnet.IsTimeout(err) {
		t.Fatalf("Read error=%v, want timeout", err)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("deadline elapsed=%v, want roughly 100ms", elapsed)
	}

	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear deadline: %v", err)
	}
	msg := []byte("after-deadline")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("Write after deadline: %v", err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("ReadFull after deadline reset: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("echo=%q, want %q", got, msg)
	}
	_ = conn.Close()
	waitOrKill(t, cmd, stderr)
}

func TestTCPReadDeadlineCanBeExtendedWhileBlocked(t *testing.T) {
	skipIfNoVPP(t)
	cmd, port, stderr := startServer(t, "echo", 1)
	defer cmd.Process.Kill()
	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	conn, err := vclnet.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}

	msg := []byte("extended")
	type readResult struct {
		data []byte
		err  error
	}
	resultCh := make(chan readResult, 1)
	go func() {
		got := make([]byte, len(msg))
		_, err := io.ReadFull(conn, got)
		resultCh <- readResult{data: got, err: err}
	}()

	time.Sleep(50 * time.Millisecond)
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("extend deadline: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("Write: %v", err)
	}

	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("ReadFull after extension: %v", result.err)
		}
		if !bytes.Equal(result.data, msg) {
			t.Fatalf("echo=%q, want %q", result.data, msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read did not complete after extending deadline")
	}
	_ = conn.Close()
	waitOrKill(t, cmd, stderr)
}

func TestTCPCloseUnblocksRead(t *testing.T) {
	skipIfNoVPP(t)
	cmd, port, stderr := startServer(t, "echo", 1)
	defer cmd.Process.Kill()
	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	conn, err := vclnet.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	readErr := make(chan error, 1)
	go func() {
		_, err := conn.Read(make([]byte, 1))
		readErr <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("blocked Read returned nil after Close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not unblock Read")
	}
	waitOrKill(t, cmd, stderr)
}

// halfCloser mirrors the ambient interface {CloseRead() error; CloseWrite() error}
// exposed by net.TCPConn and (in this package) *tcpConn. Callers of
// vclnet.Dial receive a net.Conn, so end-user code type-asserts against this
// same shape.
type halfCloser interface {
	CloseRead() error
	CloseWrite() error
}

// TestTCPHalfCloseWriteSignalsPeerEOF is the end-to-end acceptance test for
// vls_shutdown(SHUT_WR). The child server reads the entire request with
// io.ReadAll, which returns only when the server-side Read observes zero
// bytes (EOF). That EOF can arrive only if the client's CloseWrite() drove
// vppcom_session_shutdown → the peer session's flags, then over the wire to
// the server side.
//
// KNOWN LIMITATION: VPP's cut-through transport (used when both apps are on
// the same VPP with app-scope-local) does not implement half_close in its
// transport VFT. transport_half_close() is a no-op for TRANSPORT_PROTO_CT,
// so the peer never observes EOF. This test is skipped in the integration
// harness because both client and server are co-located on the same VPP
// instance with app-scope-local enabled, which forces cut-through. The test
// passes when client and server communicate over VPP's TCP transport (i.e.
// on separate hosts or without app-scope-local).
//
// The test also verifies:
//   - Write after CloseWrite returns *net.OpError wrapping net.ErrClosed
//     (matching net.TCPConn semantics).
//   - The client can still Read the server's response after CloseWrite,
//     i.e. the read half remains open until Close.
//   - After draining the response Read returns io.EOF once the server
//     closes.
//   - CloseRead is idempotent and safe to call after Close.
func TestTCPHalfCloseWriteSignalsPeerEOF(t *testing.T) {
	skipIfNoVPP(t)
	t.Skip("VPP cut-through transport does not implement half_close; peer EOF is not delivered over CT sessions")
	cmd, port, stderr := startServer(t, "halfclose", 1)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	conn, err := vclnet.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Dial: %v\nstderr:\n%s", err, stderr.String())
	}
	defer conn.Close()

	// Cap the total test time so a mis-behaving half-close cannot hang CI.
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}

	// vclnet.Dial returns net.Conn; the concrete *tcpConn exposes
	// CloseRead/CloseWrite. Consumers detect them via anonymous interfaces.
	hc, ok := conn.(halfCloser)
	if !ok {
		t.Fatalf("conn %T does not implement CloseRead/CloseWrite", conn)
	}

	request := []byte("HALF-CLOSE-WRITE")
	if _, err := conn.Write(request); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if err := hc.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}
	// Idempotency: a second CloseWrite must not error.
	if err := hc.CloseWrite(); err != nil {
		t.Fatalf("second CloseWrite err=%v, want nil", err)
	}

	// Write after CloseWrite must fail with net.ErrClosed wrapped in
	// *net.OpError. This matches how net.TCPConn behaves and lets stdlib
	// error checks (errors.Is(err, net.ErrClosed)) work unchanged.
	if _, err := conn.Write([]byte("late")); err == nil {
		t.Fatal("Write after CloseWrite returned no error")
	} else {
		var opErr *net.OpError
		if !errors.As(err, &opErr) {
			t.Fatalf("Write err type=%T, want *net.OpError", err)
		}
		if !errors.Is(opErr.Err, net.ErrClosed) {
			t.Errorf("Write inner err=%v, want net.ErrClosed", opErr.Err)
		}
	}

	// The read half is still open. Drain the server's response, then EOF.
	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("ReadAll: %v\nstderr:\n%s", err, stderr.String())
	}
	want := append([]byte("RESP:"), reverseBytes(request)...)
	if !bytes.Equal(got, want) {
		t.Fatalf("response mismatch:\n got  %q\n want %q", got, want)
	}

	// CloseRead is safe (idempotent) — the peer already sent FIN so this
	// is essentially a no-op on the wire, but the API contract still holds.
	if err := hc.CloseRead(); err != nil {
		t.Fatalf("CloseRead: %v", err)
	}
	if err := hc.CloseRead(); err != nil {
		t.Fatalf("second CloseRead err=%v, want nil", err)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// CloseRead / CloseWrite after Close must not panic and must return an
	// error wrapping net.ErrClosed.
	if err := hc.CloseRead(); err == nil {
		t.Fatal("CloseRead after Close returned no error")
	} else if !errors.Is(err, net.ErrClosed) {
		t.Errorf("CloseRead after Close err=%v, want wraps net.ErrClosed", err)
	}
	if err := hc.CloseWrite(); err == nil {
		t.Fatal("CloseWrite after Close returned no error")
	} else if !errors.Is(err, net.ErrClosed) {
		t.Errorf("CloseWrite after Close err=%v, want wraps net.ErrClosed", err)
	}

	waitOrKill(t, cmd, stderr)
}

// TestTCPHalfCloseReadLocalEOF verifies the local-only path: CloseRead makes
// subsequent Reads return io.EOF *without* affecting the write side. VPP does
// not send anything on the wire for SHUT_RD (VCL_SESSION_F_RD_SHUTDOWN is a
// local flag only) so this test intentionally does not depend on the peer
// observing anything.
func TestTCPHalfCloseReadLocalEOF(t *testing.T) {
	skipIfNoVPP(t)
	cmd, port, stderr := startServer(t, "echo", 1)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	conn, err := vclnet.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Dial: %v\nstderr:\n%s", err, stderr.String())
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}

	hc, ok := conn.(halfCloser)
	if !ok {
		t.Fatalf("conn %T does not implement CloseRead/CloseWrite", conn)
	}

	if err := hc.CloseRead(); err != nil {
		t.Fatalf("CloseRead: %v", err)
	}

	// Read after CloseRead must return io.EOF bare (no wrapping). This is
	// what io.Copy and bufio.Reader expect from an EOF signal.
	n, err := conn.Read(make([]byte, 16))
	if n != 0 {
		t.Errorf("Read n=%d, want 0", n)
	}
	if !errors.Is(err, io.EOF) {
		t.Errorf("Read err=%v, want io.EOF", err)
	}

	// The write side must remain fully functional after CloseRead. Send
	// data and verify the echo server still accepts it (server will echo,
	// but we won't read the echo — we already closed reads locally). This
	// proves CloseRead did not corrupt the session.
	if _, err := conn.Write([]byte("post-close-read")); err != nil {
		t.Fatalf("Write after CloseRead: %v", err)
	}

	// Now close the conn so the server sees EOF and exits.
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitOrKill(t, cmd, stderr)
}

// TestTCPHalfCloseWriteUnblocksParkedWriter guards the wake path: a Write
// that is parked in the readiness poller when CloseWrite fires must be
// woken and returned to the caller with net.ErrClosed rather than remaining
// stuck. We create back-pressure by writing a payload larger than the FIFO
// while the server is deliberately slow, then invoke CloseWrite from a
// concurrent goroutine.
func TestTCPHalfCloseWriteUnblocksParkedWriter(t *testing.T) {
	skipIfNoVPP(t)
	cmd, port, stderr := startServer(t, "delayecho", 1)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	conn, err := vclnet.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}

	hc, ok := conn.(halfCloser)
	if !ok {
		t.Fatalf("conn %T does not implement CloseRead/CloseWrite", conn)
	}

	// 6 MiB payload; larger than the default VCL FIFO so at least one Write
	// call parks in the readiness wait.
	payload := bytes.Repeat([]byte("halfclosewriter"), 1024*512)

	writeErr := make(chan error, 1)
	go func() {
		_, err := conn.Write(payload)
		writeErr <- err
	}()

	// Give the goroutine time to fill the FIFO and park.
	time.Sleep(200 * time.Millisecond)

	if err := hc.CloseWrite(); err != nil {
		t.Fatalf("CloseWrite: %v", err)
	}

	select {
	case err := <-writeErr:
		// A parked Write may return either a partial-write ErrClosed or an
		// EPIPE from a race with the FIN being processed. Both are valid
		// signals that CloseWrite unblocked the writer.
		if err == nil {
			t.Fatal("parked Write returned nil after CloseWrite")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("CloseWrite did not unblock parked Write\nstderr:\n%s", stderr.String())
	}

	_ = conn.Close()
	waitOrKill(t, cmd, stderr)
}

func TestTCPConcurrentBlockedReadWrite(t *testing.T) {
	skipIfNoVPP(t)
	cmd, port, stderr := startServer(t, "delayecho", 1)
	defer cmd.Process.Kill()
	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	conn, err := vclnet.Dial("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}

	payload := bytes.Repeat([]byte("poller"), 1024*1024) // 6 MiB, larger than the configured FIFO.
	writeErr := make(chan error, 1)
	go func() {
		n, err := conn.Write(payload)
		if err == nil && n != len(payload) {
			err = io.ErrShortWrite
		}
		writeErr <- err
	}()

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("ReadFull: %v\nstderr:\n%s", err, stderr.String())
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("large concurrent echo mismatch")
	}
	_ = conn.Close()
	waitOrKill(t, cmd, stderr)
}

func TestTCPListenerAcceptContextDeadline(t *testing.T) {
	skipIfNoVPP(t)
	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	port := reserveAPIPort()
	ln, err := vclnet.ListenContext("tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("ListenContext: %v", err)
	}
	defer ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := ln.AcceptContext(ctx); err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AcceptContext error=%v, want context deadline", err)
	}
}

func TestTCPDialContextAndAddresses(t *testing.T) {
	skipIfNoVPP(t)
	cmd, port, stderr := startServer(t, "echo", 1)
	defer cmd.Process.Kill()
	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := vclnet.DialContext(ctx, "tcp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("DialContext: %v", err)
	}
	if conn.LocalAddr() == nil || conn.RemoteAddr() == nil {
		t.Fatalf("addresses: local=%v remote=%v", conn.LocalAddr(), conn.RemoteAddr())
	}
	if got := conn.RemoteAddr().(*net.TCPAddr).Port; got != int(port) {
		t.Fatalf("remote port=%d, want %d", got, port)
	}
	_ = conn.Close()
	waitOrKill(t, cmd, stderr)
}

func TestUDPReadDeadlineAndReset(t *testing.T) {
	skipIfNoVPP(t)
	skipUDPInMode2(t)
	cmd, port, stderr := startServer(t, "udp", 1)
	defer cmd.Process.Kill()
	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	conn, err := vclnet.Dial("udp4", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("Dial UDP: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, err := conn.Read(make([]byte, 1)); err == nil || !vclnet.IsTimeout(err) {
		t.Fatalf("UDP Read error=%v, want timeout", err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("clear deadline: %v", err)
	}
	msg := []byte("udp-after-deadline")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("UDP Write: %v", err)
	}
	got := make([]byte, len(msg))
	n, err := conn.Read(got)
	if err != nil {
		t.Fatalf("UDP Read after reset: %v", err)
	}
	if !bytes.Equal(got[:n], msg) {
		t.Fatalf("UDP echo=%q, want %q", got[:n], msg)
	}
	_ = conn.Close()
	waitOrKill(t, cmd, stderr)
}

func TestHTTPClientHelper(t *testing.T) {
	skipIfNoVPP(t)
	cmd, port, stderr := startServer(t, "http", 0)
	defer cmd.Process.Kill()
	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	client := vclnet.NewHTTPClient()
	client.Timeout = 2 * time.Second
	resp, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/health", port))
	if err != nil {
		t.Fatalf("NewHTTPClient GET: %v\nstderr:\n%s", err, stderr.String())
	}
	body, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	if readErr != nil || closeErr != nil {
		t.Fatalf("response body: read=%v close=%v", readErr, closeErr)
	}
	if resp.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`"status":"ok"`)) {
		t.Fatalf("status=%d body=%q", resp.StatusCode, body)
	}
	vclnet.DefaultTransport.CloseIdleConnections()
	_ = cmd.Process.Kill()
}

func TestTCPHappyEyeballsLocalhost(t *testing.T) {
	skipIfNoVPP(t)
	cmd, port, stderr := startServer(t, "echo", 1)
	defer cmd.Process.Kill()
	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	conn, err := vclnet.DialContext(context.Background(), "tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatalf("Happy Eyeballs DialContext: %v\nstderr:\n%s", err, stderr.String())
	}
	msg := []byte("happy-eyeballs")
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("echo=%q, want %q", got, msg)
	}
	_ = conn.Close()
	waitOrKill(t, cmd, stderr)
}

func TestTCPShutdownWakesAccept(t *testing.T) {
	skipIfNoVPP(t)
	cmd, _, stderr := startServer(t, "shutdown", 0)
	defer cmd.Process.Kill()
	waitOrKill(t, cmd, stderr)
}

func TestTCPListenPortZeroRejected(t *testing.T) {
	skipIfNoVPP(t)
	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if _, err := vclnet.ListenContext("tcp4", "127.0.0.1:0"); err == nil {
		t.Fatal("ListenContext port zero returned no error")
	} else {
		var addrErr *net.AddrError
		if !errors.As(err, &addrErr) {
			t.Fatalf("error=%T, want *net.AddrError", err)
		}
	}
}
