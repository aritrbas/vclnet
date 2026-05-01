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

	"vclnet"
	"vclnet/internal/vclpoll"
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

func TestMain(m *testing.M) {
	if os.Getenv(envAPIServerMode) == "1" {
		runServerChild()
		return
	}
	os.Exit(m.Run())
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
	case "shutdown":
		runShutdownSelfTest(port)
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
		vclpoll.Write(connVLSH, buf[:n])
		vclpoll.Close(connVLSH)
	}
	vclpoll.Close(vlsh)
}

// --- UDP Integration Tests ---

func TestUDPIPv4EchoSingle(t *testing.T) {
	skipIfNoVPP(t)

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

	got := make([]byte, len(msg))
	n, err := conn.Read(got)
	if err != nil {
		t.Fatalf("Read: %v", err)
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

	got := make([]byte, len(msg))
	n, err := conn.Read(got)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(got[:n]) != string(msg) {
		t.Errorf("echo mismatch: got %q, want %q", got[:n], msg)
	}
	conn.Close()
	waitOrKill(t, cmd, stderr)
}

// --- Multi-VPP-Worker Stress Tests ---
//
// These tests validate vclnet with VPP configured with multiple worker threads where VPP runs
// with multiple worker threads (cpu { workers N }). They exercise:
//   - High-concurrency connect storms across VPP workers
//   - Parallel I/O from many goroutines simultaneously
//   - Session distribution across VPP worker threads
//   - The vls_mt_session_should_migrate path when sessions are accessed from
//     different OS threads than the one that created them
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
// from multiple goroutines. With multi-worker VPP, sessions are distributed
// across VPP workers while the application remains in shared VLS mode 3.
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
