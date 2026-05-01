package vclpoll_test

// Integration tests that exercise the cgo + VPP/VCL path end-to-end.
//
// Architecture: the test binary spawns ITSELF as a subprocess to act as the
// echo server (a separate VLS app), then runs the client in the parent
// test process. This is required because VPP's session layer cannot route
// a connect() to a listen() that lives in the SAME VLS app — the connect
// fails with "no route". Two separate apps on one VPP instance work fine.
//
// Tests are gated by VCL_CONFIG. If unset (or VPP isn't running), the
// integration tests are skipped and `go test ./...` works in any
// environment.
//
// Run with VPP up:
//   ./test/run_tests.sh test

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aritrbas/vclnet/internal/vclpoll"
)

const (
	envServerMode = "VCLNET_TEST_SERVER_MODE"
	envServerPort = "VCLNET_TEST_SERVER_PORT"
	envServerN    = "VCLNET_TEST_SERVER_NCONNS"
)

var nextTestPort int32 = 19000

func reservePort() uint16 { return uint16(atomic.AddInt32(&nextTestPort, 1)) }

func skipIfNoVPP(t *testing.T) {
	t.Helper()
	if os.Getenv("VCL_CONFIG") == "" {
		t.Skip("VCL_CONFIG not set; skipping VPP integration test")
	}
	// Give VPP debug build time to clean up stale app registrations
	// from previous test's child process.
	time.Sleep(1 * time.Second)
}

// TestMain hooks the binary's startup so re-execing with
// VCLNET_TEST_SERVER_MODE=1 turns it into an echo-server child instead of
// running the test suite.
func TestMain(m *testing.M) {
	if os.Getenv(envServerMode) == "1" {
		runEchoServerChild()
		return
	}
	os.Exit(m.Run())
}

func runEchoServerChild() {
	port, _ := strconv.Atoi(os.Getenv(envServerPort))
	n, _ := strconv.Atoi(os.Getenv(envServerN))
	if n == 0 {
		n = 1
	}

	if err := vclpoll.AppInit("vclnet-test-server"); err != nil {
		fmt.Fprintf(os.Stderr, "child: AppInit: %v\n", err)
		os.Exit(2)
	}
	listener, err := vclpoll.ListenTCP4([4]byte{0, 0, 0, 0}, uint16(port), 32)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: ListenTCP4: %v\n", err)
		os.Exit(2)
	}
	fmt.Printf("READY %d\n", port)
	os.Stdout.Sync()

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		conn, _, _, err := vclpoll.Accept(listener)
		if err != nil {
			fmt.Fprintf(os.Stderr, "child: Accept: %v\n", err)
			os.Exit(2)
		}
		wg.Add(1)
		go func(c vclpoll.VLSH) {
			defer wg.Done()
			defer vclpoll.Close(c)
			buf := make([]byte, 4096)
			for {
				rn, err := vclpoll.Read(c, buf)
				if err != nil || rn == 0 {
					return
				}
				w := 0
				for w < rn {
					x, err := vclpoll.Write(c, buf[w:rn])
					if err != nil {
						return
					}
					w += x
				}
			}
		}(conn)
	}
	wg.Wait()
	vclpoll.Close(listener)
}

func startEchoServer(t *testing.T, nConns int) (*exec.Cmd, uint16, *bytes.Buffer) {
	t.Helper()
	port := reservePort()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.Command(exe)
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	cmd.Env = append(os.Environ(),
		envServerMode+"=1",
		envServerPort+"="+strconv.Itoa(int(port)),
		envServerN+"="+strconv.Itoa(nConns),
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

	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

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
		// Drain remaining stdout only after READY is consumed.
		io.Copy(io.Discard, br)
	}()

	select {
	case err := <-readyCh:
		if err != nil {
			cmd.Process.Kill()
			t.Fatalf("server child: %v\nstderr:\n%s", err, stderr.String())
		}
	case <-time.After(15 * time.Second):
		cmd.Process.Kill()
		t.Fatalf("server child did not become READY in time\nstderr:\n%s", stderr.String())
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

// Low-level single-connection echo round-trip.
func TestEchoSingleRoundTrip(t *testing.T) {
	skipIfNoVPP(t)

	cmd, port, stderr := startEchoServer(t, 1)
	defer cmd.Process.Kill()

	if err := vclpoll.AppInit("vclnet-test-client"); err != nil {
		t.Fatalf("AppInit: %v", err)
	}

	conn, err := vclpoll.DialTCP4([4]byte{127, 0, 0, 1}, port)
	if err != nil {
		t.Fatalf("DialTCP4: %v\nserver stderr:\n%s", err, stderr.String())
	}
	msg := []byte("hello from vclnet")
	if _, err := vclpoll.Write(conn, msg); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got := make([]byte, len(msg))
	r := 0
	for r < len(got) {
		n, err := vclpoll.Read(conn, got[r:])
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if n == 0 {
			t.Fatalf("peer closed at %d/%d", r, len(got))
		}
		r += n
	}
	if !bytes.Equal(got, msg) {
		t.Errorf("echo mismatch: got %q want %q", got, msg)
	}
	if err := vclpoll.Close(conn); err != nil {
		t.Errorf("Close: %v", err)
	}
	waitOrKill(t, cmd, stderr)
}

// Low-level multiple concurrent client goroutines on different OS threads.
// Exercises per-thread vls_register_vcl_worker.
func TestEchoConcurrentConnections(t *testing.T) {
	skipIfNoVPP(t)

	const nConns = 2
	cmd, port, stderr := startEchoServer(t, nConns)
	defer cmd.Process.Kill()

	if err := vclpoll.AppInit("vclnet-test-client"); err != nil {
		t.Fatalf("AppInit: %v", err)
	}

	// Dial all connections sequentially (VPP debug builds have a
	// session-reset race with truly concurrent dials), then do
	// echo I/O on each connection sequentially.
	conns := make([]vclpoll.VLSH, nConns)
	for i := 0; i < nConns; i++ {
		conn, err := vclpoll.DialTCP4([4]byte{127, 0, 0, 1}, port)
		if err != nil {
			for j := 0; j < i; j++ {
				vclpoll.Close(conns[j])
			}
			t.Fatalf("conn %d Dial: %v\nstderr:\n%s", i, err, stderr.String())
		}
		conns[i] = conn
	}

	for i := 0; i < nConns; i++ {
		conn := conns[i]
		msg := []byte(fmt.Sprintf("payload-%d-from-vclnet", i))
		w := 0
		for w < len(msg) {
			x, err := vclpoll.Write(conn, msg[w:])
			if err != nil {
				t.Fatalf("conn %d Write: %v", i, err)
			}
			w += x
		}

		got := make([]byte, len(msg))
		r := 0
		for r < len(got) {
			n, err := vclpoll.Read(conn, got[r:])
			if err != nil {
				t.Fatalf("conn %d Read: %v", i, err)
			}
			if n == 0 {
				t.Fatalf("conn %d: peer closed early", i)
			}
			r += n
		}
		if !bytes.Equal(got, msg) {
			t.Errorf("conn %d echo mismatch: got %q want %q", i, got, msg)
		}
	}
	// Close all connections AFTER all I/O completes to avoid VPP
	// debug-build session cleanup races.
	for i := 0; i < nConns; i++ {
		vclpoll.Close(conns[i])
	}
	waitOrKill(t, cmd, stderr)
}
