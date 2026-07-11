package vclnet_test

// HTTP/2 and gRPC integration tests over vclnet.
//
// Server side: subprocess re-execs itself with VCLNET_API_SERVER_MODE=1 and a
// server-type selector, opens a vclnet listener, and serves either h2c
// (cleartext HTTP/2), h2 (HTTP/2 over TLS with ALPN), or grpc. Parent-side
// test process opens a client through vclnet.DialContext / DialTLSContext
// and drives the protocol.
//
// The tests verify that vclnet's net.Conn / net.Listener are byte-transparent
// enough for real HTTP/2 and gRPC stacks to work end-to-end — no framing
// bugs, no premature EOF, no half-close mishandling.

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aritrbas/vclnet"

	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// ---------------- child-side servers ----------------

// runH2CServer serves HTTP/2 in cleartext (h2c) over a plain vclnet listener.
// A real HTTP/2 stack sits on top; the vclnet layer only sees framed bytes.
func runH2CServer(port int) {
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	ln, err := vclnet.Listen("tcp4", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: h2c Listen: %v\n", err)
		os.Exit(2)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "ok proto=%s", r.Proto)
	})
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.Copy(w, r.Body)
	})

	// Go 1.24+ exposes UnencryptedHTTP2 via http.Server.Protocols, which
	// replaces the deprecated x/net/http2/h2c wrapper. The server accepts
	// prior-knowledge HTTP/2 clients over the plain vclnet listener.
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	server := &http.Server{
		Handler:   mux,
		Protocols: protocols,
	}

	fmt.Printf("READY %d\n", port)
	_ = os.Stdout.Sync()
	_ = server.Serve(ln)
}

// runH2TLSServer serves HTTP/2 over layered crypto/tls (ALPN "h2") on top of
// a vclnet listener. This is the same layered-TLS path used by
// TestTLSEchoOverVclnet, plus TLSConfig.NextProtos advertising h2.
func runH2TLSServer(port int) {
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	ln, err := vclnet.Listen("tcp4", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: h2tls Listen: %v\n", err)
		os.Exit(2)
	}

	cert, _ := generateSelfSignedCert()
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"h2"},
	}
	tlsLn := tls.NewListener(ln, tlsConfig)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintf(w, "ok proto=%s alpn=%s", r.Proto, r.TLS.NegotiatedProtocol)
	})
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.Copy(w, r.Body)
	})

	server := &http.Server{
		Handler:   mux,
		TLSConfig: tlsConfig,
	}
	// Enable HTTP/2 on the server; without this, http.Server falls back to
	// HTTP/1.1 even when ALPN is negotiated on the TLS listener.
	if err := http2.ConfigureServer(server, &http2.Server{}); err != nil {
		fmt.Fprintf(os.Stderr, "child: h2tls ConfigureServer: %v\n", err)
		os.Exit(2)
	}

	fmt.Printf("READY %d\n", port)
	_ = os.Stdout.Sync()
	_ = server.Serve(tlsLn)
}

// runGRPCServer serves gRPC (cleartext) over a vclnet listener. Uses the
// stock health-check service that ships with grpc-go so no protoc codegen
// is required — the pb stubs are already vendored in the module cache.
func runGRPCServer(port int) {
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	ln, err := vclnet.Listen("tcp4", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: grpc Listen: %v\n", err)
		os.Exit(2)
	}

	server := grpc.NewServer()
	healthpb.RegisterHealthServer(server, &grpcHealthSrv{})

	fmt.Printf("READY %d\n", port)
	_ = os.Stdout.Sync()
	if err := server.Serve(ln); err != nil {
		fmt.Fprintf(os.Stderr, "child: grpc Serve: %v\n", err)
		os.Exit(2)
	}
}

// runGRPCTLSServer serves gRPC with layered TLS credentials on top of
// vclnet.Listen. Exercises the ALPN + gRPC stack combination end-to-end.
func runGRPCTLSServer(port int) {
	addr := fmt.Sprintf("0.0.0.0:%d", port)
	ln, err := vclnet.Listen("tcp4", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "child: grpctls Listen: %v\n", err)
		os.Exit(2)
	}

	cert, _ := generateSelfSignedCert()
	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		// gRPC-Go configures ALPN=h2 itself under credentials.NewTLS, but
		// setting it explicitly makes the intent readable.
		NextProtos: []string{"h2"},
	})

	server := grpc.NewServer(grpc.Creds(creds))
	healthpb.RegisterHealthServer(server, &grpcHealthSrv{})

	fmt.Printf("READY %d\n", port)
	_ = os.Stdout.Sync()
	if err := server.Serve(ln); err != nil {
		fmt.Fprintf(os.Stderr, "child: grpctls Serve: %v\n", err)
		os.Exit(2)
	}
}

// grpcHealthSrv is a minimal Health service. Check() returns SERVING for
// any non-empty service name and NOT_SERVING for "down". Watch() streams
// a fixed sequence of state transitions so the streaming code path is
// exercised too.
type grpcHealthSrv struct {
	healthpb.UnimplementedHealthServer
}

func (s *grpcHealthSrv) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	status := healthpb.HealthCheckResponse_SERVING
	if req.Service == "down" {
		status = healthpb.HealthCheckResponse_NOT_SERVING
	}
	return &healthpb.HealthCheckResponse{Status: status}, nil
}

func (s *grpcHealthSrv) Watch(req *healthpb.HealthCheckRequest, stream healthpb.Health_WatchServer) error {
	// Send three sequential status updates to exercise server-streaming.
	states := []healthpb.HealthCheckResponse_ServingStatus{
		healthpb.HealthCheckResponse_UNKNOWN,
		healthpb.HealthCheckResponse_SERVING,
		healthpb.HealthCheckResponse_NOT_SERVING,
	}
	for _, st := range states {
		if err := stream.Send(&healthpb.HealthCheckResponse{Status: st}); err != nil {
			return err
		}
	}
	return nil
}

// ---------------- parent-side tests ----------------

// TestHTTP2CleartextOverVclnet dials an h2c server through vclnet.Dial and
// verifies that Go's http2.Transport can drive real HTTP/2 framing over the
// resulting byte stream — GET, POST body echo, and concurrent streams.
func TestHTTP2CleartextOverVclnet(t *testing.T) {
	skipIfNoVPP(t)

	cmd, port, stderr := startServer(t, "h2c", 0)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	target := fmt.Sprintf("127.0.0.1:%d", port)
	// http2.Transport with AllowHTTP=true and a custom DialTLS that returns
	// a plain conn (h2c doesn't do TLS).
	tr := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
			return vclnet.DialContext(ctx, "tcp4", addr)
		},
	}
	// Force full teardown of any HTTP/2 conns so their background ping /
	// keep-alive goroutines don't outlive this test and stress VPP's MQ
	// ring while subsequent tests run. The trailing sleep gives VPP a
	// window to drain queued session events before the next test opens a
	// fresh app — without it, VPP's app MQ ring occasionally logs
	// "svm_msg_q_free_msg: message out of order" and reject the next
	// test's I/O.
	t.Cleanup(func() {
		tr.CloseIdleConnections()
		time.Sleep(50 * time.Millisecond)
	})
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}

	// 1. Basic GET, verify HTTP/2 was negotiated.
	resp, err := client.Get("http://" + target + "/health")
	if err != nil {
		t.Fatalf("Get /health: %v\nstderr:\n%s", err, stderr.String())
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("Get: proto major=%d, want 2 (body=%q)", resp.ProtoMajor, body)
	}
	if !strings.Contains(string(body), "HTTP/2.0") {
		t.Fatalf("Get body=%q, want proto=HTTP/2.0", body)
	}

	// 2. POST body echo.
	payload := strings.Repeat("x", 8192)
	resp, err = client.Post("http://"+target+"/echo", "application/octet-stream", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("Post /echo: %v", err)
	}
	got, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(got) != payload {
		t.Fatalf("/echo mismatch: got %d bytes, want %d", len(got), len(payload))
	}

	// 3. Concurrent streams: HTTP/2 multiplexes N requests over one
	// connection. If vclnet's byte transport is faithful, all N succeed.
	const nStreams = 8
	var wg sync.WaitGroup
	errs := make(chan error, nStreams)
	for i := 0; i < nStreams; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf("stream-%d", i)
			r, err := client.Post("http://"+target+"/echo", "text/plain", strings.NewReader(body))
			if err != nil {
				errs <- fmt.Errorf("stream %d Post: %w", i, err)
				return
			}
			b, _ := io.ReadAll(r.Body)
			_ = r.Body.Close()
			if string(b) != body {
				errs <- fmt.Errorf("stream %d: got %q want %q", i, b, body)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}

// TestHTTP2TLSOverVclnet drives HTTP/2 over layered crypto/tls on top of
// vclnet.Dial. Exercises the full ALPN → HTTP/2 upgrade sequence.
func TestHTTP2TLSOverVclnet(t *testing.T) {
	skipIfNoVPP(t)

	cmd, port, stderr := startServer(t, "h2tls", 0)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	target := fmt.Sprintf("127.0.0.1:%d", port)
	tr := &http2.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{"h2"},
		},
		DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
			raw, err := vclnet.DialContext(ctx, "tcp4", addr)
			if err != nil {
				return nil, err
			}
			c := tls.Client(raw, cfg)
			if err := c.HandshakeContext(ctx); err != nil {
				_ = raw.Close()
				return nil, err
			}
			return c, nil
		},
	}
	t.Cleanup(func() {
		tr.CloseIdleConnections()
		time.Sleep(50 * time.Millisecond)
	})
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}

	resp, err := client.Get("https://" + target + "/health")
	if err != nil {
		t.Fatalf("Get /health: %v\nstderr:\n%s", err, stderr.String())
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("Get: proto major=%d, want 2 (body=%q)", resp.ProtoMajor, body)
	}
	if !strings.Contains(string(body), "alpn=h2") {
		t.Fatalf("Get body=%q, want alpn=h2", body)
	}
}

// TestGRPCOverVclnet verifies a full unary + server-streaming gRPC exchange
// runs over a vclnet-backed dial. This is the widest protocol validation
// point in the suite — it exercises HTTP/2 framing, gRPC headers, HPACK,
// flow control, and half-closes on top of the vclnet byte pipe.
func TestGRPCOverVclnet(t *testing.T) {
	skipIfNoVPP(t)

	cmd, port, stderr := startServer(t, "grpc", 0)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	target := fmt.Sprintf("127.0.0.1:%d", port)
	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		return vclnet.DialContext(ctx, "tcp4", addr)
	}
	conn, err := grpc.NewClient(target,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v\nstderr:\n%s", err, stderr.String())
	}
	// Close registered via Cleanup so the client-side keepalive/ping
	// goroutines are torn down before subsequent tests share VPP MQ state.
	t.Cleanup(func() {
		_ = conn.Close()
		time.Sleep(50 * time.Millisecond)
	})

	client := healthpb.NewHealthClient(conn)

	// Unary: default service → SERVING.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("Check: status=%v, want SERVING", resp.Status)
	}

	// Unary: "down" service → NOT_SERVING.
	resp, err = client.Check(ctx, &healthpb.HealthCheckRequest{Service: "down"})
	if err != nil {
		t.Fatalf("Check(down): %v", err)
	}
	if resp.Status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("Check(down): status=%v, want NOT_SERVING", resp.Status)
	}

	// Server-streaming: Watch delivers UNKNOWN, SERVING, NOT_SERVING.
	stream, err := client.Watch(ctx, &healthpb.HealthCheckRequest{Service: ""})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	want := []healthpb.HealthCheckResponse_ServingStatus{
		healthpb.HealthCheckResponse_UNKNOWN,
		healthpb.HealthCheckResponse_SERVING,
		healthpb.HealthCheckResponse_NOT_SERVING,
	}
	for i, w := range want {
		msg, err := stream.Recv()
		if err != nil {
			t.Fatalf("Watch stream Recv[%d]: %v", i, err)
		}
		if msg.Status != w {
			t.Fatalf("Watch[%d]: status=%v, want %v", i, msg.Status, w)
		}
	}
	// Server closed the stream after 3 messages; next Recv should hit EOF.
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("Watch stream trailing Recv: err=%v, want io.EOF", err)
	}
}

// TestGRPCTLSOverVclnet mirrors TestGRPCOverVclnet but with layered TLS
// credentials, so gRPC's TLS handshake and ALPN negotiation ride on top of
// vclnet's byte pipe.
func TestGRPCTLSOverVclnet(t *testing.T) {
	skipIfNoVPP(t)

	cmd, port, stderr := startServer(t, "grpctls", 0)
	defer cmd.Process.Kill()

	if err := vclnet.Init("vclnet-test-client"); err != nil {
		t.Fatalf("Init: %v", err)
	}

	target := fmt.Sprintf("127.0.0.1:%d", port)
	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		return vclnet.DialContext(ctx, "tcp4", addr)
	}
	creds := credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"h2"},
	})
	conn, err := grpc.NewClient(target,
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(creds),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v\nstderr:\n%s", err, stderr.String())
	}
	t.Cleanup(func() {
		_ = conn.Close()
		time.Sleep(50 * time.Millisecond)
	})

	client := healthpb.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("Check: status=%v, want SERVING", resp.Status)
	}
}
