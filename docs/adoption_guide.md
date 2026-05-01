# VCLNET adoption guide

This guide describes the behavior that is implemented and tested in this
repository. Read [../summary.md](../summary.md) for current limitations and the
canonical pending-work list before committing to production adoption.

## 1. Prerequisites

- Go 1.26 or newer.
- A VPP build with the session layer and app socket API enabled.
- VPP headers and `libvppcom.so` available at build time.
- A `libvppcom.so` that exports `vls_unregister_vcl_worker`. The adjacent VPP
  review commit `032b24d04` supplies this API; stock-release support is pending.
- A running VPP process and an accessible app socket at runtime.
- A `VCL_CONFIG` file.

Build discovery is driven by `pkg-config: vppcom`. VPP does not ship a `.pc`
file today, so the repository provides `pkgconfig/vppcom.pc.in`; render it once
per install prefix and it becomes visible to `pkg-config`:

```bash
make pc VPP_PREFIX=/opt/vpp
export PKG_CONFIG_PATH="$PWD/pkgconfig:$PKG_CONFIG_PATH"
```

The Makefile's `build`, `unit`, `race`, `test`, and `vet` targets auto-run
`make pc` and prefix the correct `PKG_CONFIG_PATH` themselves. Set
`VCLNET_SKIP_PC=1` if you have already installed a system-wide `vppcom.pc`.

pkg-config confirms where a library is, not whether it has the API vclnet
needs. Verify the selected binary before building:

```bash
nm -D /path/to/libvppcom.so | grep -w vls_unregister_vcl_worker
```

Until that API lands in a supported VPP release, carry and pin the equivalent
VPP patch in every build and runtime image.

VPP startup must include:

```text
session { enable use-app-socket-api }
```

Example `vcl.conf`:

```text
vcl {
  rx-fifo-size 4000000
  tx-fifo-size 4000000
  app-scope-local
  app-scope-global
  use-mq-eventfd
  app-socket-api /run/vpp/app_ns_sockets/default
}
```

This configuration selects the default Mode 3 dispatcher. For opt-in Mode 2,
add `multi-thread-workers` inside the `vcl` block and initialize with more than
one worker. The application setting and VCL token must agree.

## 2. Initialize once

Call `Init` before creating listeners or connections and handle its error.

```go
if err := vclnet.Init("my-service"); err != nil {
    log.Fatalf("initialize VCL: %v", err)
}
```

To opt into Mode 2:

```go
if err := vclnet.InitWithOptions("my-service", vclnet.Options{Workers: 4}); err != nil {
    log.Fatalf("initialize VCL Mode 2: %v", err)
}
```

The same selection can be made out of band with
`VCLNET_VLS_MODE=2 VCLNET_WORKERS=4`. `VCLNET_VLS_MODE=3` forces the
compatibility dispatcher. Environment values override `Options`; invalid values
and a missing Mode 2 VCL token fail initialization.

Mode 2 supports both TCP and UDP. Mode 2 UDP connects block on the worker
thread until fully established, working around a VPP half-open session cleanup
race. Mode 2 concurrent listeners have an unresolved P0 accept/MQ crash; do
not use Mode 2 for production servers. See
[mode2_accept_mq_investigation.md](mode2_accept_mq_investigation.md).

Repeated calls are no-ops after the first result. After `Shutdown`, Init and
new public network operations return `vclnet.ErrClosed`; reinitialization in
the same process is not supported.

For command-line services, the provided signal handler performs VCL teardown
on SIGINT or SIGTERM:

```go
vclnet.InstallSignalHandler()
```

Applications with an existing lifecycle manager can call `Shutdown`
directly. Stop admitting work and drain handlers first.

## 3. TCP server

```go
ln, err := vclnet.Listen("tcp", ":8080")
if err != nil {
    log.Fatal(err)
}
defer ln.Close()

server := &http.Server{Handler: handler}
if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
    log.Fatal(err)
}
```

Networks:

| Network | Behavior |
| --- | --- |
| `tcp4` | IPv4 only |
| `tcp6` | IPv6 only and V6-only socket attribute |
| `tcp` | Listener resolves one address; dialer uses dual-stack candidates |

If an accept must be cancellable, use the public listener type:

```go
ln, err := vclnet.ListenContext("tcp4", "127.0.0.1:8080")
if err != nil {
    log.Fatal(err)
}
ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()

conn, err := ln.AcceptContext(ctx)
```

Port zero is rejected because the validated VCL build does not allocate an
ephemeral VLS listener.

A context deadline is reported as `context.DeadlineExceeded`, while listener
or package shutdown is reported as `vclnet.ErrClosed`, both inside a
`*net.OpError`.

## 4. TCP client and deadlines

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

conn, err := vclnet.DialContext(ctx, "tcp", "backend.example:8080")
if err != nil {
    log.Fatal(err)
}
defer conn.Close()
```

For `"tcp"`, addresses are interleaved IPv6/IPv4 and attempts are staggered
by 250 ms by default. Configure it with `Dialer.FallbackDelay`.

I/O deadlines follow the `net.Conn` contract for the tested paths: setting,
clearing, or extending a deadline wakes an operation already parked in the selected readiness dispatcher.

```go
if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
    log.Fatal(err)
}
n, err := conn.Read(buf)
if vclnet.IsTimeout(err) {
    // deadline expired
}
_ = n

// Clear the deadline.
_ = conn.SetReadDeadline(time.Time{})
```

`Close` wakes a blocked read. One reader and one writer can wait concurrently
on the same connection.

## 5. HTTP clients

Use the supplied transport when you need custom client settings:

```go
transport := vclnet.Transport(&vclnet.Dialer{Timeout: 5 * time.Second})
client := &http.Client{
    Transport: transport,
    Timeout:   30 * time.Second,
}
```

Or use the shared default:

```go
client := vclnet.NewHTTPClient()
resp, err := client.Get("http://127.0.0.1:8080/health")
```

The test suite covers HTTP/1.1, HTTP/2 (cleartext prior-knowledge and
TLS-with-ALPN), and gRPC (unary and server-streaming, cleartext and TLS).

## 6. TLS

vclnet supports two TLS paths.

### 6.1 Layered `crypto/tls` (default choice)

Standard Go TLS can wrap a vclnet TCP connection:

```go
raw, err := vclnet.Dial("tcp4", "10.0.0.1:443")
if err != nil {
    log.Fatal(err)
}
tlsConn := tls.Client(raw, &tls.Config{
    ServerName: "backend.example",
    MinVersion: tls.VersionTLS12,
})
if err := tlsConn.Handshake(); err != nil {
    _ = raw.Close()
    log.Fatal(err)
}
defer tlsConn.Close()
```

This path retains the full Go TLS surface (SNI matching, verify callbacks,
ALPN, session tickets, `KeyLogWriter`) and is integration-tested.

### 6.2 Native VCL TLS

VPP's session layer can terminate TLS itself using its OpenSSL engine
(`VPPCOM_PROTO_TLS`). vclnet exposes this via `DialTLS` and `ListenTLS`:

```go
cfg := &vclnet.TLSConfig{
    Cert: certPEM, // PEM (leaf + optional chain)
    Key:  keyPEM,  // PEM matching key
}

listener, err := vclnet.ListenTLS("tcp4", "10.0.0.1:443", cfg)
if err != nil {
    log.Fatal(err)
}
defer listener.Close()

conn, err := vclnet.DialTLS("tcp4", "10.0.0.1:443", &vclnet.TLSConfig{})
if err != nil {
    log.Fatal(err)
}
defer conn.Close()
```

The returned values are `net.Listener` and `net.Conn`; reads and writes hand
plaintext bytes to and from the application. TLS records are produced and
consumed entirely inside VPP.

Requirements and current caveats:

- `vcl.conf` must select an in-tree TLS engine (`tls-engine 1` picks
  OpenSSL). The engine and its version are outside vclnet's control.
- Server configs must supply both `Cert` and `Key`. Client configs may leave
  them empty to use VPP's anonymous ckpair (`ckpair_index = 0`).
- Cert/key PEM bytes are registered once per unique pair (SHA-256 keyed
  `sync.Once` cache) via `vppcom_add_cert_key_pair`.
- The current mapping only sets `VPPCOM_ATTR_SET_CKPAIR`. SNI matching,
  ALPN, verify hooks, session ticket lifetimes, and TLS key logging are not
  yet exposed and remain reasons to prefer the layered path when those
  features are needed. See
  [../docs/vclnet_deep_dive.md §12.5](vclnet_deep_dive.md#125-native-vcl-tls-vppcom_proto_tls).

## 7. UDP

Connected UDP is implemented and tested in both modes:

```go
conn, err := vclnet.Dial("udp4", "10.0.0.1:53")
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

if _, err := conn.Write(query); err != nil {
    log.Fatal(err)
}
n, err := conn.Read(response)
```

Connected UDP supports IPv4, IPv6, context-aware connection setup, and
resettable deadlines in both modes. In Mode 2, connects block on the worker
thread until fully established to avoid a VPP half-open cleanup race.

`ListenPacket` returns a `net.PacketConn` backed by a per-peer session adapter.
VPP creates a separate VLS session for each peer that contacts the listener.
The adapter accepts these sessions in a background loop and fans their data
into `ReadFrom`. `WriteTo` routes to the peer's session if one exists;
otherwise it returns `vclnet.ErrUnknownPeer`. This semantic differs from
kernel UDP, which can `sendto` arbitrary addresses. For sending to new
addresses, use connected UDP via `Dial("udp", addr)`. Both modes.

## 8. Errors

Public failures use `*net.OpError` and preserve wrapped context errors or
`syscall.Errno` values.

```go
conn, err := vclnet.Dial("tcp4", "10.0.0.1:80")
if err != nil {
    switch {
    case errors.Is(err, context.DeadlineExceeded):
        log.Print("context deadline")
    case vclnet.IsTimeout(err):
        log.Print("network timeout")
    case vclnet.IsConnectionRefused(err):
        log.Print("connection refused")
    case vclnet.IsConnectionReset(err):
        log.Print("connection reset")
    }
}
_ = conn
```

VLS handles are not kernel file descriptors. Code that requires `SyscallConn`,
`File`, `splice`, or direct fd access is not compatible.

## 9. VPP workers and VLS modes

VPP-side session workers and application-side VLS workers are separate choices.
A VPP process may use several packet and session workers:

```text
cpu { workers 4 }
session { enable use-app-socket-api }
```

Mode 3 is the default and needs no application change. All application threads
share one VCL worker, so VLS state remains serialized even when VPP has several
workers.

Mode 2 creates a fixed application worker pool:

```text
vcl {
  multi-thread-workers
  # normal FIFO, scope, eventfd, and app-socket settings
}
```

```go
err := vclnet.InitWithOptions("my-service", vclnet.Options{Workers: 4})
```

Each worker is a lifetime-pinned goroutine with its own VCL worker, message
queue, epoll handle, operation queue, and waiter map. Session creation is
round-robin and every later operation is routed to the owner. Listeners use
per-worker sharding: each `Listen` or `ListenTLS` call creates one VLS
listener per worker on the same address:port (`SO_REUSEPORT`), runs a
per-worker accept loop, and fans accepted connections into a shared channel.
Both TCP and UDP sessions are supported. Mode 2 UDP connects block until fully
established to work around a VPP half-open cleanup race.

The ownership model above is implemented, but the sharded accept path is not
currently stable. Under concurrent load, VCL can receive `ACCEPTED` before the
dependent FIFO segment is mapped, then fault in its negative-reply path. The
latest four-worker Mode 2 suite reproduces it. Keep production services on
Mode 3 until [the P0 investigation](mode2_accept_mq_investigation.md) is
resolved.

Validate both paths with:

```bash
sudo -E bash test/run_multiworker.sh --mode 3 4
sudo -E bash test/run_multiworker.sh --mode 2 4
```

As of 2026-07-21, the Mode 3 command passes and the Mode 2 command fails under
concurrent HTTP accept load. Mode 2 is experimental and opt-in until the
accept/MQ fix, CI soak, and performance rollout gates in
[../summary.md](../summary.md#3-pending-work) are complete.

## 10. Containers and deployment

At runtime, a container needs:

- `libvppcom.so` and its dependent VPP libraries;
- the VCL config file;
- the VPP app socket mounted at the configured path;
- access to VPP's shared-memory resources;
- compatible VPP userspace libraries and process ABI.

Use normal environment syntax rather than putting an assignment in exec-form
`CMD`:

```dockerfile
ENV VCL_CONFIG=/etc/vpp/vcl.conf
CMD ["/service"]
```

Build-time discovery uses the `pkg-config` file rendered from
`pkgconfig/vppcom.pc.in`. Multi-stage Dockerfiles typically do this once in a
builder stage:

```dockerfile
FROM golang:1.26 AS build
COPY --from=vpp /opt/vpp /opt/vpp
COPY . /src
WORKDIR /src
RUN make pc VPP_PREFIX=/opt/vpp \
 && PKG_CONFIG_PATH=/src/pkgconfig go build -o /out/service ./cmd/service

FROM debian:bookworm-slim
COPY --from=vpp /opt/vpp/lib /opt/vpp/lib
COPY --from=build /out/service /usr/local/bin/service
ENV LD_LIBRARY_PATH=/opt/vpp/lib/x86_64-linux-gnu \
    VCL_CONFIG=/etc/vpp/vcl.conf
CMD ["service"]
```

Because the rendered `.pc` file embeds `-Wl,-rpath,${libdir}`, the runtime
loader can find `libvppcom.so` at the same absolute path used during the build
even without `LD_LIBRARY_PATH`. Set `LD_LIBRARY_PATH` explicitly if the runtime
image places the VPP libraries elsewhere.

## 11. Adoption checklist

Before rollout:

1. Run `go test -race -count=1 ./...`, `go vet ./...`, and `make build`.
2. Run `sudo -E bash test/run_integration.sh` against the exact VPP build.
3. Run `sudo -E bash test/run_multiworker.sh --mode 3 N` and
   `sudo -E bash test/run_multiworker.sh --mode 2 N` with the production worker
   count. Do not ship Mode 2 unless the complete command is green repeatedly;
   the current known result is a P0 accept failure.
4. Add application-specific protocol, load, timeout, and shutdown tests.
5. Confirm the workload does not require fd extraction or extended native
   TLS controls (SNI, ALPN, verify hooks). TCP, connected UDP (both modes),
   unconnected UDP (`ListenPacket`, both modes, known-peer `WriteTo` only),
   HTTP/1.1, HTTP/2, gRPC, native VCL TLS (`DialTLS` / `ListenTLS`),
   layered `crypto/tls`, and TCP half-close (`CloseRead` / `CloseWrite`)
   are all supported and integration-tested.
6. Measure performance on the real topology; do not reuse illustrative latency
   numbers from unrelated VPP deployments.
7. Verify `vls_unregister_vcl_worker` is exported, and pin both the VPP version
   and the downstream patch or upstream commit that supplies it.
8. Pin and document the Go and vclnet versions used for release.
