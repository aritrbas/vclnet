# VCLNET adoption guide

This guide describes the behavior that is implemented and tested in this
repository. Read [../summary.md](../summary.md) for current limitations and the
canonical pending-work list before committing to production adoption.

## 1. Prerequisites

- Go 1.26 or newer.
- A VPP build with the session layer and app socket API enabled.
- VPP headers and `libvppcom.so` available at build time.
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

Mode 2 is currently TCP-only. Its UDP entry points return an error wrapping
`syscall.EOPNOTSUPP` before creating VLS state; select Mode 3 for UDP workloads.

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

The test suite covers HTTP/1.1 keep-alive-configured sequential requests. HTTP/2 and
current gRPC releases are pending explicit integration tests; do not infer
support solely from interface compatibility.

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

* `vcl.conf` must select an in-tree TLS engine (`tls-engine 1` picks
  OpenSSL). The engine and its version are outside vclnet's control.
* Server configs must supply both `Cert` and `Key`. Client configs may leave
  them empty to use VPP's anonymous ckpair (`ckpair_index = 0`).
* Cert/key PEM bytes are registered once per unique pair (SHA-256 keyed
  `sync.Once` cache) via `vppcom_add_cert_key_pair`.
* The current mapping only sets `VPPCOM_ATTR_SET_CKPAIR`. SNI matching,
  ALPN, verify hooks, session ticket lifetimes, and TLS key logging are not
  yet exposed and remain reasons to prefer the layered path when those
  features are needed. See
  [../docs/vclnet_deep_dive.md §12.5](vclnet_deep_dive.md#125-native-vcl-tls-vppcom_proto_tls).

## 7. UDP

Connected UDP is implemented and tested in Mode 3:

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

The Mode 3 connected UDP path supports IPv4, IPv6, context-aware connection
setup, and resettable deadlines. Mode 2 rejects UDP before VLS allocation with
an error wrapping `syscall.EOPNOTSUPP` because the pinned VPP 26.10 build can
crash during cut-through datagram cleanup.

Do not adopt `ListenPacket` for arbitrary peers yet. VPP represents incoming
UDP peers as sessions accepted from a UDP listener; vclnet does not yet provide
the per-peer adapter needed to make classic `PacketConn.ReadFrom` and
`WriteTo` semantics correct. The API remains provisional and its integration
test is intentionally skipped.

## 8. Errors

Public failures use `*net.OpError` and preserve wrapped context errors or
`syscall.Errno` values. Mode 2 UDP failures preserve
`errors.Is(err, syscall.EOPNOTSUPP)`.

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
round-robin and every later operation is routed to the owner. Mode 2 uses one
owner per listener in the current v1 design; listener sharding is pending.
Only TCP sessions enter this pool on the pinned VPP build. Use Mode 3 if the
application requires UDP.

Validate both paths with:

```bash
sudo -E bash test/run_multiworker.sh --mode 3 4
sudo -E bash test/run_multiworker.sh --mode 2 4
```

Mode 2 is experimental and opt-in until its CI soak and performance rollout
gates in [../summary.md](../summary.md#3-pending-work) are complete.

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
   count.
4. Add application-specific protocol, load, timeout, and shutdown tests.
5. Confirm the workload does not require unconnected UDP, Mode 2 UDP, fd
   extraction, extended native TLS controls (SNI, ALPN, verify hooks),
   HTTP/2, or untested gRPC behavior. Basic native VCL TLS
   (`DialTLS` / `ListenTLS`), layered `crypto/tls`, and TCP half-close
   (`CloseRead` / `CloseWrite`) are supported and follow `net.TCPConn`
   semantics.
6. Measure performance on the real topology; do not reuse illustrative latency
   numbers from unrelated VPP deployments.
7. Pin and document the VPP/Go/library versions used for release.
