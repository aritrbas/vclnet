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

Do not add `multi-thread-workers`: the current shared poller is designed for
VLS mode 3 and is not compatible with mode 2 session ownership.

## 2. Initialize once

Call `Init` before creating listeners or connections and handle its error.

```go
if err := vclnet.Init("my-service"); err != nil {
    log.Fatalf("initialize VCL: %v", err)
}
```

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
clearing, or extending a deadline wakes an operation already waiting in the
shared poller.

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

This layered `crypto/tls` path is integration-tested. Native VCL TLS
(`VPPCOM_PROTO_TLS`) is not exposed.

## 7. UDP

Connected UDP is implemented and tested:

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

The connected UDP path supports IPv4, IPv6, context-aware connection setup,
and resettable deadlines.

Do not adopt `ListenPacket` for arbitrary peers yet. VPP represents incoming
UDP peers as sessions accepted from a UDP listener; vclnet does not yet provide
the per-peer adapter needed to make classic `PacketConn.ReadFrom` and
`WriteTo` semantics correct. The API remains provisional and its integration
test is intentionally skipped.

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

## 9. Multi-worker VPP

A VPP process may use several packet/session worker threads:

```text
cpu { workers 4 }
session { enable use-app-socket-api }
```

No application change is required, and the repository has a multi-worker stress
suite. This does **not** make the VCL client parallel: vclnet still uses VLS
mode 3, where calls share one VCL worker and serialize on VLS locks.

The repository's `test/run_multiworker.sh` deliberately leaves
`multi-thread-workers` out of `vcl.conf`. Mode 2 requires a future
session-affine event-loop design.

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
2. Run `sudo bash test/run_integration.sh` against the exact VPP build.
3. Run `sudo bash test/run_multiworker.sh N` with the production worker count.
4. Add application-specific protocol, load, timeout, and shutdown tests.
5. Confirm the workload does not require unconnected UDP, fd extraction,
   half-close, native VCL TLS, HTTP/2, or untested gRPC behavior.
6. Measure performance on the real topology; do not reuse illustrative latency
   numbers from unrelated VPP deployments.
7. Pin and document the VPP/Go/library versions used for release.
