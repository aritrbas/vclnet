# vclnet

vclnet is a CGo wrapper around VPP's VCL/VLS API. It provides Go
`net.Listener`, `net.Conn`, and (provisionally) `net.PacketConn`
implementations whose data path uses VPP instead of the kernel network stack.

The package replaces the earlier Frida syscall-interception prototypes. The
architecture and migration rationale are documented in
[docs/architecture.md](docs/architecture.md), with VPP internals in
[docs/vclnet_deep_dive.md](docs/vclnet_deep_dive.md). The source-audited
analysis of VLS pthread ownership, Go goroutine memory, and the limits of a
Frida goroutine shim is in
[docs/frida_goroutine_tracking_analysis.md](docs/frida_goroutine_tracking_analysis.md).

## Current status

The TCP path is implemented and passes the local VPP integration harness on
IPv4 and IPv6 in both VLS modes. Connected UDP is integrated in the default
Mode 3 path. HTTP/1.1, layered `crypto/tls`, context cancellation, live I/O
deadlines, Happy Eyeballs, concurrent I/O,
shutdown, and VPP configurations with multiple worker threads are covered.

This repository should still be treated as pre-production infrastructure:

- `ListenPacket` implements a per-peer session adapter over VPP's
  session-oriented UDP. `ReadFrom` works for any peer; `WriteTo` only reaches
  peers that have already sent data (VPP cannot originate a session to an
  arbitrary address from a listener). For sending to new addresses, use
  connected UDP via `Dial("udp", addr)`. Mode 3 only.
- VLS Mode 3 remains the default compatibility path. Opt-in Mode 2 now uses N
  session-affine, lifetime-pinned workers with per-worker epoll and no shared
  poller. It requires `multi-thread-workers` and remains behind rollout, soak,
  and performance gates.
- Mode 2 currently admits TCP sessions only. With the pinned VPP 26.10 build,
  connected UDP can crash VPP during cut-through cleanup, so Mode 2 rejects
  every UDP entry point with an error wrapping `EOPNOTSUPP` before allocating
  VLS state. Use Mode 3 for UDP.
- Mode 2 listeners use per-worker sharding: one VLS listener per worker on the
  same address:port (`SO_REUSEPORT`), with per-worker accept loops fanning into
  a shared channel. This distributes accept load across all workers without
  cross-worker VLS access.
- Benchmarks exist, but the repository does not contain a reproducible
  kernel-vs-VPP baseline. Treat performance claims as hypotheses until measured
  on the target hardware and topology.

The canonical, prioritized pending-work list is in
[summary.md](summary.md#3-pending-work).

## Supported behavior

| API / network | Status |
| --- | --- |
| `Listen("tcp", "tcp4", "tcp6", ...)` | Integrated on IPv4 and IPv6 |
| `Dial`, `DialContext`, `DialTimeout` for TCP | Integrated; `"tcp"` uses staggered dual-stack attempts |
| TCP reads, writes, close, addresses, and resettable deadlines | Integrated |
| `ListenContext` / `TCPListener.AcceptContext` | Integrated |
| Connected UDP via `Dial("udp", "udp4", "udp6", ...)` | Integrated on IPv4 and IPv6 in Mode 3; Mode 2 returns `EOPNOTSUPP` |
| Unconnected UDP via `ListenPacket` | Per-peer session adapter in Mode 3; `ReadFrom` for any peer, `WriteTo` to known peers only |
| `Transport` / `NewHTTPClient` | HTTP/1.1 integration covered |
| `crypto/tls` layered over a vclnet TCP connection | Integrated |
| Native VCL TLS (`VPPCOM_PROTO_TLS`) via `DialTLS` / `ListenTLS` | Integrated; VPP terminates TLS, no `crypto/tls` on the caller side |
| TCP `CloseRead` / `CloseWrite` | Integrated via `vls_shutdown` (SHUT_RD is local-only EOF; SHUT_WR sends a peer FIN over full TCP; no-op over cut-through transport) |

DNS resolution uses Go's normal resolver. Only the connection data path is
routed through VPP.

## Quick start

```go
package main

import (
    "log"
    "net/http"

    "github.com/aritrbas/vclnet"
)

func main() {
    if err := vclnet.Init("my-service"); err != nil {
        log.Fatal(err)
    }
    vclnet.InstallSignalHandler()

    ln, err := vclnet.Listen("tcp", ":8080")
    if err != nil {
        log.Fatal(err)
    }
    defer ln.Close()

    err = http.Serve(ln, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
        _, _ = w.Write([]byte("hello from VPP\n"))
    }))
    log.Print(err)
}
```

Mode 2 opt-in:

```go
if err := vclnet.InitWithOptions("my-service", vclnet.Options{Workers: 4}); err != nil {
    log.Fatal(err)
}
```

The matching VCL config must contain `multi-thread-workers`. `Init` without
options keeps Mode 3 compatibility.

Context-aware dialing:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

conn, err := vclnet.DialContext(ctx, "tcp", "backend.example:8080")
```

Connected UDP:

```go
conn, err := vclnet.Dial("udp4", "10.0.0.1:53")
if err != nil {
    // handle error
}
defer conn.Close()
_, _ = conn.Write(query)
n, err := conn.Read(response)
_ = n
_ = err
```

Connected UDP currently requires Mode 3.

Unconnected UDP (per-peer session adapter):

```go
pc, err := vclnet.ListenPacket("udp4", ":9000")
if err != nil {
    // handle error
}
defer pc.Close()

buf := make([]byte, 65536)
n, addr, err := pc.ReadFrom(buf)
if err != nil {
    // handle error
}
// Reply to the peer that sent data (WriteTo only works for known peers).
_, err = pc.WriteTo(buf[:n], addr)
_ = err
```

`WriteTo` to an address that has not sent data returns `vclnet.ErrUnknownPeer`.
VPP's UDP model is session-based — each peer gets its own internal session only
after it contacts this listener. For sending to arbitrary addresses, use
connected UDP via `Dial`.

TCP half-close:

```go
conn, err := vclnet.Dial("tcp", "backend.example:8080")
if err != nil {
    // handle error
}
defer conn.Close()

if _, err := conn.Write(request); err != nil {
    // handle error
}

// Signal end-of-request over the wire. The peer's Read observes EOF; the
// local read half stays open so the response can still be received.
if hc, ok := conn.(interface{ CloseWrite() error }); ok {
    if err := hc.CloseWrite(); err != nil {
        // handle error
    }
}

body, err := io.ReadAll(conn)
_ = body
_ = err
```

`CloseRead` / `CloseWrite` are discovered via anonymous interfaces just as with
`net.TCPConn`, so idioms in `net/http`, `crypto/tls`, and third-party code
work unchanged. `CloseWrite` returns `net.ErrClosed` on subsequent `Write`
calls; `CloseRead` returns `io.EOF` (bare) on subsequent `Read` calls.

Native VCL TLS (`VPPCOM_PROTO_TLS`) — VPP terminates TLS inside the session
layer using its own crypto engine (OpenSSL by default). The `net.Conn`
returned by `DialTLS`/`ListenTLS` already speaks cleartext application data;
do **not** wrap it in `crypto/tls`.

```go
ln, err := vclnet.ListenTLS("tcp4", ":8443", &vclnet.TLSConfig{
    Cert: certPEM, // PEM-encoded leaf (+ optional chain)
    Key:  keyPEM,  // PEM-encoded matching private key
})
if err != nil {
    // handle error
}
defer ln.Close()

for {
    conn, err := ln.Accept()
    if err != nil {
        // handle error
    }
    go handle(conn) // conn already carries decrypted application data
}
```

Client side:

```go
conn, err := vclnet.DialTLS("tcp4", "backend.example:8443", nil) // anonymous client
if err != nil {
    // handle error
}
defer conn.Close()
_, _ = conn.Write([]byte("GET / HTTP/1.0\r\n\r\n"))
```

Trade-offs vs layered `crypto/tls`:

- Native VCL TLS eliminates the crypto/tls ↔ vclnet copy per record and
  keeps ciphertext inside VPP's SVM FIFOs.
- Layered `crypto/tls` still works over a plain `vclnet.Dial` / `Listen` and
  gives you the full Go TLS surface: SNI matching, cert verification hooks,
  session tickets, key logging, ALPN, etc. Use it whenever you need
  behaviour that VPP's ext-config surface does not yet expose.
- The `TLSConfig` in vclnet intentionally exposes only the minimum needed
  by `vppcom_add_cert_key_pair` + `VPPCOM_ATTR_SET_CKPAIR`. Richer
  configuration (SNI matching, verify callbacks, ALPN) is on the
  pending-work list and would require plumbing `VPPCOM_ATTR_SET_ENDPT_EXT_CFG`.

## Public API

```go
type Options struct {
    Workers int
}
func Init(appName string) error
func InitWithOptions(appName string, opts Options) error
func Listen(network, address string) (net.Listener, error)
func ListenContext(network, address string) (*TCPListener, error)
func ListenPacket(network, address string) (net.PacketConn, error)
func Dial(network, address string) (net.Conn, error)
func DialContext(ctx context.Context, network, address string) (net.Conn, error)
func DialTimeout(network, address string, timeout time.Duration) (net.Conn, error)

type Dialer struct {
    Timeout       time.Duration
    FallbackDelay time.Duration
}
func (d *Dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error)

type TCPListener struct { /* unexported fields */ }
func (l *TCPListener) Accept() (net.Conn, error)
func (l *TCPListener) AcceptContext(ctx context.Context) (net.Conn, error)
func (l *TCPListener) Close() error
func (l *TCPListener) Addr() net.Addr

func Transport(d *Dialer) *http.Transport
func NewHTTPClient() *http.Client
var DefaultTransport *http.Transport

type TLSConfig struct {
    Cert []byte // PEM-encoded certificate (chain)
    Key  []byte // PEM-encoded matching private key
}
func DialTLS(network, address string, cfg *TLSConfig) (net.Conn, error)
func DialTLSContext(ctx context.Context, network, address string, cfg *TLSConfig) (net.Conn, error)
func ListenTLS(network, address string, cfg *TLSConfig) (net.Listener, error)
var ErrTLSMissingCert error
var ErrTLSPartialCert error
var ErrUnknownPeer error // WriteTo to address not seen via ReadFrom

func Shutdown()
func ShutdownWithTimeout(drainTimeout time.Duration)
func ShutdownDone() <-chan struct{}
func InstallSignalHandler()

func IsTimeout(err error) bool
func IsConnectionRefused(err error) bool
func IsConnectionReset(err error) bool
```

`Shutdown` is idempotent. It closes every tracked listener first (stopping
new accepts), waits up to a bounded drain window (5 s by default) for
tracked connections, PacketConns, and in-flight dials to finish naturally,
then force-closes any stragglers so blocked reads/writes unpark with
`ErrClosed`. Use `ShutdownWithTimeout(d)` for an explicit drain window;
zero waits indefinitely, negative skips the drain entirely. Applications
should still stop admitting new work at the application layer (drain HTTP
handlers, refuse new RPCs) before calling Shutdown.

## Build and runtime requirements

The module declares Go 1.26 or newer. This workspace was validated with Go
1.26.1 and a VPP 26.10 development build.

### VPP discovery via pkg-config

`internal/vclpoll/cgo.go` links against `libvppcom` with `#cgo pkg-config: vppcom`.
VPP does not currently ship a `.pc` file, so this repository provides one:
`pkgconfig/vppcom.pc.in` is a template rendered on demand by `make pc`.

For a typical build, tell the Makefile where VPP is installed:

```bash
make pc VPP_PREFIX=/opt/vpp
make build            # bin/echo_server, bin/http_server, …
make unit             # `go test` on the module and internal/vclpoll
```

`make build`, `make unit`, `make test`, `make race`, and `make vet` all
auto-run `make pc` first. If you also want `go build` / `go test` invoked
directly to pick up the pkg-config file, export `PKG_CONFIG_PATH`:

```bash
export PKG_CONFIG_PATH="$PWD/pkgconfig:$PKG_CONFIG_PATH"
go build ./...
```

Advanced overrides:

| Variable         | Meaning                                          | Default                                          |
| ---------------- | ------------------------------------------------ | ------------------------------------------------ |
| `VPP_PREFIX`     | Install root that contains `include/` and libs   | (required unless the other three are set)        |
| `VPP_INCLUDEDIR` | Directory holding `vcl/vppcom.h`                 | `$VPP_PREFIX/include`                            |
| `VPP_LIBDIR`     | Directory holding `libvppcom.so`                 | `$VPP_PREFIX/lib/$(dpkg-architecture …)` or `lib`|
| `VPP_VERSION`    | Free-form version string                         | `0.0.0`                                          |
| `VCLNET_SKIP_PC` | Set to `1` if you already installed `vppcom.pc` system-wide | unset                                     |

### VPP runtime configuration

VPP must run with:

```text
session { enable use-app-socket-api }
```

A typical VCL config is:

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

Set `VCL_CONFIG` to that file before starting the application. This selects
Mode 3, which is the default.

To opt into Mode 2, add `multi-thread-workers` inside the `vcl` block and
select more than one worker at initialization:

```text
vcl {
  multi-thread-workers
  # the remaining settings are the same as above
}
```

```go
err := vclnet.InitWithOptions("my-service", vclnet.Options{Workers: 4})
```

CI and deployments can override selection without a code change by setting
`VCLNET_VLS_MODE=2` and `VCLNET_WORKERS=4`. `VCLNET_VLS_MODE=3` forces the
default dispatcher even when a larger worker count is configured. Mode 2
requires the VCL token; initialization fails clearly when it is missing.
Mode 2 is currently TCP-only; its UDP APIs fail before session creation with an
error wrapping `syscall.EOPNOTSUPP`.

## Tests

No-VPP validation (still needs `pkg-config` to find `vppcom.pc` because CGo
resolves link flags at compile time — the tests self-skip if VPP is not
actually running):

```bash
make pc VPP_PREFIX=/opt/vpp   # once; git-ignored output
make unit                     # go test on the module + internal/vclpoll
make race                     # go test -race
make vet
make build                    # every example
```

If you invoke `go test` / `go build` directly, either export
`PKG_CONFIG_PATH="$PWD/pkgconfig:$PKG_CONFIG_PATH"` or install a system-wide
`vppcom.pc`.

The integration files skip when `VCL_CONFIG` is absent. The dedicated
harness starts and stops an isolated VPP instance:

```bash
sudo -E bash test/run_integration.sh
sudo -E bash test/run_multiworker.sh --mode 3 4
sudo -E bash test/run_multiworker.sh --mode 2 4
```

`sudo -E` preserves `VPP_PREFIX`, `RUN_AS_USER`, and other overrides consumed
by `test/env.sh` — see that file for the full list.

Current top-level coverage consists of:

- 173 no-VPP tests across the public package, lifecycle registry, Mode 3
  poller, Mode 2 workers, and sharded listeners;
- 34 runnable public-package single-worker integration tests (including a
  concurrent-Shutdown stress test), plus one deliberately skipped test
  (half-close over cut-through);
- 2 low-level VCL poll integration tests;
- 5 multi-worker stress tests, 1 sharded-accept scaling test, plus 2 Mode 2
  invariants for ownership and safe UDP rejection;
- 2 opt-in benchmarks.

Use `go test -list .` and the test source as the source of truth; counts may
change as coverage grows.

The integration suite covers TCP in both modes, Mode 3 connected UDP on
IPv4/IPv6, unconnected UDP PacketConn echo, deadline expiry and reset, deadline
updates during a blocked read, close unblocking, concurrent blocked read/write
on a payload larger than the FIFO, HTTP, layered TLS, Happy Eyeballs,
context-aware accept, shutdown, address reporting, and multi-worker stress.

## Repository layout

```text
.
|-- *.go                         public package implementation
|-- vclnet_test.go               no-VPP contract tests
|-- integration_test.go          VPP integration tests and benchmarks
|-- internal/vclpoll/
|   |-- cgo.go                   VLS CGo bridge (links via pkg-config)
|   |-- dispatch.go              stable Mode 2 and Mode 3 dispatcher boundary
|   |-- poller.go                Mode 3 shared readiness poller
|   |-- mode2.go                 ownership and session-affine routing
|   |-- worker.go                pinned Mode 2 worker and per-worker epoll
|   |-- shard_listener.go       per-worker listener sharding and accept fan-in
|   `-- *_test.go                helper, worker, and VPP integration tests
|-- pkgconfig/vppcom.pc.in       template for VPP discovery (see Build)
|-- examples/                    echo, HTTP, and concurrency examples
|-- test/env.sh                  shared VPP path / user detection helper
|-- test/run_*.sh                VPP test/demo runners
|-- docs/architecture.md         design and Frida migration rationale
|-- docs/vclnet_deep_dive.md     VPP/VCL/VLS internals
|-- docs/frida_goroutine_tracking_analysis.md
|                                Frida, goroutine, and VLS memory analysis
|-- docs/adoption_guide.md       application integration guide
|-- docs/executive_report.md     decision-maker summary
`-- summary.md                  current status and canonical pending work
```

## Important limitations

- A VCL app cannot connect to its own listener in these tests. Client and
  server run as separate processes.
- TCP `CloseWrite` (half-close) does not deliver peer EOF over VPP's
  cut-through transport. When both endpoints attach to the same VPP with
  `app-scope-local`, `transport_half_close` is a no-op for the CT protocol.
  Half-close works correctly over the full TCP transport.
- Mode 3 remains the default. Mode 2 is opt-in, requires
  `multi-thread-workers`, and permanently pins one OS thread per worker.
- Listener port zero is rejected because the validated VCL build does not assign
  an ephemeral port.
- The test scripts target the local VPP source/build layout and use Linux VLS
  epoll constants.
- VPP debug builds have exhibited a cut-through cleanup race under overlapping
  sessions; validation uses the release build.
- VLS handles are not kernel file descriptors and cannot be converted to
  `*os.File`.
