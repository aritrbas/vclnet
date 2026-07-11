# vclnet architecture

Status: TCP is implemented and validated in both VLS modes. Connected UDP is
implemented and validated in Mode 3; Mode 2 rejects it before VLS allocation
because of a pinned VPP cleanup crash. See [../summary.md](../summary.md) for
the canonical limitations and pending work.

For VPP session/FIFO internals, see
[vclnet_deep_dive.md](vclnet_deep_dive.md).

## 1. Why this library exists

VPP normally offers transparent socket redirection to C applications through
an LD_PRELOAD shim. Go's runtime generally issues network syscalls directly and
owns readiness through its kernel epoll integration, so libc interception does
not provide a reliable path.

A VCL Locked Session handle is not a kernel file descriptor. Returning one to
Go's `netFD` or `os.File` paths causes kernel operations to fail, usually
with `EBADF`.

VCL also stores worker and lock state in pthread-local storage, while Go
goroutines move among OS threads. Direct calls therefore need an explicit
thread-affinity boundary.

vclnet solves both problems at the Go interface layer:

- it implements `net.Conn`, `net.Listener`, and selected
  `net.PacketConn` behavior;
- it keeps VLS handles internal;
- it selects either the compatibility Mode 3 dispatcher or an opt-in Mode 2
  session-affine worker pool;
- it keeps every VLS call on a valid pthread boundary;
- it provides VLS readiness loops instead of involving `runtime.netpoll`.

## 2. Earlier interception prototypes

Two Frida-based prototypes preceded this package.

The per-function prototype intercepted Go syscall wrappers and sent them
through LDP. The single-entry prototype intercepted a common syscall
trampoline and called VLS directly. They demonstrated reachability, but their
architecture did not scale safely:

| Problem | Interception consequence | vclnet approach |
| --- | --- | --- |
| VCL handle is not a kernel fd | Fake descriptors leak to unhooked paths | Handle stays inside `internal/vclpoll` |
| Go owns kernel readiness | EAGAIN cannot be handed to normal netpoll | Separate VLS epoll poller |
| VCL state is per pthread | Hooks run on scheduler-selected threads | `runtime.LockOSThread` per VLS call |
| JavaScript callback path | Operations serialize and blocking stalls hooks | Native Go/CGo calls |
| Go ABI and symbol drift | Hooks require release-specific maintenance | Supported CGo ABI |
| Error construction | Hooks synthesize Go ABI values | Normal Go errors and `*net.OpError` |

The explicit `net.Conn` boundary is the production engineering direction.
This statement is about maintainability and correctness, not a claim that the
repository has completed every production-hardening item.

## 3. Layers

```text
+---------------------------------------------------------------+
| Go application                                                |
| net/http or code using net.Listener / net.Conn                |
+-------------------------------+-------------------------------+
                                |
+-------------------------------v-------------------------------+
| package vclnet                                                |
|                                                               |
| vclnet.go       Init, Listen, ListenPacket, Dial wrappers      |
| dialer.go       context dialing and Happy Eyeballs             |
| listener.go     listener and AcceptContext                     |
| conn.go         TCP Conn and resettable deadlines              |
| udpconn.go      connected UDP net.Conn                         |
| packetconn.go   per-peer session adapter for net.PacketConn    |
| transport.go    HTTP transport/client                          |
| shutdown.go     package lifecycle and graceful drain           |
| lifecycle.go    live listener/conn/PacketConn/dial registry    |
| addr/errors.go  resolver and net.OpError adaptation            |
+-------------------------------+-------------------------------+
                                |
+-------------------------------v-------------------------------+
| internal/vclpoll                                              |
|                                                               |
| dispatch.go          stable API and selected threading dispatcher   |
| cgo.go               VLS bridge and one-shot helpers                |
| poller.go            Mode 3 shared VLS epoll owner                  |
| mode2.go             virtual handles and session ownership routing  |
| worker.go            pinned Mode 2 workers and per-worker epoll     |
| shard_listener.go    per-worker listener sharding and accept fan-in |
+-------------------------------+-------------------------------+
                                |
+-------------------------------v-------------------------------+
| libvppcom.so                                                  |
+-------------------------------+-------------------------------+
                                |
                    shared memory and message queues
                                |
+-------------------------------v-------------------------------+
| VPP session layer and transports                              |
+---------------------------------------------------------------+
```

## 4. Public behavior

Implemented and integrated:

- TCP IPv4 and IPv6 listen, accept, connect, read, write, close;
- connected UDP IPv4 and IPv6 in Mode 3;
- unconnected UDP (`ListenPacket`) with a per-peer session adapter in Mode 3;
- context-aware TCP connection setup in both modes and UDP setup in Mode 3;
- dual-stack Happy Eyeballs for `"tcp"`;
- resettable read/write deadlines that affect blocked operations;
- context-aware accept;
- HTTP/1.1, HTTP/2 (cleartext prior-knowledge via `UnencryptedHTTP2` and TLS-with-ALPN), and layered `crypto/tls`;
- gRPC unary and server-streaming RPCs over both cleartext and TLS transports;
- native VCL TLS (`DialTLS` / `ListenTLS`) via `VPPCOM_PROTO_TLS` with
  `vppcom_add_cert_key_pair` + `SET_CKPAIR`, sharing the same
  `net.Conn`/`net.Listener` surface as the plain TCP path;
- shutdown that wakes dispatcher-backed operations and rejects new work;
- TCP half-close (`CloseRead` / `CloseWrite`) via `vls_shutdown`.

Provisional or absent:

- Mode 2 UDP, which returns an error wrapping `EOPNOTSUPP`;
- extended native TLS controls (SNI, ALPN, verify hooks via
  `SET_ENDPT_EXT_CFG`);
- fd extraction;

`ListenPacket` returns a `net.PacketConn` backed by a per-peer session adapter.
VPP's UDP model creates a separate VLS session for each peer that contacts the
listener. The adapter accepts these sessions in a background loop and fans
their data into `ReadFrom`. `WriteTo` routes to the peer's session if one
exists; otherwise it returns `ErrUnknownPeer`. This semantic difference from
kernel UDP (which can `sendto` arbitrary addresses) is inherent to VPP's
session layer.

## 5. Threading boundaries

The dispatcher is selected once during `Init` or `InitWithOptions`. Public and
internal call sites keep using the same package functions; the dispatcher
decides which OS thread may enter VLS.

### Mode 3

Every immediate CGo operation follows the compatibility pattern:

```text
goroutine
  |
  +-- runtime.LockOSThread
  +-- find pthread_self in worker registry
  |     `-- first use: vls_register_vcl_worker
  +-- call vls_*
  `-- runtime.UnlockOSThread
```

The lock is held for one immediate VLS operation, not during a readiness wait.
All registered threads share VCL worker 0 because the VCL config omits
`multi-thread-workers`.

### Mode 2

`InitWithOptions` creates N goroutines, locks each to its OS thread for its
whole lifetime, creates VCL worker 0 on the bootstrap worker, and registers the
remaining VCL workers sequentially. Each loop owns one VLS epoll handle and an
operation channel.

Session creation is round-robin. Every later I/O, attribute, readiness, and
close operation is submitted to the worker that created or accepted the
session. Raw VLS handles are worker-local pool indexes and can collide, so the
dispatcher exposes process-unique internal handles mapped to `{worker, raw}`.
Before touching a raw session it checks `vlsh_to_session_and_worker_index`; a
mismatch is rejected and counted before VLS can migrate or clone the session.

Mode 2 requires `multi-thread-workers` in `vcl.conf`. The pinned VPP 26.10
library does not export `vls_use_workers_only`; the configuration token is the
supported switch and initialization verifies it with `vls_mt_wrk_supported`.

Only TCP sessions are admitted in Mode 2 on this build. Every Mode 2 UDP entry
point returns an error wrapping `EOPNOTSUPP` before a VLS session is allocated;
see the cleanup-race analysis in the deep-dive document.

#### Sharded listeners

Mode 2 listeners use per-worker sharding. `Listen` and `ListenTLS` create one
VLS listener per worker on the same address:port (via `SO_REUSEPORT` /
`VPPCOM_ATTR_SET_REUSEPORT`). Each worker runs its own accept loop against its
local listener handle, and accepted connections fan into a single buffered
channel. The public `AcceptContext` reads from this channel, so accept load
distributes across all workers without cross-worker VLS access.

```text
Worker 0: listen(addr) -> accept loop -> \
Worker 1: listen(addr) -> accept loop -> --> fan-in channel --> AcceptContext
Worker 2: listen(addr) -> accept loop -> /
Worker 3: listen(addr) -> accept loop -> /
```

This design ensures every VLS operation (listen, accept, and subsequent I/O on
accepted connections) stays on its owning worker's pinned OS thread. The
per-worker epoll drives readiness for both listener and data sessions on that
worker.

## 6. Non-blocking readiness

All sessions are non-blocking. An immediate VLS call either returns a result or
reports EAGAIN or EINPROGRESS. The calling goroutine then waits on a Go channel
and holds no OS thread while parked.

### Mode 3 shared poller

One permanently pinned goroutine owns a persistent VLS epoll handle. It stores
a wait set per session, registers the union event mask, wakes only matching
readers or writers, and treats error or hangup as terminal. Exact waiter
cancellation lets one deadline change without removing a concurrent operation.

### Mode 2 per-worker loops

Each owner loop combines bounded operation batches with a short
`vls_epoll_wait`. The epoll data key is the process-unique virtual handle, while
epoll control always receives the owner-local raw handle. The worker stores the
same union-mask and exact-waiter state used by Mode 3. Cancellation is submitted
back to the owner so it cannot race cross-worker state. A full operation batch
uses a zero epoll timeout, preserving readiness fairness without imposing a
10 ms sleep on a saturated queue.

```text
Read, Write, Accept, or Connect
  |
  +-- submit or execute immediate vls_* on the valid worker
  |      `-- ready: return result
  |
  `-- EAGAIN or EINPROGRESS
         +-- add exact waiter to the selected epoll loop
         +-- block caller on a Go channel
         `-- wake and retry on the same owner
```

### Deadline updates

Each connection direction has resettable deadline state. Setting, extending,
or clearing a deadline closes the old notification channel. A woken operation
checks the current value: it returns a timeout if expired or registers again
with the new channel if the deadline moved.

## 7. Connection setup

### TCP single address

```text
resolve address
  -> vls_create(non-blocking)
  -> vls_connect
       |-- immediate success
       `-- EINPROGRESS/EAGAIN
             -> selected readiness dispatcher (EPOLLOUT | EPOLLERR | EPOLLHUP)
                with context cancellation
             -> vppcom_session_get_error to distinguish success from a
                refused / unreachable / handshake-failed outcome
  -> context/shutdown check
  -> tcpConn
```

`VPPCOM_ATTR_GET_ERROR` remains a stub in the pinned VPP 26.10 build, but
`vppcom_session_get_error` (exposed as `vclpoll.SessionConnectError`)
inspects the session's `vpp_error` field populated by the
`SESSION_CTRL_EVT_CONNECTED` handler. `SESSION_E_REFUSED` maps to
`ECONNREFUSED`, `SESSION_E_PORTINUSE` to `EADDRINUSE`, and any other
non-zero session error to `EFAULT`. Dial paths wait on the union of the
success and error events so one waiter covers both outcomes; when the
waiter wakes for `EPOLLOUT`, the subsequent query rejects a stale event
by returning a wrapped errno instead of a working conn.

**Coverage gap on this VPP build:** empirically, a connect to an unused
loopback port with `app-scope-local` set does not deliver the
`SESSION_CTRL_EVT_CONNECTED`-with-error event to the app's epoll (VPP
increments the session `no route` counter but the postponed event is
dropped before generation). The client-side query is wired regardless, so
this only affects error-signalling latency, not correctness — a real-NIC
peer that RSTs will surface as `ECONNREFUSED`; loopback misses fall back
to context-timeout cancellation. Full investigation, VPP source
references, and reproduction steps are in
[connect_error_investigation.md](connect_error_investigation.md).

### Happy Eyeballs

For `"tcp"`, all resolver results are split by family and interleaved,
starting with IPv6. The first attempt starts immediately and later attempts are
staggered (250 ms by default) or accelerated after failure. The first success
wins. The child context cancels in-flight attempts, and any attempt that still
returns a successful connection is drained and closed.

### UDP

Mode 3 connected UDP uses the same split connect/poller cancellation pattern.

`ListenPacket` creates a bound UDP VLS listener and wraps it in a per-peer
session adapter (`packetConn`). VPP's server-side UDP is session-oriented: each
peer that contacts the listener gets its own VLS session (accepted like TCP).
The adapter runs a background accept loop, spawns a reader goroutine per peer,
and fans data into a shared channel for `ReadFrom`. `WriteTo` routes to the
peer's accepted session; unknown peers return `ErrUnknownPeer`.

Mode 2 UDP is deliberately unavailable on the pinned VPP 26.10 build because
closing a cut-through datagram session can leave VPP with a stale TX event.

## 8. Addressing

Supported network strings are `tcp`, `tcp4`, `tcp6`, `udp`,
`udp4`, and `udp6`.

- Numeric service names are resolved with the correct TCP or UDP service
  database.
- Literal family mismatches return `*net.AddrError`.
- Unsuffixed single-address resolution prefers IPv4.
- Happy Eyeballs retains all suitable addresses and interleaves families.
- DNS stays on `net.DefaultResolver`.
- Listener addresses are queried from VCL after fixed-port bind. This VCL build
  does not allocate an ephemeral listener for port zero, so vclnet rejects it.

IPv6-only listeners set the VCL V6-only attribute and treat a failure to set it
as listener setup failure.

## 9. Errors

Low-level negative VCL results become `*vclpoll.VCLError` containing a
`syscall.Errno`. Public functions wrap errors in `*net.OpError` and preserve
the unresolved target string when resolution/setup fails.

This supports:

```go
errors.Is(err, syscall.ECONNREFUSED)
errors.Is(err, syscall.ECONNRESET)
errors.Is(err, syscall.EOPNOTSUPP) // Mode 2 UDP
vclnet.IsTimeout(err)
```

Context cancellation and deadlines remain distinguishable from package or
listener close.

## 10. Shutdown

`Shutdown` is idempotent and process-final. It uses a package-level
`liveRegistry` (`lifecycle.go`) to track open listeners, connections,
PacketConns, and in-flight dials:

```text
mark public package closed
  -> close every tracked listener (stops admitting new work; wakes AcceptContext)
  -> wait up to the drain window (5 s default) for tracked conns/PacketConns/dials
       to finish naturally — waitDrain wakes on the last removal
  -> force-close remaining tracked conns and PacketConns so parked reads/writes
     unpark with ErrClosed
  -> prevent parked operations from re-entering VLS
  -> stop the active dispatcher and wake exact waiters
  -> destroy the VCL application after readiness workers stop
```

`ShutdownWithTimeout(d)` exposes the drain window explicitly. Zero waits
indefinitely; negative skips the drain and force-closes immediately.

Every public entry point (`Listen`, `ListenTLS`, `ListenPacket`,
`AcceptContext`, `Dial`, `DialTLSContext`) registers its result before
returning; each object's `Close` unregisters. In-flight dials are counted
separately so Shutdown does not race a connect that has completed the VLS
work but not yet handed the conn back to the caller.

Mode 3 stops its shared poller before app destruction. Mode 2 marks the pool
stopping, closes every worker stop channel, wakes waiters, closes sessions on
their owners, drains bounded VPP cleanup notifications, and waits for every
non-bootstrap OS thread to disappear before worker 0 destroys the app. The
bootstrap M is then parked because the pinned VPP pthread destructor is unsafe
after global VLS state is gone.

Applications should still stop admitting new work at the application layer
(drain HTTP handlers, refuse new RPCs) before calling Shutdown; the drain
window catches whatever is already in flight.

## 11. VLS modes and multi-worker VPP

Mode 3 remains the default. It shares VCL worker 0 across registered calling
threads and uses one readiness poller. VLS locks serialize application-side
state, which is compatible and broadly validated.

Mode 2 is implemented behind `Options{Workers: N}` or the environment
overrides `VCLNET_VLS_MODE=2` and `VCLNET_WORKERS=N`. It requires
`multi-thread-workers`, creates N permanently pinned event loops, and routes
every session operation to its owner. The shared Mode 3 poller is never started
in this path. This path currently admits TCP sessions only; UDP fails before
VLS allocation with an error wrapping `EOPNOTSUPP`.

Listeners use per-worker sharding: each `Listen` or `ListenTLS` call creates
one VLS listener per worker on the same address:port using `SO_REUSEPORT`,
runs a per-worker accept loop, and fans accepted connections into a shared
channel. This distributes accept load across all workers without cross-worker
VLS access.

`cpu { workers N }` configures VPP-side workers and is independent of VLS mode.
The multi-worker harness accepts `--mode 3` and `--mode 2` to validate both
application-side designs explicitly. Mode 2 remains opt-in until sustained CI
and performance gates pass.

## 12. Cut-through

When both applications attach to the same VPP with compatible namespace and
`app-scope-local`, VPP can select its cut-through transport and connect their
shared-memory FIFOs directly.

vclnet does not need a separate cut-through API; it follows VCL's normal
connect/listen path. The repository does not ship comparative benchmark data,
so this architecture document intentionally makes no numeric speedup claim.

## 13. Validation

```bash
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
make build

sudo -E bash test/run_integration.sh
sudo -E bash test/run_multiworker.sh --mode 3 4
sudo -E bash test/run_multiworker.sh --mode 2 4
```

The standard harness uses separate server subprocesses because the tested VCL
configuration cannot connect an app back to its own listener. It restarts VPP
before the low-level poll tests to isolate session state.

The Mode 2 multi-worker run includes five TCP stress tests, a sharded-accept
scaling test, ownership and safe-UDP-rejection invariants, followed by a VPP
liveness probe.

See [../summary.md](../summary.md#2-test-inventory) for current counts and
[../summary.md](../summary.md#3-pending-work) for open work.

## Appendix: current build binding

`internal/vclpoll/cgo.go` links against `libvppcom` through pkg-config
(`#cgo pkg-config: vppcom`). The repository ships
`pkgconfig/vppcom.pc.in`, and `make pc VPP_PREFIX=…` renders a local
`pkgconfig/vppcom.pc` that resolves `-I`, `-L`, `-lvppcom`, and the
`-Wl,-rpath,${libdir}` linker flag from the chosen install prefix.

Because the rpath is baked into the resulting binary, the runtime loader
still finds `libvppcom.so` at that absolute path without `LD_PRELOAD` or
`LD_LIBRARY_PATH`. Only the compile step needs pkg-config to see the file
(pass `PKG_CONFIG_PATH=$PWD/pkgconfig` or use the Makefile targets, which do
this automatically).
