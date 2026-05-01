# vclnet architecture

Status: TCP and connected UDP are implemented and validated in the local VPP
harness. See [../summary.md](../summary.md) for the canonical limitations and
pending work.

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
- it pins the goroutine during each VLS call;
- it provides its own VLS readiness poller instead of involving
  `runtime.netpoll`.

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
| udpconn.go      connected UDP; provisional PacketConn surface  |
| transport.go    HTTP transport/client                          |
| shutdown.go     package lifecycle                              |
| addr/errors.go  resolver and net.OpError adaptation            |
+-------------------------------+-------------------------------+
                                |
+-------------------------------v-------------------------------+
| internal/vclpoll                                              |
|                                                               |
| cgo.go          VLS calls, thread pinning, address conversion  |
| poller.go       one persistent VLS epoll owner                 |
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
- connected UDP IPv4 and IPv6;
- context-aware TCP/UDP connection setup;
- dual-stack Happy Eyeballs for `"tcp"`;
- resettable read/write deadlines that affect blocked operations;
- context-aware accept;
- HTTP/1.1 and layered `crypto/tls`;
- shutdown that wakes poller-backed operations and rejects new work.

Provisional or absent:

- arbitrary-peer unconnected UDP;
- TCP half-close;
- native VCL TLS;
- fd extraction;
- HTTP/2 and gRPC validation.

Although the dynamic UDP type satisfies both `net.Conn` and
`net.PacketConn`, `ListenPacket` is not end-to-end supported until a
per-peer session adapter exists.

## 5. Thread-affinity boundary

Every CGo entry point that touches VLS follows this pattern:

```text
goroutine
  |
  +-- runtime.LockOSThread
  +-- find pthread_self in worker registry
  |     `-- first use: vls_register_vcl_worker
  +-- call vls_*
  `-- runtime.UnlockOSThread
```

The lock is held for one immediate VLS operation, not while the goroutine waits
for readiness. This preserves VLS pthread-local lock tracking without pinning
an OS thread for the whole blocked I/O interval.

`AppInit` is special: `vls_app_create` creates the initial worker on the
calling OS thread, and that thread id is recorded in the registry.

## 6. Shared readiness poller

All sessions are created non-blocking. VLS calls can return EAGAIN (or
EINPROGRESS during connect). A single goroutine, permanently pinned to an OS
thread, owns one VLS epoll handle.

```text
Read/Write/Accept
  |
  +-- immediate vls_* call
  |      `-- ready: return result
  |
  `-- EAGAIN
         +-- unlock OS thread
         +-- register waiter with shared poller
         +-- block on Go channel
         `-- wake, pin again, retry
```

The poller stores a wait set per VLS handle:

```text
VLSH 42
  interest registered in VLS epoll: EPOLLIN | EPOLLOUT
  waiters:
    - reader A: EPOLLIN
    - reader B: EPOLLIN
    - writer C: EPOLLOUT
```

When VLS reports an event, only matching waiters wake. EPOLLERR/HUP wakes every
waiter on that session. Cancelling a deadline removes the exact waiter pointer,
not the entire session registration, so a concurrent operation remains parked.

VLS epoll wait also drives the VCL message queue. The poller uses a 100 ms
maximum wait to keep control events moving even when no session event is
immediately ready.

### Deadline updates

Each connection direction has a resettable deadline state:

1. `SetReadDeadline` or `SetWriteDeadline` closes the old notification
   channel, waking operations using the old value.
2. A new channel/timer represents the new value.
3. A woken operation checks whether the current deadline expired.
4. If the deadline was extended or cleared, it registers again with the new
   channel; otherwise it returns a timeout.

This covers deadlines set after an operation has already blocked, not only
deadlines that were expired before the call.

## 7. Connection setup

### TCP single address

```text
resolve address
  -> vls_create(non-blocking)
  -> vls_connect
       |-- immediate success
       `-- EINPROGRESS/EAGAIN
             -> shared poller EPOLLOUT with context cancellation
  -> context/shutdown check
  -> tcpConn
```

VPP's session error attribute has historically been unreliable in the target
build, so the current path treats EPOLLOUT as completion. Reliable post-connect
error verification is a pending P1 item.

### Happy Eyeballs

For `"tcp"`, all resolver results are split by family and interleaved,
starting with IPv6. The first attempt starts immediately and later attempts are
staggered (250 ms by default) or accelerated after failure. The first success
wins. The child context cancels in-flight attempts, and any attempt that still
returns a successful connection is drained and closed.

### UDP

Connected UDP uses the same split connect/poller cancellation pattern. The
server-side VPP UDP model is session-oriented: a bound/listening UDP VLS handle
accepts per-peer sessions. vclnet does not yet translate that model into
arbitrary-peer `PacketConn` semantics.

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
vclnet.IsTimeout(err)
```

Context cancellation and deadlines remain distinguishable from package or
listener close.

## 10. Shutdown

`Shutdown` is idempotent:

```text
mark public package closed
  -> mark low-level application unavailable
  -> stop poller and wake its waiters
  -> destroy VCL app only if Init succeeded
```

A blocked accept is covered by integration testing. Existing connection
objects also see the package-closed state when woken. A full connection
registry and graceful drain policy remain pending; services should stop
admitting work before calling Shutdown.

## 11. VLS modes and multi-worker VPP

The current design uses VLS mode 3 (single-worker multi-thread). All calling
threads share the initial VCL worker, and VLS locks serialize its state. This
makes cross-thread poller access valid for the tested configuration.

`cpu { workers N }` configures VPP-side workers. The multi-worker integration
suite validates that topology, but application-side VLS remains mode 3.

With VCL `multi-thread-workers` (mode 2), sessions belong to the VCL worker
that created them. A poller on another thread would need migration, which is
not valid for all active session states. Mode 2 therefore needs fixed,
permanently pinned worker loops, each owning its sessions and epoll instance.

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

sudo bash test/run_integration.sh
sudo bash test/run_multiworker.sh 4
```

The standard harness uses separate server subprocesses because the tested VCL
configuration cannot connect an app back to its own listener. It restarts VPP
before the low-level poll tests to isolate session state.

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
