# VCLNET decision report

## Executive summary

VCLNET gives Go programs an explicit `net.Conn` / `net.Listener` interface
to VPP's VCL session layer. This avoids the fundamental limitation of the
earlier LD_PRELOAD/Frida experiments: Go's networking runtime issues kernel
syscalls directly and cannot treat a VCL session handle as a kernel file
descriptor.

The current repository demonstrates a viable architecture for TCP in both VLS
modes, connected UDP in the default Mode 3 path, and unconnected UDP via a
per-peer session adapter (`ListenPacket`). Its isolated VPP test harness covers
IPv4, IPv6, HTTP/1.1, layered TLS, native VCL TLS, context cancellation, live
deadlines, concurrent reads/writes, shutdown, PacketConn echo, and VPP
configured with multiple worker threads.

It is not yet a generally distributable production library. The highest
priority gaps are automated VPP compatibility CI and completion of Mode 2
rollout validation. Mode 3 remains the compatibility default. An opt-in Mode 2
implementation now uses fixed, session-affine worker loops with per-worker
sharded listeners for application-side parallelism, but its rollout still
requires sustained CI, soak testing, and a performance baseline. The pinned VPP
26.10 build crashes during Mode 2 cut-through UDP cleanup, so Mode 2 rejects
UDP before allocating VLS state and preserves
`errors.Is(err, syscall.EOPNOTSUPP)`.

The prioritized source of truth is
[../summary.md](../summary.md#3-pending-work).

## 1. Problem

VPP's transparent preload path is designed for programs that call networking
functions through libc. Go's standard network stack does not normally do that:
its runtime owns kernel file descriptors and readiness through its own epoll
integration.

A VCL session handle is an index into VCL state, not a kernel fd. Passing it to
Go's runtime poller, `os.File`, or normal syscalls produces incorrect behavior
such as `EBADF`. VCL also relies on per-pthread state, while Go schedules
goroutines across operating-system threads.

These are interface and threading mismatches, not configuration issues.

## 2. Approaches evaluated

### Frida syscall interception

The earlier prototypes intercepted Go runtime/syscall entry points and
redirected selected calls to LDP or VLS.

They established that basic echo/HTTP traffic could reach VPP, but the design
had structural problems:

- one JavaScript interception path serialized concurrent operations;
- fake fd values escaped into code expecting kernel descriptors;
- waits could not integrate safely with Go's runtime poller;
- hooks depended on Go symbols, ABI details, and syscall behavior;
- VCL's per-thread state did not align with goroutine migration;
- deployment required runtime injection and difficult mixed-stack debugging.

Those prototypes remain useful research evidence, not a production path.

### Explicit Go networking adapter

VCLNET implements the Go interfaces directly and calls VLS through CGo:

```text
Go application
    |
    | net.Conn / net.Listener
    v
vclnet
    |
    | CGo, pinned VLS calls, opaque handles
    v
libvppcom.so
    |
    | shared-memory FIFOs and message queues
    v
VPP
```

This puts the boundary at an interface both systems can support. Go never sees
a fake fd, and VLS calls execute while the goroutine is pinned to its current OS
thread.

## 3. Current evidence

The no-VPP suite has 165 top-level tests. The VPP-backed suites contain:

- 33 runnable public-package single-worker tests (including native VCL TLS,
  half-close, layered TLS, deadline, PacketConn echo, and Happy Eyeballs
  tests);
- 2 low-level VCL poll tests;
- 5 multi-worker stress tests, 1 sharded-accept scaling test, plus 2 Mode 2
  ownership and UDP-rejection invariant tests;
- 1 deliberately skipped test (half-close over cut-through transport);
- 2 opt-in benchmarks.

Covered behavior includes:

- TCP IPv4/IPv6 connect, listen, accept, read, write, close, and half-close
  (`CloseRead` → local `io.EOF`; `CloseWrite` → peer FIN and local
  `net.ErrClosed`);
- connected UDP IPv4/IPv6 in Mode 3;
- unconnected UDP (`ListenPacket`) with per-peer session adapter in Mode 3
  (3-message echo round-trip validated);
- HTTP/1.1 requests, responses, keep-alive-configured requests, and the public client helper;
- standard `crypto/tls` over a VCL-backed connection;
- native VCL TLS (`DialTLS`/`ListenTLS`) via VPP's OpenSSL engine with
  functional parity against layered `crypto/tls`;
- Happy Eyeballs on localhost;
- connect/accept context cancellation;
- deadlines that expire, clear, and change while a read is blocked;
- close waking a blocked read;
- `CloseWrite` waking a writer parked on a full VCL FIFO;
- simultaneous blocked read/write on a 6 MiB payload;
- multiple VPP worker threads with concurrent connection and HTTP stress;
- shutdown before Init and shutdown waking a blocked accept.

The local audit uses Go 1.26.1 and a VPP 26.10 development build from the
release-build tree. This is local evidence, not yet a compatibility matrix.

## 4. What the evidence does not establish

The repository does not currently establish:

- safe Mode 2 UDP in the pinned VPP build;
- sustained full-surface and soak validation of VLS Mode 2;
- clean-host packaging across supported distributions and VPP installs;
- HTTP/2 or current gRPC interoperability;
- extended native TLS controls (SNI matching, ALPN, verify hooks, session
  ticket policy, keylog) via `SET_ENDPT_EXT_CFG`;
- a version range across VPP releases;
- production soak behavior during concurrent lifecycle transitions;
- a reproducible performance advantage over kernel networking.

The benchmark functions measure VCLNET paths only. No checked-in dataset
documents hardware, topology, kernel baseline, variance, or statistical
method. Exact latency and speedup claims were therefore removed from this
report.

## 5. Threading and scaling

The package now has two implementations behind one internal dispatcher.

Mode 3 is the default compatibility path. It pins a calling goroutine for each
immediate VLS call, registers encountered OS threads, and uses one permanently
pinned shared VLS epoll poller. All threads share VCL worker 0, so calls remain
serialized inside VLS.

Mode 2 is opt-in through `InitWithOptions` or environment overrides and requires
`multi-thread-workers` in `vcl.conf`. It creates N permanently pinned Go worker
loops. Each owns one VCL worker, message queue, epoll handle, operation channel,
and exact waiter set. Session creation is round-robin; every later operation is
submitted to the owner. Only TCP sessions are admitted to this pool today; UDP
returns `EOPNOTSUPP` before VLS allocation.

Raw VLS handles collide across worker-local pools, so Mode 2 maps a
process-unique internal handle to `{owner, raw}`. Before each operation it
checks the raw VCL worker index and rejects a mismatch before VLS can enter its
migration or clone path. Listeners use per-worker sharding: each `Listen` or
`ListenTLS` call creates one VLS listener per worker on the same address:port
using `SO_REUSEPORT`, runs a per-worker accept loop, and fans accepted
connections into a shared channel.

A VPP process configured with `cpu { workers N }` can distribute VPP-side work
in either VLS mode. It does not by itself select application-side Mode 2.

## 6. Cut-through

With compatible VCL scopes and two applications attached to the same VPP,
VPP may select its local cut-through transport. The data then moves through
shared-memory FIFOs rather than a kernel TCP path.

VCLNET can reach that mechanism because it uses normal VCL sessions and the
test configuration enables `app-scope-local`. However, this report makes no
numeric performance claim. The actual benefit depends on payload size,
notification batching, VLS locking, CPU placement, memory topology, and the
comparison baseline.

A production performance report should capture raw data for:

- same-host cut-through;
- VPP TCP without cut-through;
- kernel TCP;
- multiple payload sizes and concurrency levels;
- tail latency, throughput, CPU, context switches, and memory bandwidth;
- mode-3 serialization effects.

## 7. Principal risks

| Risk | Current mitigation | Remaining action |
| --- | --- | --- |
| Clean-host packaging | pkg-config template and prefix-driven Make targets | Validate supported distro and container builds |
| VPP API/behavior drift | Integration harness exercises local VPP | P0 automated version matrix |
| Unconnected UDP limitation | Per-peer session adapter implemented; `WriteTo` only reaches known peers | Document or mitigate the no-originate-session VPP constraint |
| Connect failure ambiguity | Immediate hard failures are wrapped | Add reliable post-EPOLLOUT error query/tests |
| Lifecycle races | Shutdown gates new work and wakes dispatcher waits | Track/drain all live objects and soak test |
| Mode 2 rollout risk | Session-affine pool, ownership preflight, per-worker sharded listeners, dual-mode harness | Full-surface soak, CI history, and performance baseline |
| Mode 2 cut-through UDP crash | UDP fails before VLS allocation; harness probes VPP after tests | Produce and report a minimal reproducer; enable only on a verified-safe VPP build |
| Unsupported ecosystem assumptions | Docs now distinguish tested vs inferred | Add HTTP/2/gRPC/application-specific tests |

## 8. Recommendation

Continue with VCLNET as the engineering path for Go-to-VPP integration; the
explicit interface is materially more maintainable than syscall interception.

Before broad production adoption:

1. validate clean-host and container packaging against supported VPP installs;
2. put the isolated VPP suites in CI against pinned versions;
3. add application-protocol and lifecycle soak tests;
4. collect a reproducible performance baseline;
5. keep Mode 2 opt-in until sustained CI and measurements establish both
   correctness and a useful gain over Mode 3.

A narrower deployment can proceed earlier when its workload uses the tested
TCP surface in either mode, or connected UDP in Mode 3, pins the validated VPP
build, runs the same integration suite, and accepts the documented limitations.

---

Document status: audited 2026-07-09.
