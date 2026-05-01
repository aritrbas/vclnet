# VCLNET decision report

## Executive summary

VCLNET gives Go programs an explicit `net.Conn` / `net.Listener` interface
to VPP's VCL session layer. This avoids the fundamental limitation of the
earlier LD_PRELOAD/Frida experiments: Go's networking runtime issues kernel
syscalls directly and cannot treat a VCL session handle as a kernel file
descriptor.

The current repository demonstrates a viable architecture for TCP and connected
UDP. Its isolated VPP test harness covers IPv4, IPv6, HTTP/1.1, layered TLS,
context cancellation, live deadlines, concurrent reads/writes, shutdown, and
VPP configured with multiple worker threads.

It is not yet a generally distributable production library. The highest
priority gaps are portable build/module packaging, automated VPP compatibility
CI, and a decision on the incomplete unconnected-UDP API. Application-side VLS
also remains serialized in mode 3; mode 2 requires a different, session-affine
worker architecture.

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

The no-VPP suite has 129 top-level tests. The VPP-backed suites contain:

- 26 runnable public-package single-worker tests;
- 2 low-level VCL poll tests;
- 5 multi-VPP-worker stress tests;
- 1 intentionally skipped unconnected-UDP test;
- 2 opt-in benchmarks.

Covered behavior includes:

- TCP IPv4/IPv6 connect, listen, accept, read, write, close;
- connected UDP IPv4/IPv6;
- HTTP/1.1 requests, responses, keep-alive-configured requests, and the public client helper;
- standard `crypto/tls` over a VCL-backed connection;
- Happy Eyeballs on localhost;
- connect/accept context cancellation;
- deadlines that expire, clear, and change while a read is blocked;
- close waking a blocked read;
- simultaneous blocked read/write on a 6 MiB payload;
- multiple VPP worker threads with concurrent connection and HTTP stress;
- shutdown before Init and shutdown waking a blocked accept.

The local audit uses Go 1.26.1 and a VPP 26.06 development build from the
release-build tree. This is local evidence, not yet a compatibility matrix.

## 4. What the evidence does not establish

The repository does not currently establish:

- correct arbitrary-peer `net.PacketConn` behavior;
- VLS `multi-thread-workers` mode;
- portable compilation outside the author's VPP tree;
- HTTP/2 or current gRPC interoperability;
- native VCL TLS;
- TCP half-close;
- a version range across VPP releases;
- production soak behavior during concurrent lifecycle transitions;
- a reproducible performance advantage over kernel networking.

The benchmark functions measure VCLNET paths only. No checked-in dataset
documents hardware, topology, kernel baseline, variance, or statistical
method. Exact latency and speedup claims were therefore removed from this
report.

## 5. Threading and scaling

VCL stores worker state in pthread-local storage. VCLNET pins each goroutine for
the duration of a VLS call and registers each encountered OS thread. Sessions
are non-blocking; on EAGAIN the goroutine releases the thread and waits on a Go
channel.

One permanent poller goroutine owns a VLS epoll instance. It tracks a union
interest mask per session and independent waiters for reads and writes.
Cancelling a deadline removes only that waiter, so another operation on the
same connection stays registered.

The current VCL configuration is mode 3:

- all application threads share one VCL worker;
- VLS protects shared state with locks;
- the design is correct for the tested loads;
- calls serialize inside VLS.

A VPP process configured with `cpu { workers N }` can distribute VPP-side
session work, but it does not remove application-side mode-3 serialization.

Mode 2 (`multi-thread-workers`) gives each thread a VCL worker and message
queue. The current shared poller cannot use it safely because the poller would
touch sessions created by other workers. A future implementation needs fixed,
permanently pinned event loops and session affinity.

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
| Workstation-specific CGo/rpath | Known build works in this workspace | P0 portable discovery and clean build |
| VPP API/behavior drift | Integration harness exercises local VPP | P0 automated version matrix |
| Incomplete UDP surface | Connected UDP tested; unconnected test skipped | Implement adapter or deprecate API |
| Connect failure ambiguity | Immediate hard failures are wrapped | Add reliable post-EPOLLOUT error query/tests |
| Lifecycle races | Shutdown gates new work and wakes poller waits | Track/drain all live objects and soak test |
| Application-side serialization | Mode 3 is correct and simple | Session-affine mode-2 redesign if data justifies it |
| Debug-build cut-through cleanup race | Release build is used for validation | Reproduce/report upstream with a minimal case |
| Unsupported ecosystem assumptions | Docs now distinguish tested vs inferred | Add HTTP/2/gRPC/application-specific tests |

## 8. Recommendation

Continue with VCLNET as the engineering path for Go-to-VPP integration; the
explicit interface is materially more maintainable than syscall interception.

Before broad production adoption:

1. complete portable build/module packaging;
2. put the isolated VPP suites in CI against pinned versions;
3. resolve the `ListenPacket` contract;
4. add application-protocol and lifecycle soak tests;
5. collect a reproducible performance baseline;
6. pursue VLS mode 2 only if measurements show mode-3 serialization is a
   limiting factor.

A narrower deployment can proceed earlier when its workload uses only the
tested TCP or connected-UDP surface, pins the validated VPP build, runs the
same integration suite, and accepts the documented limitations.

---

Document status: audited 2026-07-08.
