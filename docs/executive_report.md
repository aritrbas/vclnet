# VCLNET decision report

Document status: updated 2026-07-21.

## Executive summary

VCLNET gives Go programs an explicit `net.Conn` / `net.Listener` interface
to VPP's VCL session layer. This avoids the fundamental limitation of the
earlier LD_PRELOAD/Frida experiments: Go's networking runtime issues kernel
syscalls directly and cannot treat a VCL session handle as a kernel file
descriptor.

The current repository demonstrates a viable architecture for TCP and UDP in
both VLS modes, including unconnected UDP via a per-peer session adapter
(`ListenPacket`). Its isolated VPP test harness covers IPv4, IPv6, HTTP/1.1,
layered TLS, native VCL TLS, context cancellation, live deadlines, concurrent
reads/writes, shutdown, PacketConn echo, and VPP configured with multiple
worker threads.

It is not yet a generally distributable production library. Mode 3 remains the
compatibility default and passed the latest local matrix. Mode 2 uses fixed,
session-affine worker loops with per-worker sharded listeners, but the current
four-worker suite can crash the VCL application during concurrent accept and
leave an inconsistent control ring. The build also depends on a worker-teardown
API supplied by a local VPP review patch rather than an established stock
release. Those are P0 release blockers alongside automated compatibility CI.

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

The no-VPP suite has 175 top-level tests. The VPP-backed suites contain:

- 40 runnable public-package single-worker tests (including native VCL TLS,
  half-close, layered TLS, deadline, PacketConn echo, Happy Eyeballs,
  concurrent-Shutdown stress, connection-refused/TLS-refused cases,
  HTTP/2 cleartext + TLS-ALPN, and gRPC cleartext + TLS);
- 2 low-level VCL poll tests;
- 5 multi-worker stress tests, 1 sharded-accept scaling test, 1 Mode 2
  ownership test, 1 Mode 2 UDP ListenPacket test, plus 1 Mode 2
  repeated-shutdown stress test;
- 1 deliberately skipped test (half-close over cut-through transport);
- 2 opt-in benchmarks.

Covered behavior includes:

- TCP IPv4/IPv6 connect, listen, accept, read, write, close, and half-close
  (`CloseRead` → local `io.EOF`; `CloseWrite` → peer FIN and local
  `net.ErrClosed`);
- async-connect completion verified via `vppcom_session_get_error` before
  a conn is returned, so a stale `EPOLLOUT` cannot yield a spurious
  success; VPP-side refused-peer delivery is a documented gap on the
  pinned loopback build;
- connected UDP IPv4/IPv6 in both modes;
- unconnected UDP (`ListenPacket`) with per-peer session adapter in both
  modes (3-message echo round-trip validated in Mode 3, ListenPacket in
  Mode 2);
- HTTP/1.1 requests, responses, keep-alive-configured requests, and the public client helper;
- HTTP/2 over vclnet (cleartext prior-knowledge and TLS-with-ALPN,
  including concurrent streams);
- gRPC unary and server-streaming RPCs over both cleartext and TLS
  transports (validated with `grpc-go`'s stock Health service);
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

The 2026-07-21 local audit uses Go 1.26.1 and patched VPP
`v26.10-rc0~231-g0a143dac6` from the release-build tree. Unit, race, vet,
build, both standard integration modes, and Mode 3 multi-worker passed. Mode 2
multi-worker failed in `TestMultiWorkerHTTPConcurrent` with a VCL
`vls_accept` SIGSEGV and a subsequent VPP MQ-order warning. This is local
evidence, not yet a compatibility matrix.

## 4. What the evidence does not establish

The repository does not currently establish:

- safe concurrent listener/accept behavior in Mode 2;
- build and runtime compatibility with an unpatched stock VPP release;
- full Mode 2 UDP data-path stress testing (connect and ListenPacket pass;
  multi-message echo needs further validation);
- sustained full-surface and soak validation of VLS Mode 2;
- clean-host packaging across supported distributions and VPP installs;
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
submitted to the owner. Both TCP and UDP sessions are supported; UDP connects
block on the worker thread until fully established to work around a VPP
half-open session cleanup race.

Raw VLS handles collide across worker-local pools, so Mode 2 maps a
process-unique internal handle to `{owner, raw}`. Before each operation it
checks the raw VCL worker index and rejects a mismatch before VLS can enter its
migration or clone path. Listeners use per-worker sharding: each `Listen` or
`ListenTLS` call creates one VLS listener per worker on the same address:port
using `SO_REUSEPORT`, runs a per-worker accept loop, and fans accepted
connections into a shared channel.

That listener design currently has a P0 correctness failure. Source and crash
analysis indicate that a dependent `ACCEPTED` event can be observed before its
FIFO segment is published to VCL; the failed-attach reply then null-dereferences
after reserving an MQ element. See
[mode2_accept_mq_investigation.md](mode2_accept_mq_investigation.md). Mode 2 is
not a production server path until the ordering and error-path fixes are
validated.

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
| Mode 2 accepted-session ordering | Mode 3 remains default; failure chain and reproducer are documented | Make failed-accept replies null-safe, enforce segment-before-session publication, and add a deterministic segment-growth regression |
| Worker teardown API compatibility | Local patched VPP exports `vls_unregister_vcl_worker`; repeated shutdown passes | Merge upstream or explicitly carry/pin the patch and verify the symbol in packaging |
| VPP API/behavior drift | Integration harness exercises local VPP | P0 automated version matrix |
| Unconnected UDP limitation | Per-peer session adapter implemented; `WriteTo` only reaches known peers | Document or mitigate the no-originate-session VPP constraint |
| Connect failure ambiguity | Client-side: dial waits on the union of `EPOLLOUT`\|`EPOLLERR`\|`EPOLLHUP` and calls `vppcom_session_get_error` before returning, so a stale EPOLLOUT cannot yield a spurious success; VPP-side: refused-peer signalling doesn't reach the app's epoll on the pinned loopback build (documented in [connect_error_investigation.md](connect_error_investigation.md)) | Reproduce refused/unreachable on a real-NIC topology and confirm the CONNECTED-with-error path is observed |
| Lifecycle races | `liveRegistry` tracks listeners, conns, PacketConns, and in-flight dials; Shutdown closes listeners first, drains up to 5 s, then force-closes stragglers; concurrent-Shutdown stress covers active accepts/reads/writes/dials | Long-duration soak in CI |
| Mode 2 rollout risk | Session-affine pool, ownership preflight, per-worker sharded listeners, dual-mode harness | Close the accept P0, then run full-surface soak, CI history, and performance baseline |
| Mode 2 half-open UDP cleanup race | Mode 2 UDP connects block until established; harness probes VPP after tests | Resolved on the vclnet side; VPP upstream bug remains (half-open cleanup stale RPC) |
| Unsupported ecosystem assumptions | Docs distinguish tested vs inferred; HTTP/2 and gRPC (unary + server-streaming, both cleartext and TLS) now covered in the standard integration harness | Continue expanding as new stacks are adopted |

## 8. Recommendation

Continue with VCLNET as the engineering path for Go-to-VPP integration; the
explicit interface is materially more maintainable than syscall interception.

Before broad production adoption:

1. validate clean-host and container packaging against supported VPP installs;
2. land or explicitly carry the required worker-unregister API;
3. fix and regression-test Mode 2 accepted-session segment ordering;
4. put the isolated VPP suites in CI against pinned versions;
5. add application-protocol and lifecycle soak tests;
6. collect a reproducible performance baseline;
7. keep Mode 2 opt-in until sustained CI and measurements establish both
   correctness and a useful gain over Mode 3.

A narrower Mode 3 deployment can proceed earlier when its workload uses the
tested TCP and UDP surface, pins the validated patched VPP build, runs the same
integration suite, and accepts the documented limitations. Mode 2 server
deployment should wait for the P0 accept fix.
