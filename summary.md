# vclnet implementation summary

Last audited: 2026-07-09

## 1. What works today

vclnet exposes Go networking interfaces backed by the VPP VCL Locked
Sessions (VLS) API. Mode 3 remains the compatibility default. An opt-in Mode 2
worker pool now routes every operation to a permanently pinned, session-affine
VCL worker.

| Area | Current behavior | Validation |
| --- | --- | --- |
| TCP | Listen, accept, dial, read, write, close on IPv4 and IPv6 | Unit plus VPP integration |
| Context dialing | TCP in both modes and connected UDP in Mode 3 honor cancellation while resolving or connecting | Unit plus VPP integration for successful paths |
| Happy Eyeballs | Unsuffixed `"tcp"` interleaves IPv6 and IPv4 attempts with a configurable stagger and closes successful losers | Localhost VPP integration plus helper tests |
| Deadlines | Resettable read and write deadlines wake operations already parked for readiness | Timer unit tests plus TCP and Mode 3 UDP VPP integration |
| Concurrent I/O | One session can retain separate read and write waiters | Readiness state-machine tests plus 6 MiB TCP integration |
| Listener cancellation | `TCPListener.AcceptContext` distinguishes context expiry from listener or package close | Unit plus VPP integration |
| Connected UDP | `Dial("udp*")`, read, write, and deadlines on IPv4 and IPv6 in Mode 3; Mode 2 fails before allocation with `EOPNOTSUPP` | VPP integration plus Mode 2 rejection tests |
| HTTP and TLS | HTTP/1.1 and standard `crypto/tls` over vclnet TCP | VPP integration |
| Shutdown | Idempotent, wakes parked operations, stops VLS workers, and rejects later operations | Unit and subprocess VPP integration |
| VLS Mode 3 | Shared VCL worker with one persistent readiness poller | Default; standard and multi-VPP-worker harnesses |
| VLS Mode 2 | N pinned VCL workers, per-worker epoll, virtual process-wide handles, ownership preflight, and no shared poller; TCP only for the pinned VPP build | Opt-in; unit tests and multi-worker TCP, IPv6, HTTP, ownership, and UDP-rejection stress |

DNS remains on the host resolver. VLS handles remain internal and are never
passed to the Go runtime poller.

Mode 2 is selected with `InitWithOptions` or environment overrides:

```go
err := vclnet.InitWithOptions("my-service", vclnet.Options{Workers: 4})
```

```text
VCLNET_VLS_MODE=2
VCLNET_WORKERS=4
```

Its `vcl.conf` must contain `multi-thread-workers`. Mode 3 remains the default
until sustained CI and a reproducible performance baseline justify a rollout
change. Mode 2 UDP remains disabled because the pinned VPP 26.06 build crashes
while cleaning up cut-through datagram TX state; callers receive an error
wrapping `EOPNOTSUPP` before any VLS datagram session is created.

### Mode 2 stability assessment

The Mode 2 concurrency core has explicit, testable invariants: workers remain
pinned for their lifetime, raw VLS handles never cross owners, process-wide
virtual handles disambiguate worker-local indexes, and each worker owns its
readiness state. The current four-worker VPP harness passes TCP, IPv6, HTTP,
large-payload, ownership, UDP-rejection, VPP-liveness, and process-exit checks.

That evidence does **not** make Mode 2 production-stable yet. Two compatibility
gaps remain release blockers:

- **UDP:** VPP 26.06 crashes while cleaning up Mode 2 cut-through datagram
  state. vclnet fails closed with `EOPNOTSUPP`; Mode 2 is TCP-only until the VPP
  defect is reproduced and fixed upstream or a verified-safe build is selected.
- **Worker retirement:** teardown currently waits for non-bootstrap worker
  threads through Linux `/proc/self/task`. Go cannot terminate its process-main
  OS thread, so that case is recognized as quiesced and skipped. This behavior
  is deterministic and regression-tested, but the `/proc` polling and special
  case should be replaced by an explicit, platform-independent worker terminal
  state before Mode 2 is considered production-ready.

## 2. Test inventory

The repository currently has 141 top-level no-VPP tests:

- 118 public-package contract and unit tests;
- 9 shared Mode 3 poller tests;
- 11 Mode 2 worker, ownership, parking, UDP rejection, and shutdown tests;
- 3 byte-order and errno helper tests.

VPP-backed coverage currently has:

- 26 runnable public-package tests in the standard integration harness;
- 1 deliberately skipped unconnected-UDP `PacketConn` test;
- 2 low-level vclpoll echo tests;
- 5 multi-worker stress tests plus 2 Mode 2 ownership and UDP-rejection
  invariant tests;
- 2 opt-in benchmarks.

The standard harness exercises TCP IPv4 and IPv6, connected UDP IPv4 and IPv6,
HTTP, layered TLS, Happy Eyeballs, context-aware accept, deadline expiry and
updates, close-unblock behavior, simultaneous blocked read and write, address
reporting, and shutdown.

Commands:

```bash
make test
make race
make vet
make build

sudo -E bash test/run_integration.sh
sudo -E bash test/run_multiworker.sh --mode 3 4
sudo -E bash test/run_multiworker.sh --mode 2 4
```

Counts are snapshots. `go test -list .` and the test files are the source of
truth.

## 3. Pending work

This is the canonical pending-work list. Other documents link here rather than
maintaining independent roadmaps.

| Priority | Work item | Completion criteria |
| --- | --- | --- |
| P0 | Automated compatibility CI | Run unit, race, vet, build, standard VPP integration, Mode 3 multi-worker, and Mode 2 multi-worker jobs against a documented VPP version matrix; retain logs and fail on unexpected skips or a VPP crash |
| P0 | Resolve the Mode 2 UDP cleanup crash | Produce a minimal VPP reproducer, report or fix the stale cut-through TX event upstream, and enable Mode 2 UDP only on a verified-safe VPP build; retain fail-fast `EOPNOTSUPP` and VPP-liveness regressions until then |
| P0 | Replace Mode 2 thread-retirement polling | Remove `/proc/self/task` polling and the process-main-thread exception; use an explicit worker terminal state or supported unregister/join mechanism, prove no VLS call or TLS destructor can race `vppcom_app_destroy`, and pass repeated shutdown stress |
| P0 | Complete Mode 2 rollout validation | Run the full supported TCP integration surface, repeated shutdown cases, a long concurrency soak, and no-migration and safe-UDP-rejection assertions in CI; keep Mode 3 as default until the sustained-green and performance gates pass |
| P1 | Shard Mode 2 listeners | Replace the correct but single-owner v1 listener path with one listener per worker and an accept fan-in; demonstrate accept scaling without cross-worker VLS access |
| P1 | Decide the unconnected UDP contract | Implement a per-peer session adapter with correct concurrent `PacketConn` behavior, or remove or deprecate `ListenPacket`; enable the skipped integration test |
| P1 | Verify asynchronous connect completion errors | Replace the EPOLLOUT-implies-success assumption when VPP exposes a reliable session error query, and add refused and unreachable integration cases |
| P1 | Establish reproducible performance baselines | Record topology, hardware, VPP and kernel configs, payload and concurrency distributions, raw benchmark output, and comparisons before publishing speedup claims |
| P1 | Harden lifecycle and graceful drain | Track live listeners and connections, define graceful-drain ordering, and stress concurrent Shutdown with active reads, writes, accepts, and dials |
| P2 | Native VCL TLS | Expose `VPPCOM_PROTO_TLS` plus certificate and key configuration and compare it with layered `crypto/tls` |
| P2 | TCP half-close | Add and test `CloseRead` and `CloseWrite` using VCL shutdown semantics |
| P2 | UDP edge semantics | Decide port-zero listeners, zero-length datagrams, truncation, connected `WriteTo`, multicast and broadcast, and source-address behavior |
| P2 | Wider protocol validation | Add HTTP/2 and current gRPC integration tests before claiming those stacks are supported |

## 4. Known limitations

1. **Unconnected UDP is incomplete.** `ListenPacket` can create a bound VLS
   listener, but arbitrary peer-oriented `ReadFrom` and `WriteTo` behavior is
   not implemented end to end. Use connected UDP in Mode 3.
2. **Mode 2 UDP is disabled.** The pinned VPP 26.06 build can crash after a
   Mode 2 connected-UDP close while processing stale cut-through TX state.
   Mode 2 UDP calls therefore fail before VLS allocation with an error wrapping
   `EOPNOTSUPP`; use Mode 3 for connected UDP.
3. **Mode 3 is still the default.** It is the broadest-tested compatibility
   path, but application-side VLS work serializes on one shared worker.
4. **Mode 2 is opt-in.** It requires `multi-thread-workers`, permanently pins
   one OS thread per requested worker, and currently keeps each listener on one
   owner worker. Listener sharding is pending.
5. **Mode 2 uses virtual handles internally.** Raw VLS handles are worker-local
   pool indexes and can collide, so vclpoll maps process-unique handles to an
   owning worker and raw handle. They never escape the internal package.
6. **Mode 2 worker retirement is Linux-specific today.** Shutdown waits for
   exit-capable worker threads through `/proc/self/task`; a worker on Go's
   process-main thread is treated as quiesced because the runtime parks that
   thread instead of terminating it. This path passes its regression and live
   harness tests, but replacing it is a P0 maintainability and portability item.
7. **Mode 2 teardown is process-final.** The bootstrap OS thread is parked
   after `vppcom_app_destroy` because the pinned VPP VLS destructor is unsafe
   after global VLS state is destroyed. Reinitialization after Shutdown is not
   supported in either mode.
8. **Client and server need separate VCL apps.** The integration topology uses
   subprocesses because one VCL app cannot connect back to its own listener.
9. **Release VPP is the validated target.** Cut-through cleanup exposed the
   Mode 2 UDP failure above; fail-fast rejection prevents that path and the
   harness treats any VPP process crash as a test failure.
10. **No comparative benchmark is shipped.** Benchmark functions are test tools,
   not evidence for a specific speedup.

## 5. Architecture snapshot

```text
Go application
    |
    | net.Listener / net.Conn
    v
vclnet public package
    |
    | stable package functions
    v
internal/vclpoll dispatcher
    |-- Mode 3 default
    |     |-- per-call LockOSThread and thread registration
    |     `-- one persistent shared VLS epoll poller
    |
    `-- Mode 2 opt-in
          |-- N lifetime-pinned worker goroutines
          |-- process handle -> {owner worker, raw VLS handle}
          |-- one VLS epoll and exact waiter set per worker
          |-- all admitted TCP operations run on the owner
          `-- UDP rejected before VLS allocation with EOPNOTSUPP
    |
    v
libvppcom.so <-> shared-memory FIFOs and message queues <-> VPP
```

All sessions are non-blocking. On EAGAIN, callers wait on Go channels rather
than holding a calling OS thread. Mode 3 delegates readiness to its shared
poller. Mode 2 registers the waiter on the session owner and lets that same
worker drive epoll and retry the operation.

Raw Mode 2 ownership is checked before every session operation with
`vlsh_to_session_and_worker_index`. A mismatch is rejected and counted before
VLS can enter its migration or clone path.

## 6. Threading modes

| Property | Mode 3 default | Mode 2 opt-in |
| --- | --- | --- |
| VCL workers visible to the app | One shared worker | One per pinned event loop |
| Message queues | One, protected by VLS locks | One per worker |
| Session routing | Any registered thread under shared-mode locks | Every operation submitted to the owner |
| Readiness | One shared epoll poller | One epoll and wait set per worker |
| Application-side parallelism | Serialized | Parallel across owner workers |
| Listener behavior | Shared-mode listener | Correct single-owner v1 listener |
| Protocol surface | TCP and connected UDP | TCP; UDP fails safely with `EOPNOTSUPP` |
| Configuration | No `multi-thread-workers` token | Requires `multi-thread-workers` |
| Status | Default and broadly tested | Experimental, opt-in, rollout validation pending |

Running VPP with `cpu { workers N }` is separate from selecting a VLS mode.
The multi-worker harness accepts `--mode 3` and `--mode 2` so both dimensions
are explicit.

## 7. Error and lifecycle behavior

VCL negative return values become `VCLError` values containing
`syscall.Errno`. Public operations wrap them in `*net.OpError`, preserving
`errors.Is` checks such as `ECONNREFUSED`, `ECONNRESET`, and `ETIMEDOUT`.

`Shutdown`:

1. marks the package closed so new public operations fail;
2. prevents parked operations from re-entering VLS;
3. stops the active dispatcher and wakes its exact waiters;
4. in Mode 2, closes sessions on their owners, drains worker MQ cleanup, and
   waits for non-bootstrap worker threads to exit;
5. calls `vppcom_app_destroy` only after the active readiness machinery has
   stopped.

Shutdown is idempotent and process-final. Services should stop admitting work
and allow handlers to drain before calling it.
