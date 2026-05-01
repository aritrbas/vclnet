# vclnet implementation summary

Last audited: 2026-07-08

## 1. What works today

vclnet exposes Go networking interfaces backed by VPP's VCL Locked Sessions
(VLS) API.

| Area | Current behavior | Validation |
| --- | --- | --- |
| TCP | Listen, accept, dial, read, write, close on IPv4 and IPv6 | Unit plus VPP integration |
| Context dialing | TCP and connected UDP honor cancellation while resolving/connecting | Unit plus VPP integration for successful paths |
| Happy Eyeballs | Unsuffixed `"tcp"` interleaves IPv6/IPv4 attempts with a configurable stagger and closes successful losers | Localhost VPP integration plus helper tests |
| Deadlines | Resettable read/write deadlines wake operations already blocked in the shared poller | Timer unit tests plus TCP/UDP VPP integration |
| Concurrent I/O | One session can retain separate read and write waiters in the shared poller | Poller state-machine tests plus 6 MiB TCP integration |
| Listener cancellation | `TCPListener.AcceptContext` distinguishes context expiry from listener/package close | Unit plus VPP integration |
| Connected UDP | `Dial("udp*")`, read, write, deadlines on IPv4 and IPv6 | VPP integration |
| HTTP | `net/http` server, `Transport`, and `NewHTTPClient` over TCP | VPP integration |
| TLS | Standard `crypto/tls` layered over a vclnet TCP connection | VPP integration |
| Shutdown | Idempotent; safe before Init; wakes a blocked accept and rejects later public operations | Subprocess unit and VPP integration |
| Multiple VPP workers | TCP/IPv6/HTTP stress with VPP configured for multiple workers | `test/run_multiworker.sh` |

DNS remains on the host resolver. VLS session handles remain internal and are
never passed to Go's kernel poller.

## 2. Test inventory

The repository currently has 129 top-level no-VPP tests:

- 117 public-package contract/unit tests;
- 9 shared-poller tests;
- 3 byte-order/errno helper tests.

VPP-backed coverage currently has:

- 26 runnable public-package tests in the standard single-worker harness;
- 1 deliberately skipped unconnected-UDP `PacketConn` test;
- 2 low-level vclpoll echo tests;
- 5 multi-VPP-worker stress tests;
- 2 opt-in benchmarks.

The standard harness exercises TCP IPv4/IPv6, connected UDP IPv4/IPv6,
HTTP, layered TLS, Happy Eyeballs, context-aware accept, deadline
expiry/reset/update, close-unblock behavior, simultaneous blocked read/write,
address reporting, and shutdown.

Commands:

```bash
# Does not require VPP; integration tests self-skip.
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...

# Starts an isolated VPP release build.
sudo bash test/run_integration.sh
sudo bash test/run_multiworker.sh 4

# Compiles every example.
make build
```

Counts are snapshots, not an API guarantee. `go test -list .` and the test
files are the source of truth.

## 3. Pending work

This is the canonical pending-work list. Other documents should link here
instead of maintaining independent roadmaps.

| Priority | Work item | Completion criteria |
| --- | --- | --- |
| P0 | Automated compatibility CI | Run unit/race/vet plus isolated VPP integration against a documented VPP version matrix; retain logs and fail on unexpected skips |
| P1 | Decide and implement the unconnected UDP contract | Either implement a per-peer session adapter that provides correct `PacketConn` semantics (including concurrent peers), or remove/deprecate `ListenPacket`; enable the currently skipped integration test |
| P1 | Verify asynchronous connect completion errors | Replace the current EPOLLOUT-implies-success assumption when VPP exposes a reliable session error query, and add refused/unreachable integration cases |
| P1 | Establish reproducible performance baselines | Record topology, hardware, VPP/kernel configs, payload/concurrency distributions, raw benchmark output, and comparisons; do not publish multiplier claims before this |
| P1 | Harden lifecycle/drain behavior | Track live listeners/connections, define graceful-drain ordering, and stress concurrent Shutdown with active reads, writes, accepts, and dials |
| P2 | VLS `multi-thread-workers` mode | Replace the cross-thread shared-poller model with session-affine, permanently pinned worker event loops; validate mode 2 without session migration failures |
| P2 | Native VCL TLS | Expose `VPPCOM_PROTO_TLS` plus certificate/key configuration and compare it with layered `crypto/tls` |
| P2 | TCP half-close | Add and test `CloseRead` / `CloseWrite` using VCL shutdown semantics |
| P2 | UDP edge semantics | Decide whether to add ephemeral port-zero listener support (the current behavior is a documented rejection), zero-length datagrams, truncation behavior, connected `WriteTo`, multicast/broadcast expectations, and source-address handling |
| P2 | Wider protocol/application validation | Add HTTP/2 and current gRPC integration tests before claiming those stacks are supported |

## 4. Known limitations

1. **Unconnected UDP is incomplete.** `ListenPacket` can create a bound VLS
   UDP listener, but arbitrary peer-oriented `ReadFrom`/`WriteTo` behavior
   is not implemented end to end. Use connected UDP.
2. **Application-side VLS is serialized.** The current configuration uses VLS
   mode 3. A VPP process may have several worker threads, but application VLS
   calls share one worker and its locks.
3. **Mode 2 is incompatible with the current poller.** In
   `multi-thread-workers` mode, a session belongs to the thread/worker that
   created it. The single poller touching every session would trigger migration
   paths that fail for active sessions.
4. **Client and server need separate VCL apps.** The test topology uses
   subprocesses because a process cannot connect back to a listener in the same
   VCL app.
5. **Release VPP is the validated target.** A cut-through cleanup race was
   observed with debug VPP builds under overlapping sessions.
6. **No recorded comparative benchmark is shipped.** Benchmark functions are
   test tools, not evidence for a specific speedup.

## 5. Architecture snapshot

```text
Go application
    |
    | net.Listener / net.Conn
    v
vclnet public package
    |
    | internal calls with opaque VLS handles
    v
internal/vclpoll
    |-- CGo wrappers around vls_*
    |-- per-call LockOSThread and thread registration
    `-- one persistent VLS epoll poller
            |-- union event mask per session
            |-- independent read/write waiters
            `-- exact waiter cancellation
    |
    v
libvppcom.so <-> shared memory FIFOs and message queues <-> VPP
```

All sessions are non-blocking. On EAGAIN, the calling goroutine releases its OS
thread and waits on a Go channel. The poller owns the VLS epoll handle, drains
the message queue, and wakes only waiters whose event masks match. Deadline
updates cancel the precise waiter, allowing a concurrent read or write on the
same session to remain registered.

## 6. Threading modes

| Property | Mode 3 (current) | Mode 2 (future work) |
| --- | --- | --- |
| VCL workers visible to the app | One shared worker | One worker per pinned event loop |
| Message queues | One, protected by VLS locks | One per worker |
| Session access from another thread | Safe through shared mode locks | Requires affinity or migration |
| Current shared poller | Compatible | Incompatible |
| Parallelism inside VCL | Serialized | Potentially parallel |
| Status | Tested | Not supported |

Running VPP with `cpu { workers N }` is not the same as enabling VCL
`multi-thread-workers`. The multi-worker test script validates the former
while deliberately retaining VLS mode 3.

## 7. Error and lifecycle behavior

VCL negative return values are wrapped as `VCLError` values containing
`syscall.Errno`. Public operations wrap them in `*net.OpError`, preserving
`errors.Is` checks such as `ECONNREFUSED`, `ECONNRESET`, and
`ETIMEDOUT`.

`Shutdown`:

1. marks the package closed so new public operations fail;
2. prevents woken low-level operations from re-entering VLS;
3. stops the shared poller and wakes its waiters;
4. calls `vppcom_app_destroy` only if Init succeeded.

It is idempotent, but applications should still stop admitting new work and
allow handlers to drain before calling it.
