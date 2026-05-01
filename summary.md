# vclnet implementation summary

Last updated: 2026-07-21

## 1. What works today

vclnet exposes Go networking interfaces backed by the VPP VCL Locked
Sessions (VLS) API. Mode 3 remains the compatibility default. An opt-in Mode 2
worker pool now routes every operation to a permanently pinned, session-affine
VCL worker.

| Area | Current behavior | Validation |
| --- | --- | --- |
| TCP | Listen, accept, dial, read, write, close, and half-close on IPv4 and IPv6 | Unit plus VPP integration |
| TCP half-close | `CloseRead` and `CloseWrite` route to `vls_shutdown(SHUT_RD/SHUT_WR)`; SHUT_WR emits a peer FIN, SHUT_RD is local-only. Does not work over cut-through transport (see limitation 11) | Unit state tests plus VPP integration (wake-parked-writer, local-EOF); peer-EOF test skipped under CT |
| Context dialing | TCP and connected UDP in both modes honor cancellation while resolving or connecting; async-connect completion is verified via `vppcom_session_get_error` after the poller wakes for `EPOLLOUT`\|`EPOLLERR`\|`EPOLLHUP`, so a successful handshake reliably returns a ready conn and never a spurious success from a stale EPOLLOUT. A `connectWait` background goroutine polls for session errors every 50 ms during connect, working around a VPP epoll delivery gap and surfacing connect failures in ~50–500 ms | Unit plus VPP integration; refused-loopback tests require a fast connect error |
| Happy Eyeballs | Unsuffixed `"tcp"` interleaves IPv6 and IPv4 attempts with a configurable stagger and closes successful losers | Localhost VPP integration plus helper tests |
| Deadlines | Resettable read and write deadlines wake operations already parked for readiness | Timer unit tests plus TCP and UDP VPP integration |
| Concurrent I/O | One session can retain separate read and write waiters | Readiness state-machine tests plus 6 MiB TCP integration |
| Listener cancellation | `TCPListener.AcceptContext` distinguishes context expiry from listener or package close | Unit plus VPP integration |
| Connected UDP | `Dial("udp*")`, read, write, and deadlines on IPv4 and IPv6 in both modes. Mode 2 routes all UDP operations through the owning worker and blocks connects until fully established to prevent VPP half-open cleanup crashes | VPP integration plus Mode 2 UDP tests |
| Unconnected UDP (PacketConn) | `ListenPacket("udp*")` with per-peer session adapter; `ReadFrom` receives from any peer, `WriteTo` routes to known peers (those that have sent data). Both modes | Unit tests plus VPP integration (echo round-trip with 3 messages, Mode 2 ListenPacket) |
| HTTP and layered TLS | HTTP/1.1, HTTP/2 (cleartext prior-knowledge and TLS-with-ALPN), and standard `crypto/tls` over vclnet TCP | VPP integration (HTTP/1.1, HTTP/2 GET/POST + concurrent streams, HTTP/2 over ALPN-negotiated TLS) |
| gRPC | Unary and server-streaming RPCs run over both cleartext and TLS transports on top of `vclnet.Listen` / `vclnet.DialContext`. Uses stock `grpc-go` with a `WithContextDialer` shim | VPP integration (`grpc-go` Health service, `Check` + `Watch`) |
| Native VCL TLS | `DialTLS` / `ListenTLS` route TLS termination into VPP via `VPPCOM_PROTO_TLS` (OpenSSL engine, `vppcom_add_cert_key_pair` + `SET_CKPAIR`). No `crypto/tls` on the caller side | Unit config + VPP integration (echo, 128 KiB fragmentation, layered/native parity) |
| Shutdown | Idempotent, tracks live listeners/conns/PacketConns/dials, drains listeners first, waits up to 5 s for in-flight I/O to finish, then force-closes stragglers and destroys the VCL app | Unit lifecycle registry + subprocess VPP concurrent-Shutdown stress |
| VLS Mode 3 | Shared VCL worker with one persistent readiness poller | Default; standard and multi-VPP-worker harnesses |
| VLS Mode 2 | N pinned VCL workers, per-worker epoll, virtual process-wide handles, ownership preflight, per-worker sharded listeners with accept fan-in, and no shared poller; TCP and UDP | Opt-in and experimental; unit and standard integration tests pass, but the four-VPP-worker Mode 2 suite currently fails under concurrent accept load |

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
change.

### Mode 2 stability assessment

The Mode 2 concurrency core has explicit, testable invariants: workers remain
pinned for their lifetime, raw VLS handles never cross owners, process-wide
virtual handles disambiguate worker-local indexes, listeners are sharded across
all workers with per-worker accept loops, and each worker owns its readiness
state. Unit tests, the standard Mode 2 harness, repeated shutdown, and several
multi-worker cases validate those invariants. They do not make the sharded
listener path safe under concurrent accept load.

That evidence does **not** make Mode 2 production-stable yet. Remaining
compatibility gaps:

- **Sharded-accept SIGSEGV (P0, unresolved):** the 2026-07-21 four-worker Mode
  2 run failed in `TestMultiWorkerHTTPConcurrent`. The VCL application process
  received `SIGSEGV` inside `vls_accept`/`vclpoll_accept_nb_full` (fault address
  `0x14`), and VPP subsequently logged an out-of-order control-ring element.
  The immediate chain is a null session dereference in VCL's failed-accept
  reply after an `ACCEPTED` event references an unavailable FIFO segment; the
  leading preceding trigger is cross-VPP-thread ordering of `APP_ADD_SEGMENT`
  and `ACCEPTED`. See
  [the detailed investigation](docs/mode2_accept_mq_investigation.md).
- **VPP API dependency (P0):** deterministic pinned-worker retirement calls
  `vls_unregister_vcl_worker`. The adjacent patched VPP build exports it and
  repeated-shutdown tests pass, but the API is not yet an established stock
  VPP release contract. All current vclnet binaries link to this symbol, even
  when only Mode 3 is selected.

### Validation snapshot (2026-07-21)

Against the local VPP release build (`v26.10-rc0~231-g0a143dac6`):

| Check | Result |
| --- | --- |
| `go test -count=1 ./...` | Pass |
| `go test -race -count=1 ./...` | Pass |
| `go vet ./...`, `go build ./...`, formatting and shell checks | Pass |
| `test/run_integration.sh` (Mode 3) | Pass; expected cut-through half-close skip |
| `test/run_integration.sh --mode 2` | Pass; expected cut-through half-close skip |
| `test/run_multiworker.sh --mode 3 4` | Pass |
| `test/run_multiworker.sh --mode 2 4` | **Fail:** concurrent accept/VCL SIGSEGV and MQ-order warning |

## 2. Test inventory

The repository currently has 175 top-level no-VPP tests:

- 135 public-package contract and unit tests (including native VCL TLS
  contract tests, PacketConn per-peer adapter tests, and connected UDP
  error-handling tests);
- 8 lifecycle registry tests (add/remove, wait-drain wake-up, timeout,
  concurrent add/remove, and snapshot stability);
- 9 shared Mode 3 poller tests;
- 13 Mode 2 worker, ownership, parking, UDP, and shutdown tests
  (including the worker-retirement terminal-state tests that replaced the
  removed `/proc/self/task`-polling test);
- 7 sharded listener tests (per-worker creation, accept fan-in, context
  cancellation, close/drain, lookup disambiguation, and blocking semantics);
- 3 byte-order and errno helper tests.

VPP-backed coverage currently has:

- 40 runnable public-package tests in the standard integration harness
  (including native VCL TLS, half-close, layered-TLS, deadline,
  Happy Eyeballs, shutdown, concurrent-shutdown stress, PacketConn echo,
  connection-refused and TLS-refused cases, address tests, HTTP/2
  cleartext + TLS-with-ALPN, and gRPC cleartext + TLS);
- 1 deliberately skipped test (half-close over cut-through transport);
- 2 low-level vclpoll echo tests;
- 5 multi-worker stress tests, 1 sharded-accept scaling test, 1 Mode 2
  ownership test, 1 Mode 2 UDP ListenPacket test, plus 1 Mode 2 repeated-shutdown
  stress test (20 independent worker-pool start/stop cycles against one VPP
  instance);
- 2 opt-in benchmarks.

The standard harness exercises TCP IPv4 and IPv6, connected UDP IPv4 and IPv6,
HTTP/1.1, HTTP/2 (cleartext and TLS-with-ALPN), gRPC (cleartext and TLS,
unary and server-streaming), layered TLS, native VCL TLS (short and 128 KiB
fragmented echo plus a native-vs-layered parity test), Happy Eyeballs,
context-aware accept, deadline expiry and updates, close-unblock behavior,
simultaneous blocked read and write, PacketConn echo via per-peer session
adapter, address reporting, shutdown, and TCP half-close (both
`CloseWrite` peer-EOF and `CloseRead` local-EOF paths, plus parked-writer
wake-up).

Commands:

```bash
make test
make race
make vet
make build

sudo -E bash test/run_integration.sh
sudo -E bash test/run_integration.sh --mode 2
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
| P0 | Fix Mode 2 accepted-session segment ordering and the failed-accept crash | Make VCL's negative accepted reply null-safe so it cannot abandon a reserved control-ring element; enforce `APP_ADD_SEGMENT` publication before any dependent `ACCEPTED` event across VPP worker threads; add a deterministic segment-growth/order regression; run concurrent HTTP and raw accept tests repeatedly without an app signal, an MQ-order warning, or a VPP failure. See [the investigation](docs/mode2_accept_mq_investigation.md) |
| P0 | Land or explicitly carry the VPP worker-unregister API | Upstream (or pin as a documented downstream patch) `vls_unregister_vcl_worker`, which clears the pthread destructor key and performs normal VLS worker cleanup on the owning thread; verify the required symbol during build/package validation; keep repeated Mode 2 shutdown green. The local adjacent VPP review commit `032b24d04` supplies the API, but stock-version compatibility is not yet established |
| P0 | Automated compatibility CI | Run unit, race, vet, build, standard VPP integration, Mode 3 multi-worker, and Mode 2 multi-worker jobs against a documented VPP version matrix; retain logs and fail on unexpected skips or a VPP crash |
| P0 | Complete Mode 2 rollout validation | Run the full supported TCP and UDP integration surface, repeated shutdown cases, a long concurrency soak, and no-migration assertions in CI; keep Mode 3 as default until the sustained-green and performance gates pass |
| P1 | Establish reproducible performance baselines | Record topology, hardware, VPP and kernel configs, payload and concurrency distributions, raw benchmark output, and comparisons before publishing speedup claims |
| P2 | Extended native TLS controls | Reach the rest of VPP's `TRANSPORT_ENDPT_EXT_CFG_CRYPTO` surface — SNI, ALPN, `verify_cfg`, `ca_trust_index`, `tls_profile_index` — via `VPPCOM_ATTR_SET_ENDPT_EXT_CFG`, and expose them on `TLSConfig` |
| P2 | UDP edge semantics | Decide port-zero listeners, zero-length datagrams, truncation, connected `WriteTo`, multicast and broadcast, and source-address behavior |

## 4. Known limitations

1. **Unconnected UDP uses a per-peer session model.** `ListenPacket` returns a
   `PacketConn` backed by VPP's session-oriented UDP. `ReadFrom` works for any
   peer that sends data; `WriteTo` only reaches peers already seen (VPP cannot
   originate a session to an arbitrary address from a listener). For sending to
   new addresses, use connected UDP via `Dial("udp", addr)`. Both modes.
2. **Mode 2 UDP connects are blocking.** Mode 2 UDP connects block on the
   worker thread until fully established to prevent a VPP half-open session
   cleanup race. This is safe but means connect latency is slightly higher than
   Mode 3's async path. The underlying VPP bug (stale RPC accessing a freed
   pool slot in `session_half_open_cleanup_notify_rpc`) remains unfixed
   upstream.
3. **Mode 3 is still the default.** It is the broadest-tested compatibility
   path, but application-side VLS work serializes on one shared worker.
4. **Mode 2 is opt-in.** It requires `multi-thread-workers` and permanently pins
   one OS thread per requested worker. Listeners are sharded across all workers
   with per-worker accept loops and a fan-in channel.
5. **Mode 2 uses virtual handles internally.** Raw VLS handles are worker-local
   pool indexes and can collide, so vclpoll maps process-unique handles to an
   owning worker and raw handle. They never escape the internal package.
6. **Mode 2 sharded accept can crash the VCL application under concurrent
   load (unresolved).** The application faults in the VCL accept error path;
   the interrupted accepted reply leaves the app-to-VPP MQ ring inconsistent,
   after which VPP reports an out-of-order element. Mode 2 concurrent
   listeners are not production-safe. See
   [the accept/MQ investigation](docs/mode2_accept_mq_investigation.md).
7. **The current build requires a patched VPP API.**
   `internal/vclpoll` links directly to `vls_unregister_vcl_worker`. The local
   VPP review build supplies it; a stock VPP build without that export cannot
   link vclnet. Upstream or carry and pin the patch before distribution.
8. **Mode 2 teardown is process-final.** The bootstrap OS thread is parked
   after `vppcom_app_destroy` because the pinned VPP VLS destructor is unsafe
   after global VLS state is destroyed. Reinitialization after Shutdown is not
   supported in either mode.
9. **Client and server need separate VCL apps.** The integration topology uses
   subprocesses because one VCL app cannot connect back to its own listener.
10. **The local patched VPP release build is the validated target.** The
    harness treats an application or VPP process crash as a test failure. Mode
    2 UDP connects are blocking to work around a VPP half-open cleanup race
    that remains unfixed upstream.
11. **TCP half-close does not work over cut-through transport.** When both
    endpoints connect through the same VPP with `app-scope-local`, VPP selects
    its cut-through (CT) protocol, which does not implement `half_close`.
    `CloseWrite` is a no-op at the VPP level and the peer never observes EOF.
    Half-close works correctly over the full TCP transport (separate VPP
    instances or without `app-scope-local`).
12. **No comparative benchmark is shipped.** Benchmark functions are test tools,
    not evidence for a specific speedup.
13. **VPP drops the epoll event for refused loopback connects.** Connecting to
    an unused loopback port with `app-scope-local` set produces a VPP-side
    `SESSION_E_NOROUTE` counter increment without the `SESSION_CTRL_EVT_CONNECTED`-
    with-error event reaching the app's epoll. The `connectWait` workaround
    (background goroutine polling `vppcom_session_get_error` every 50 ms)
    detects the error and wakes the waiter, surfacing connect failures in
    ~50–500 ms. The underlying VPP bug remains — an upstream fix would
    eliminate the polling overhead. Full investigation in
    [docs/connect_error_investigation.md](docs/connect_error_investigation.md).

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
          |-- per-worker sharded listeners with accept fan-in
          |-- all admitted operations run on the owner
          `-- UDP connects block until established (no half-open handles)
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
| Listener behavior | Shared-mode listener | Per-worker sharded listeners with SO_REUSEPORT and accept fan-in |
| Protocol surface | TCP and connected UDP | TCP and connected UDP (blocking connects) |
| Configuration | No `multi-thread-workers` token | Requires `multi-thread-workers` |
| Status | Default and broadly tested | Experimental, opt-in, rollout validation pending |

Running VPP with `cpu { workers N }` is separate from selecting a VLS mode.
The multi-worker harness accepts `--mode 3` and `--mode 2` so both dimensions
are explicit.

## 7. Error and lifecycle behavior

VCL negative return values become `VCLError` values containing
`syscall.Errno`. Public operations wrap them in `*net.OpError`, preserving
`errors.Is` checks such as `ECONNREFUSED`, `ECONNRESET`, and `ETIMEDOUT`.

`Shutdown` (and its explicit-timeout form `ShutdownWithTimeout(d)`):

1. marks the package closed so new public operations fail;
2. closes every tracked listener (stops accepting new work at the process
   boundary and wakes blocked `AcceptContext` callers);
3. waits up to the drain window (5 s by default) for tracked connections,
   PacketConns, and in-flight dials to finish naturally;
4. force-closes any remaining tracked conns and PacketConns after the drain
   window elapses, so blocked reads/writes unpark with `ErrClosed`;
5. prevents parked operations from re-entering VLS;
6. stops the active dispatcher and wakes its exact waiters;
7. in Mode 2, closes sessions on their owners, drains worker MQ cleanup, and
   waits for non-bootstrap worker threads to exit;
8. calls `vppcom_app_destroy` only after the active readiness machinery has
   stopped.

`liveRegistry` (internal, `lifecycle.go`) is the tracking mechanism. Every
`Listen`, `ListenTLS`, `ListenPacket`, `AcceptContext`, `Dial`, and
`DialTLSContext` call registers its result before returning; each object's
`Close` unregisters. In-flight dials are counted independently so Shutdown
does not race a connect that has completed the VLS work but not yet handed
the conn back to the caller.

Shutdown is idempotent and process-final. Services should still stop
admitting new work at the application layer (drain HTTP handlers, refuse new
RPCs) before calling Shutdown; the drain window catches whatever is already
in flight.
