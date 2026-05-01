# Asynchronous connect error surfacing — investigation notes

## 1. Scope and outcome

This document is the audit trail for the completed historical P1 item
**"Verify asynchronous connect completion errors."** It captures the code
paths, VPP internals, tests, symptoms, and hypotheses that came out of the
investigation. Current open priorities are maintained in
[../summary.md](../summary.md#3-pending-work).

- **Client-side plumbing** — landed. Dial paths (TCP, UDP, native VCL TLS)
  now wait on the union of `EPOLLOUT | EPOLLERR | EPOLLHUP` and call
  `vppcom_session_get_error` (via `vclpoll.SessionConnectError`) before
  handing a `net.Conn` back to the caller. A stale EPOLLOUT can no longer
  yield a spurious success.
- **VPP-side signal delivery** — partial. On the pinned VPP 26.10 build,
  a refused loopback connect increments the session error counter but the
  `SESSION_CTRL_EVT_CONNECTED`-with-error event does not wake the app's
  epoll waiter.
- **Workaround** — landed. The `connectWait` helper in `dialer.go` spawns
  a background goroutine that polls `vppcom_session_get_error` every 50 ms
  while the main waiter is parked in `PollWaitContext`. When VPP's
  `vcl_session_connected_handler` sets the session's `vpp_error` field (which
  it does even when the epoll event is dropped), the goroutine detects it
  and calls `WakeVLSH` to unblock the main waiter. Additionally, if
  `PollWaitContext` returns false (context expired), a final
  `SessionConnectError` check catches errors recorded after registration.
  This reduces connect-failure latency from the full context timeout (3–15 s)
  to ~50–500 ms (roughly 50–80 ms in the 2026-07-21 local audit).
  `TestTCPDialConnectionRefused` and
  `TestTCPDialTLSConnectionRefused` now require a fast connect error and
  treat a context timeout as a test failure.

The rest of this file records the investigation in enough detail that a
future audit (or an upstream VPP bug filing) can pick it up cold.

## 2. Baseline understanding

### 2.1 Previous behaviour (before this work)

`dialer.go`'s `dialSingleTCP` did:

```go
vlsh, immediate, err := connectStart(addr)   // vls_connect(non-blocking)
if !immediate {
    if !vclpoll.PollWaitContext(vlsh, EPOLLOUT, ctx.Done()) {
        // treated as failure only if context cancelled
    }
}
// return conn
```

The internal comment on the compatibility helper `mode3DialTCP4` documented
the risk:

> Note: VPP's `VPPCOM_ATTR_GET_ERROR` is a stub that always returns 0
> (memory file findings from frida-vpp), so we do not double-check
> connection success via SO_ERROR — EPOLLOUT is taken to mean connected,
> matching what LDP itself does in practice.

Practical consequence: if VPP ever raised EPOLLOUT without the connect
having actually succeeded, Dial returned a "connected" `net.Conn` and the
error surfaced only on the first `Read`/`Write` — as an opaque
`ECONNRESET` or similar. That's the ambiguity this P1 item aimed to fix.

### 2.2 VPP APIs surveyed

`src/vcl/vppcom.c` in the pinned VPP source tree (26.10) exposes two
error-inspection paths:

- `VPPCOM_ATTR_GET_ERROR` — reachable via `vls_attr`. Implementation at
  `vppcom.c:4135` unconditionally writes 0 into the caller's buffer.
  Labelled `#VPP-TBD#`. **Not useful.**
- `vppcom_session_get_error(uint32_t sh)` — direct C function at
  `vppcom.c:5173`. Inspects `vcl_session_t::vpp_error` and maps
  `SESSION_E_REFUSED` → `VPPCOM_ECONNREFUSED`, `SESSION_E_PORTINUSE` →
  `VPPCOM_EADDRINUSE`, any other non-zero session error →
  `VPPCOM_EFAULT`, `SESSION_E_NONE` → `VPPCOM_OK`.

`vppcom_session_get_error` takes a **vppcom session handle** (`sh`), not a
VLS handle (`vlsh`). Translation is via `vlsh_to_sh` (declared in
`src/vcl/vcl_locked.h`). No `vls_session_get_error` wrapper exists, so
vclnet composes them in a small C helper.

### 2.3 How VPP signals connect completion

VCL's `vppcom_session_connect` in non-blocking mode:

```c
vcl_send_session_connect(wrk, session);           // MQ → VPP
if (VCL_SESS_ATTR_NONBLOCK) {
    session->session_state = VCL_STATE_UPDATED;
    return VPPCOM_EINPROGRESS;
}
```

VPP then processes the CONNECT via `session_mq_connect_handler` →
`session_mq_connect_one` → `vnet_connect`. On success VPP allocates a
half-open session, TCP sends SYN, and when the handshake completes
VPP fires `app_worker_connect_notify` with `err = 0`. On failure
(SESSION_E_NOROUTE, SESSION_E_REFUSED, etc.) VPP fires the same call
with the corresponding `err`.

`app_worker_connect_notify` schedules a `SESSION_CTRL_EVT_CONNECTED`
event onto `app_wrk->wrk_evts[thread_index]`. `session_input_node` later
dispatches it, calling the app's `session_connected_callback` — for
external SAPI apps that is `mq_send_session_connected_cb`. This places
the message on the app's MQ ring (`SESSION_MQ_CTRL_EVT_RING`) and
signals the MQ eventfd via `svm_msg_q_add_raw` →
`svm_msg_q_send_signal`.

On the app side, `vppcom_epoll_wait_eventfd` polls `mqs_epfd`, sees the
eventfd fire, drains the MQ, and per event calls
`vcl_epoll_wait_handle_mq_event`. For `SESSION_CTRL_EVT_CONNECTED`:

```c
if (!e->postponed)
    sid = vcl_session_connected_handler(wrk, connected_msg);  // sets vpp_error
else
    sid = e->session_index;
s = vcl_session_get(wrk, sid);
if (vcl_session_is_closed(s) || !vcl_ep_session_needs_evt(s, EPOLLOUT))
    break;
// ...generate EPOLLOUT (+ EPOLLHUP if state == DETACHED)
```

`vcl_ep_session_needs_evt` requires:

1. `s->vep.ev.events & EPOLLOUT` — set at `vppcom_epoll_ctl(ADD)` time
   when the caller registered EPOLLOUT.
2. `s->vep.lt_next == VCL_INVALID_SESSION_INDEX` — level-triggered path
   not active for this session.

Both conditions are trivially satisfied by our shared poller.

## 3. Client-side changes

### 3.1 CGo helper

`internal/vclpoll/cgo.go`:

```c
static int vclpoll_session_get_connect_error(int vlsh) {
    vcl_session_handle_t sh = vlsh_to_sh((vls_handle_t)vlsh);
    if (sh == (vcl_session_handle_t)-1) {
        return -EBADF;
    }
    return vppcom_session_get_error((uint32_t)sh);
}
```

Wrapped in Go as `rawSessionConnectError(vlsh VLSH) error`. Return value:
`nil` if the session's `vpp_error == SESSION_E_NONE`; otherwise a
`*VCLError{Errno: syscall.Errno(-rv)}` where `-rv` is `ECONNREFUSED`,
`EADDRINUSE`, `EFAULT`, or `EBADFD` depending on VPP's mapping.

### 3.2 Dispatcher integration

Added `sessionConnectError(VLSH) error` to the `dispatcher` interface
(`internal/vclpoll/dispatch.go`).

- **Mode 3** — `mode3SessionConnectError` pins the calling thread with
  `pin()` and calls `rawSessionConnectError`.
- **Mode 2** — `(*mode2Dispatcher).sessionConnectError` submits the call
  through `sessionCall`, so the query runs on the owning worker's pinned
  OS thread. Ownership preflight is applied identically to reads/writes.

### 3.3 Dial paths

`dialer.go` defines a `connectWait` helper that all three dial paths
(`dialSingleTCP`, `dialUDP`, `DialTLSContext`) call when the connect is
non-blocking:

```go
const connectReadyEvents = 0x004 | 0x008 | 0x010 // EPOLLOUT | EPOLLERR | EPOLLHUP
const connectErrorPollInterval = 50 * time.Millisecond

func connectWait(vlsh vclpoll.VLSH, ctx context.Context) error {
    // Background goroutine polls SessionConnectError every 50ms.
    // When an error is detected, it calls WakeVLSH to unblock the
    // main PollWaitContext waiter.
    ...
    if ok := vclpoll.PollWaitContext(vlsh, connectReadyEvents, ctx.Done()); !ok {
        // Final check: VPP may have set vpp_error after we registered.
        if err := vclpoll.SessionConnectError(vlsh); err != nil {
            return err
        }
        return interruptedConnectError(ctx)
    }
    return vclpoll.SessionConnectError(vlsh)
}
```

Applied uniformly to `dialSingleTCP`, `dialUDP`, and `DialTLSContext`.
The compatibility helper `mode3DialTCP4` was left alone (used by low-level
integration tests only, not on any public path) with a comment noting
the reliable path is used by public dials.

### 3.4 WakeVLSH

`internal/vclpoll/dispatch.go` exports `WakeVLSH(vlsh VLSH)`. It unblocks
any goroutine parked in `PollWaitContext` on the given vlsh without closing
the session. Mode 3 delegates to `pollUnregister`; Mode 2 submits a wake
to the owning worker via `removeWaiters`.

### 3.5 Public API

New exported function `vclpoll.SessionConnectError(vlsh VLSH) error`
provides the same guarantee for any caller that manages its own split
connect. The vclnet package doesn't re-export it — it is an internal
plumbing primitive.

## 4. Tests written

### 4.1 Positive path (already covered)

`TestTCPIPv4EchoSingle` and every other `Dial → Read/Write` integration
test exercises `SessionConnectError` on the happy path: VPP fires
CONNECTED with `err = 0`, `vpp_error` stays `SESSION_E_NONE`, our query
returns nil, Dial hands back a working conn. All still pass.

### 4.2 Refused path (new)

`TestTCPDialConnectionRefused` — dials `127.0.0.1:$reserved_port` with no
listener present. `TestTCPDialTLSConnectionRefused` — same via
`DialTLSContext`. Both:

- Fail on **regression**: if Dial returns `err == nil`, the tests
  `t.Fatalf` — the invariant "no spurious success" is enforced.
- Fail on **timeout**: if `context.DeadlineExceeded` is returned, the
  tests `t.Fatalf` — the `connectWait` background polling should always
  detect the error before the 3 s context expires.
- Warn on **slow detection**: if the error takes longer than 2 s, the
  tests `t.Errorf` — the background polling may not be working correctly.
- Pass on any surfaced connect error (`ECONNREFUSED`, wrapped `EFAULT`,
  etc.) within the expected latency window.

## 5. Symptom

Initial test runs against the standard integration harness
(`sudo -E bash test/run_integration.sh`):

```text
=== RUN   TestTCPDialConnectionRefused
    integration_test.go:2593: Dial refused-port error=dial tcp4 127.0.0.1:40026:
        context deadline exceeded, want ECONNREFUSED
--- FAIL: TestTCPDialConnectionRefused (5.00s)
=== RUN   TestTCPDialTLSConnectionRefused
    integration_test.go:2620: DialTLS returned timeout instead of connect error:
        dial tcp4 127.0.0.1:40027: context deadline exceeded
--- FAIL: TestTCPDialTLSConnectionRefused (5.00s)
```

The test spent the entire 5 s context window waiting on `PollWaitContext`
and returned `context.DeadlineExceeded`. Increasing the timeout to 15 s
did not change the outcome — the readiness dispatcher never woke.

## 6. Root cause hypothesis

### 6.1 VPP does receive and process the connect

The definitive datapoint comes from `vppctl show session stats` after a
failed dial:

```text
=== Before test ===
Thread 0: no sessions
=== After test ===
Thread 0: no sessions
=== Session errors ===
Thread 0:
 1 no route
```

VPP processed the connect, resolved it, and hit `SESSION_E_NOROUTE`.

### 6.2 Where SESSION_E_NOROUTE comes from

In `src/vnet/session/application_local.c`:

```c
ct_session_connect (transport_endpoint_cfg_t * tep) {
    ...
    // Local scope: look up in application_local_session_table.
    lh = session_lookup_local_endpoint (table_index, sep);
    if (lh == SESSION_INVALID_HANDLE)
        goto global_scope;
    ...

global_scope:
    if (session_endpoint_is_local (sep))
        return SESSION_E_NOROUTE;             // <-- our path
    ...
}
```

`session_endpoint_is_local` is defined as:

```c
return (ip_is_zero (&sep->ip, sep->is_ip4)
        || ip_is_local_host (&sep->ip, sep->is_ip4));
```

`127.0.0.1` matches `ip_is_local_host`. VPP short-circuits before ever
allocating a half-open session or sending a SYN. `vnet_connect` returns
`SESSION_E_NOROUTE` to `session_mq_connect_one`, which then:

```c
if ((rv = vnet_connect (a))) {
    session_worker_stat_error_inc (wrk, rv, 1);     // <-- counted here
    app_wrk = application_get_worker (app, mp->wrk_index);
    app_worker_connect_notify (app_wrk, 0, rv, mp->context);
}
```

Both the counter increment and the notify call are present. The counter
we observed. The notify — apparently — is what does not reach the app.

### 6.3 Why the notify does not wake our waiter (working theory)

The `SESSION_CTRL_EVT_CONNECTED` event flows through two layers:

1. **`app_worker_connect_notify`** stores the event on
   `app_wrk->wrk_evts[thread_index]` and schedules `session_input_node`.
2. **`session_input_node`** dispatches it via
   `session_connected_callback` → `mq_send_session_connected_cb`, which
   places the message on the SAPI MQ and signals the MQ eventfd.

The failure could sit at any of:

- **Thread-index mismatch in step 1.** When `s == NULL` (error case),
  `app_worker_connect_notify` sets `thread_index = vlib_get_thread_index()`
  — the current thread of the connect RPC handler
  (`transport_cl_thread()`, typically thread 0 with no CPU workers).
  `session_wrk_program_app_wrk_evts` asserts that `thread_index` matches
  the current worker; the assertion is a no-op in release builds. If the
  scheduling assumption breaks in a debug build, the event never
  propagates.
- **Session-input-node interrupt not firing.** `session_wrk_program_app_wrk_evts`
  calls `vlib_node_set_interrupt_pending` only when the app-worker was
  not already in the pending bitmap. If the pending bitmap contains
  stale state from a previous run (or if the interrupt handler is not
  currently running for some reason), the scheduled event is queued but
  never dispatched.
- **`vcl_ep_session_needs_evt (s, EPOLLOUT)` returning false.** The
  postponed CONNECTED event's `sid` is `e->session_index`. After
  `vcl_session_connected_handler` set `vpp_handle = INVALID_HANDLE`, the
  session lookup might resolve to a different session, or `s->vep`'s
  event mask might be zeroed out by the CONNECTED handler before the
  postponed path re-reads it. This is a code path we cannot easily
  instrument without a debug VPP build.

We did **not** conclusively identify which of these applies on the pinned
VPP 26.10 build.

### 6.4 Why the client-side is nonetheless correct

Independent of the VPP-side delivery gap:

- If VPP DOES fire the event correctly (real-NIC RST, some other VPP
  build), our poller wakes, `SessionConnectError` returns a wrapped
  errno, Dial fails cleanly.
- If VPP fires a spurious EPOLLOUT (the historical worry — what if a
  transport signalled write-ready before the handshake completed?),
  `SessionConnectError` still runs; `vpp_error` would still be
  `SESSION_E_NONE` (no error posted), so we'd return the conn. That's
  the same as the old code, and matches VPP's semantics: if there's no
  session error, the connect is complete.
- If VPP fires a real EPOLLOUT after a real CONNECTED success,
  `SessionConnectError` returns nil, Dial returns the conn.

The failure mode "session_endpoint_is_local + no listener never wakes
epoll" is orthogonal to the connect-error-query design. It affects
error-signalling latency (client hangs to the context deadline) but not
correctness.

## 7. Resolution

The `connectWait` workaround (§3.3–3.4) resolves this for all practical
purposes. The background polling goroutine detects VPP's session error
within one poll interval (~50 ms) and wakes the blocked waiter. Tests now
require a fast connect error and treat timeout as a failure.

Remaining optional follow-ups:

- **Upstream VPP fix.** File a bug with the loopback reproducer so VPP's
  epoll path correctly delivers the `SESSION_CTRL_EVT_CONNECTED`-with-error
  event. This would eliminate the need for the background polling goroutine
  (which burns a small amount of CPU during connect).
- **Wire-side topology.** Validate that a real NIC or veth-pair topology
  delivers the event without the polling workaround, confirming the bug is
  specific to the `session_endpoint_is_local` short-circuit path.

## 8. Artifacts

- **Source code**
  - `internal/vclpoll/cgo.go` — C helper `vclpoll_session_get_connect_error`
    and Go wrapper `rawSessionConnectError`, `mode3SessionConnectError`.
  - `internal/vclpoll/dispatch.go` — added `sessionConnectError` to the
    `dispatcher` interface, exported `SessionConnectError` and `WakeVLSH`.
  - `internal/vclpoll/mode2.go` — Mode 2 dispatcher methods for
    `sessionConnectError` (via `sessionCall`) and `wakeVLSH` (via `submit`).
  - `dialer.go` — `connectWait` helper with background polling goroutine;
    all three dial paths (`dialSingleTCP`, `dialUDP`, `DialTLSContext`)
    refactored to use it.
  - `tls.go` — `DialTLSContext` uses `connectWait`.
- **Tests**
  - `integration_test.go`:
    - `TestTCPDialConnectionRefused` (new)
    - `TestTCPDialTLSConnectionRefused` (new)
- **Documentation**
  - `summary.md` — current behavior and the refused-loopback known limitation.
  - `docs/architecture.md` — §7 rewritten to reflect the new sequence
    and document the loopback gap.
  - `docs/vclnet_deep_dive.md` — §12.2 updated.
  - `docs/executive_report.md` — risk table row and evidence list.
  - This document.

## 9. Reproducing the diagnostic

```bash
# Start VPP with the same config the tests use, plus a loopback interface.
sudo -n bash -c '
    mkdir -p /tmp/vclnet-test/app_ns_sockets /tmp/vclnet-share
    cat > /tmp/vclnet-share/vcl.conf <<CONF
vcl {
  rx-fifo-size 4000000
  tx-fifo-size 4000000
  app-scope-local
  app-scope-global
  use-mq-eventfd
  app-socket-api /tmp/vclnet-test/app_ns_sockets/default
}
CONF
    /path/to/vpp \
      unix { nodaemon log /tmp/vpp.log cli-listen /tmp/vclnet-test/cli.sock \
             runtime-dir /tmp/vclnet-test } \
      session { enable use-app-socket-api } &
'

# Configure loopback and run one failing test.
/path/to/vppctl -s /tmp/vclnet-test/cli.sock create loopback interface
/path/to/vppctl -s /tmp/vclnet-test/cli.sock set interface state loop0 up
/path/to/vppctl -s /tmp/vclnet-test/cli.sock \
    set interface ip address loop0 127.0.0.1/8
/path/to/vppctl -s /tmp/vclnet-test/cli.sock show session stats     # baseline

VCL_CONFIG=/tmp/vclnet-share/vcl.conf \
    go test -v -count=1 -timeout 15s -run '^TestTCPDialConnectionRefused$' .

/path/to/vppctl -s /tmp/vclnet-test/cli.sock show session stats     # observe increment
```

Expected observations (after `connectWait` fix):

- Test surfaces a connect error within ~50–500 ms (well under the context
  deadline).
- `show session stats` afterward reports `1 no route` (or more, if the
  test was run repeatedly) on Thread 0.
- `show session verbose` reports `no sessions` — VPP tore down its side
  of the state after `SESSION_E_NOROUTE`.
