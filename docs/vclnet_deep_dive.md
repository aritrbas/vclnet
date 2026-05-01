# VCLNET Deep-Dive Report

> **Goal of the project (one line):** *Give Go applications cut-through, kernel-bypass networking through VPP, with a drop-in `net.Conn` / `net.Listener` API.*

This document is a single-source technical report covering:

1.  Why this work matters at all (the Go ↔ VPP impedance mismatch)
2.  The VPP session layer and FIFO management (data plane)
3.  The VPP application/worker model and the application socket API (control plane)
4.  How VCL (per-thread) and VLS (locked) work — multi-threaded VCL in detail
5.  Why goroutines break VCL by default
6.  How vclnet bridges the gap (CGo + `LockOSThread` + worker registry)
7.  Three concrete deep-dive questions about goroutines, file descriptors, and cut-through
8.  Why those questions are *the* important questions
9.  Current status, limitations, and roadmap

---

## Table of Contents

1.  [Executive Summary — Why This Work Matters](#1-executive-summary--why-this-work-matters)
2.  [The Go ↔ VPP Impedance Mismatch](#2-the-go--vpp-impedance-mismatch)
3.  [VPP Session Layer: Architecture](#3-vpp-session-layer-architecture)
4.  [SVM FIFOs and the Shared-Memory Data Plane](#4-svm-fifos-and-the-shared-memory-data-plane)
5.  [Application, App-Worker, and the App-Socket API](#5-application-app-worker-and-the-app-socket-api)
6.  [Cut-Through Sessions in Depth](#6-cut-through-sessions-in-depth)
7.  [VCL — The Per-Thread Client Library](#7-vcl--the-per-thread-client-library)
8.  [VLS — VCL Locked Sessions and Multi-Thread Modes](#8-vls--vcl-locked-sessions-and-multi-thread-modes)
9.  [Why Goroutines Are Hostile to VCL by Default](#9-why-goroutines-are-hostile-to-vcl-by-default)
10. [VCLNET Architecture](#10-vclnet-architecture)
11. [The `pin()` Pattern and the Worker Registry](#11-the-pin-pattern-and-the-worker-registry)
12. [Non-Blocking I/O and the Shared Poller](#12-non-blocking-io-and-the-shared-poller)
13. [Earlier Attempts: Why Frida-Based Approaches Failed](#13-earlier-attempts-why-frida-based-approaches-failed)
14. [Deep-Dive Q&A: Goroutines, FDs, and Cut-Through](#14-deep-dive-qa-goroutines-fds-and-cut-through)
15. [Known Bugs, Workarounds, and the VPP Debug-Build Race](#15-known-bugs-workarounds-and-the-vpp-debug-build-race)
16. [Current Status and Pending Work](#16-current-status-and-pending-work)
17. [Appendix A: vcl.conf Tokens and Their Effects](#appendix-a-vclconf-tokens-and-their-effects)
18. [Appendix B: Key Source Locations (Cross-Reference)](#appendix-b-key-source-locations-cross-reference)

---

## 1. Executive Summary — Why This Work Matters

VPP (Vector Packet Processing) is a userspace networking stack that processes packets at line-rate (Mpps) by bypassing the kernel. To make application-level code (TCP, UDP, TLS, QUIC, HTTP) usable on top of VPP, the project exposes a **session layer** plus a client library called **VCL** (VPP Communications Library). Applications written in C can use VCL directly, or transparently via the **LD_PRELOAD shim** `libvcl_ldpreload.so` that intercepts `libc`'s `socket()`, `read()`, `write()`, `epoll_*`, etc.

**Go programs cannot use either of those mechanisms.** The Go runtime issues raw `SYSCALL` instructions for its network calls (the runtime poller is built directly on the kernel's `epoll`), so `LD_PRELOAD` never fires. And direct VCL use from Go is hard because VCL keeps **per-pthread state in `__thread` storage**, while Go's M:N scheduler freely moves goroutines between OS threads.

**Why the world wants this:**

| Capability VPP gives you | What it means for Go apps |
| --- | --- |
| Kernel-bypass TCP / UDP / TLS / QUIC | µs-class latency, no syscall overhead, no TCP context-switch cost |
| **Cut-through sessions** (app-to-app on the same host) | Service mesh / sidecar pattern with **no TCP at all** — pure SHM FIFO memcpy between processes |
| Multi-NIC / multi-queue scaling via DPDK | Wire-speed for a Go HTTP server without per-conn kernel scheduler involvement |
| One shared NIC across many tenant apps | Multi-tenant userspace dataplane (Calico-VPP, Envoy-on-VPP, etc.) |

`vclnet` provides a viable explicit path for Go code that can accept a custom `net.Conn` or `net.Listener`. HTTP/1.1 is tested; HTTP/2 and current gRPC behavior still require dedicated integration coverage. The drop-in API:

```go
ln, _ := vclnet.Listen("tcp", ":8080")
http.Serve(ln, mux) // standard net/http, transported by VPP
```

Two previous attempts (`frida-vpp` and `go-frida-vpp`) tried to intercept Go's syscalls via Frida-injected JavaScript. Both worked for one connection and failed structurally under concurrency. `vclnet` is the engineered replacement that uses CGo + `runtime.LockOSThread` to honour VCL's threading contract.

**Why this matters for cut-through specifically:** With `app-scope-local` enabled in `vcl.conf`, two VLS apps on the same VPP that connect to each other get a **cut-through transport** — their sessions share SVM FIFOs directly and the data path becomes a pure shared-memory memcpy. For a Go service mesh or Go-based sidecar talking to a co-located Go workload, this is **the** performance unlock. Without vclnet (or something morally equivalent), Go apps cannot access that path at all.

---

## 2. The Go ↔ VPP Impedance Mismatch

There are four concrete impedance mismatches between Go and VCL:

### 2.1 LD_PRELOAD doesn't reach Go

```text
+-----------------------------------------------------------------+
|  C app:                                                         |
|     socket() -> libc -> [LD_PRELOAD vcl_ldpreload] -> vcl_*    OK
|                                                                 |
|  Go app:                                                        |
|     net.Dial -> syscall.Connect -> SYSCALL instruction          |
|                  (Go ASM, never goes through libc)              |
|     => kernel handles it directly                              KO
|                                                                 |
+-----------------------------------------------------------------+
```

The Go runtime *deliberately* bypasses libc because it implements its own preemptive M:N scheduler that needs to control exactly when each goroutine releases its OS thread back to the scheduler. It cannot tolerate libc's hidden allocations, locks, or thread-local state.

### 2.2 The Go runtime owns "FDs"

Go's `net.Conn` is layered on `netFD`, which is registered with `runtime.pollDesc` and the kernel's `epoll`. A "file descriptor" in Go is not just an integer — it is an object whose **readiness is reported via kernel epoll** and whose `Read`/`Write` is driven by the runtime poller. There is no public way to plug an alternative readiness source.

VCL's `vlsh` is **not a kernel file descriptor.** It is an opaque integer that indexes into `vcm->workers[wrk_idx].sessions[]`. The Linux kernel does not know it exists. If `vlsh` ever leaks into a Go FD-typed parameter (`os.NewFile`, `syscall.Read`, anything), the kernel returns `EBADF` and the app crashes.

### 2.3 VCL keeps per-pthread state

```c
// src/vcl/vcl_private.h
extern __thread uword __vcl_worker_index;
```

`__thread` is GCC's thread-local storage qualifier. Each pthread has its own copy. Every `vppcom_*` / `vls_*` entry point reads `__vcl_worker_index` to find:

* The per-worker session pool (`vcm->workers[wrk_idx].sessions`).
* The per-worker message queue (MQ) pair that talks to VPP.
* The per-worker epoll table.
* The per-worker hash maps (`sh -> vlsh`, `vpp_handle -> session_index`).

If a goroutine starts a VCL call on M1 and is rescheduled on M2 mid-call, the second half of the call reads a *different* `__vcl_worker_index`, and the world ends.

### 2.4 Goroutines and Ms come and go

Go's M:N scheduler:

* Routinely moves goroutines between Ms (work stealing).
* Spawns new Ms when an existing M blocks in cgo or a syscall (`newm` / `handoffp`).
* Destroys idle Ms after some delay.

When an M dies, glibc runs its `pthread_key_create` destructors, including the one VLS installed in `vls_app_create`:

```c
// src/vcl/vcl_locked.c
if (pthread_key_create (&vls_mt_pthread_stop_key, vls_mt_del))
    return -1;
```

`vls_mt_del` calls `vppcom_worker_unregister()` — **it deletes the VCL worker that thread was using, including its MQ pair with VPP.** If you let the Go runtime destroy Ms freely, VCL workers blink in and out of existence, sessions get orphaned, and VPP itself sees worker add/del churn nobody designed for.

---

## 3. VPP Session Layer: Architecture

```text
                       +----------------------------+
                       |   App (vclnet Go process)  |
                       +-------------+--------------+
                                     |
                       vls_*  /  vppcom_*  (CGo from Go)
                                     |
                       +-------------v--------------+
                       |     libvppcom.so (VCL)     |
                       |   VCL workers + sessions   |
                       +-------------+--------------+
                                     |  shared memory (FIFO segments)
                                     |  + per-app-worker MQ
                       +-------------v--------------+
                       |        vpp_main process    |
                       |                            |
                       |   session_main (per-wrk)   |
                       |        |                   |
                       |   +----v-----+ +--------+  |
                       |   |  app_wrk | | xport  |  |
                       |   |   table  | | TCP/CT |  |
                       |   +----+-----+ +---+----+  |
                       |        |           |       |
                       |        v           v       |
                       |   svm_fifo_t   nic/dpdk    |
                       +----------------------------+
```

### 3.1 Core types

| Type | Lives in | Owns |
| --- | --- | --- |
| `session_t` | `vpp_main`'s `session_worker_t.sessions` pool | rx/tx `svm_fifo_t *`, transport index, app_wrk index, state machine |
| `transport_connection_t` (TCP, UDP, **CT**, QUIC…) | per-transport, per-worker pool | protocol-specific state |
| `application_t` | global pool | App identity, namespace, properties, callbacks |
| `app_worker_t` | per-app | Per-worker MQ to that app, segment manager indices |
| `app_listener_t` | per-app | Listen session(s), accept-rotor across workers |
| `segment_manager_t` / `fifo_segment_t` | per-app-worker | The shared memory regions used for FIFOs |

`session_worker_t` is **per VPP worker thread**:

```c
// src/vnet/session/session.h
typedef struct session_worker_ {
    CLIB_CACHE_LINE_ALIGN_MARK (cacheline0);
    session_t *sessions;             // Per-worker session pool
    svm_msg_q_t *vpp_event_queue;    // MQ for this worker
    ...
} session_worker_t;
```

So **every session in VPP is owned by exactly one worker thread**. Cross-worker access requires RPC (`session_send_rpc_evt_to_thread_force`).

### 3.2 Session lifecycle (server side, simplified)

```text
app:  vls_create(TCP) ----> [session.c] session_alloc on a worker
app:  vls_bind(ep)    ----> session_listen on that worker
app:  vls_listen      ----> transport_listen for TCP, ct_start_listen for CT
        +---- listen session created ----+
        |                                |
        v                                v
    LAYER global listen table       LAYER local listen table  (cut-through path)
        (TCP, real wire)            (CT, intra-host SHM)

peer connects:
  IF peer is on the wire (TCP)
     ct_listener_is_self_proxy? NO  -> TCP transport handles SYN
                                YES -> upgrade to local CT
  ELSE (peer is local VLS app, scope-local match)
     -> CT transport creates two ct_connection_t (sct + cct)
     -> shared FIFOs between two app_workers
     -> app_worker_accept_notify(server) via server's MQ
```

### 3.3 Transport pluggability

A "transport" registers a `transport_proto_vft_t` (vtable) with the session layer. TCP, UDP, CT, QUIC, TLS, etc. are all transports. The session layer is transport-agnostic — it just shuffles bytes between an app and a transport's FIFOs.

```c
// Cut-through registers itself as TRANSPORT_PROTO_CT
VLIB_INIT_FUNCTION (ct_transport_init);
ct_transport_init(...) {
    transport_register_protocol (TRANSPORT_PROTO_CT, &cut_thru_proto,
                                 FIB_PROTOCOL_IP4, ~0);
    ...
}
```

---

## 4. SVM FIFOs and the Shared-Memory Data Plane

The data plane between an app and VPP (and between two co-located apps in cut-through mode) is built on **SVM FIFOs** (`svm_fifo_t`).

### 4.1 What an SVM FIFO is

A single-producer/single-consumer ring with:

* **`svm_fifo_shared_t`** — the "shared" part actually lives in a shared-memory segment, with three cache-line-aligned regions: shared signals, consumer (head), producer (tail).
* **Chunked storage** — a chain of `svm_fifo_chunk_t` (powers-of-2 from 4KB to 4MB), grown on demand.
* **Out-of-order tracking** — rbtree-based, used by TCP for SACK / out-of-order receive.
* **Event signalling** — `has_event`, `want_deq_ntf`, etc., used to wake the peer.

```c
// src/svm/fifo_types.h
typedef struct svm_fifo_shr_ {
    CLIB_CACHE_LINE_ALIGN_MARK (shared);
    fs_sptr_t start_chunk;
    fs_sptr_t end_chunk;
    u32 size;
    ...
    CLIB_CACHE_LINE_ALIGN_MARK (consumer);
    fs_sptr_t head_chunk;
    u32 head;
    CLIB_CACHE_LINE_ALIGN_MARK (producer);
    fs_sptr_t tail_chunk;
    u32 tail;
} svm_fifo_shared_t;
```

Producer and consumer use atomic load-acquire / store-release on `head`/`tail` to avoid locking on the fast path.

### 4.2 Where FIFOs live

* For a normal TCP session, the FIFOs sit in a **fifo segment owned by the app's segment manager**. VPP writes received bytes into rx_fifo; the app reads them. The app writes bytes into tx_fifo; VPP's TCP transport drains them out to the wire.
* For a **cut-through (CT) session**, two app workers share the same fifo segment (`FIFO_SEGMENT_F_CUSTOM_USE`). Each side's `tx_fifo` is the other side's `rx_fifo`. There is no transport on the wire at all.

### 4.3 The notification dance

Because the FIFOs are SPSC and lock-free, a fast producer can outpace a slow consumer. To avoid busy-waiting, the consumer (app's read side) requests dequeue notifications when full, and producer signals through `has_event`. Crucially, these signals are delivered as **events on the per-worker MQ**, which the app drains via `vls_epoll_wait`.

```c
// CT tx: enqueue + tell peer
peer_s->flags |= SESSION_F_RX_EVT;
return session_enqueue_notify (peer_s);
```

### 4.4 The Message Queue (MQ)

Every app worker has a pair of `svm_msg_q_t` MQs with VPP (one for each direction). These are:

* A `pthread_mutex_t` + `pthread_cond_t` + ring buffer in shared memory.
* Optionally backed by an `eventfd` if `use-mq-eventfd` is set, so the app can `epoll` on it instead of using condvar wakeups.

```c
// src/svm/message_queue.h
typedef struct svm_msg_q_shr_queue_ {
    pthread_mutex_t mutex;
    pthread_cond_t  condvar;
    u32 head, tail, cursize, maxsize, elsize, pad;
    u8 data[0];
} svm_msg_q_shared_queue_t;
```

This is what every `vls_epoll_wait` ultimately drains. **If you stop draining the MQ, sessions stall** — accepts disappear, connection closes are not observed, etc.

---

## 5. Application, App-Worker, and the App-Socket API

### 5.1 The handshake (`vls_app_create` → `vppcom_app_create` → SAPI attach)

When a process calls `vls_app_create("my-app")`:

1.  `vppcom_app_create` reads `vcl.conf` (path from `VCL_CONFIG` env var), allocates the first `vcl_worker_t`, and sets `__vcl_worker_index` for the current thread to that worker's index.
2.  `vcl_api_attach` opens a SEQPACKET Unix socket to VPP's app-socket-api endpoint (e.g. `/run/vpp/app_ns_sockets/default`).
3.  Sends an `app_sapi_attach_msg_t` with the app's properties.
4.  VPP responds with file descriptors over `SCM_RIGHTS`:
    * The **VPP-side MQ segment** (one shared segment per VPP worker, contains the workers' MQs).
    * The **app's initial fifo segment** (from which sessions will allocate rx/tx FIFOs).
    * Optionally an **MQ eventfd** for `epoll`-based wake-up.
5.  The app `mmap`s those segments and is now part of VPP's session-layer world.

```c
// src/vcl/vcl_sapi.c
if (mp->fd_flags & SESSION_FD_F_MEMFD_SEGMENT) {
    rv = vcl_segment_attach (segment_handle, ..., fds[n_fds_used++]);
}
vcl_segment_attach_mq (segment_handle, mp->app_mq, 0, &wrk->app_event_queue);
if (mp->fd_flags & SESSION_FD_F_MQ_EVENTFD) {
    svm_msg_q_set_eventfd (wrk->app_event_queue, fds[n_fds_used++]);
}
```

### 5.2 `application_t`, `app_worker_t`, `app_listener_t`

* **`application_t`** identifies a process (or logical app). It has segment-manager properties, callbacks (`session_cb_vft_t`), and a list of `app_worker_t`.
* **`app_worker_t`** is the per-thread (or per-process, depending on mode) handle in VPP. It owns an `event_queue` (MQ), `connects_seg_manager`, and listener tables.
* **`app_listener_t`** is the listener object. It has a `workers` bitmap and an `accept_rotor` so accepts can be spread across workers (round-robin).

```c
// src/vnet/session/application.h
typedef struct app_worker_ {
    u32 wrk_index, wrk_map_index, app_index;
    svm_msg_q_t *event_queue;          // MQ this worker reads
    u32 connects_seg_manager;           // SM for outgoing connects
    uword *listeners_table;             // listener_handle -> SM
    u32 api_client_index;
    u8 mq_congested;
    session_handle_t *half_open_table;
    session_event_t **wrk_evts;
    ...
} app_worker_t;
```

### 5.3 Two attach modes

| Mode | Trigger | Meaning |
| --- | --- | --- |
| **Binary API (BAPI)** | older path; tcp `/tmp/vpp-api.sock` | Uses VPP's general binary API. Less common for VCL today. |
| **Socket API (SAPI)** | `session { enable use-app-socket-api }` in VPP, plus `app-socket-api <path>` in `vcl.conf` | Uses dedicated SEQPACKET socket; lower overhead; the path vclnet uses. |

vclnet's test config:

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

`app-scope-local` + `app-scope-global` together enable **both** cut-through-when-local and TCP-when-remote within the same app — the session layer picks whichever applies per-connection.

---

## 6. Cut-Through Sessions in Depth

### 6.1 What "cut-through" means

When two VLS apps on the same host connect to each other, the session layer **bypasses the transport** (TCP, IP, NIC) entirely. Instead it creates a pair of `ct_connection_t` linked via `peer_index`, allocates shared FIFOs that both processes map, and routes bytes via SHM. The "transport" is `TRANSPORT_PROTO_CT`.

This is the killer feature for sidecar / service-mesh / loopback patterns. Without cut-through, two co-located apps still pay the cost of building TCP packets, walking the IP stack, and looping back through the NIC driver. With cut-through, it's `memcpy(src, dst, len)`.

### 6.2 The CT data structures

```c
// src/vnet/session/application_local.h
typedef struct ct_connection_ {
    transport_connection_t connection;
    u32 client_wrk;
    u32 server_wrk;
    u32 client_opaque;
    u32 peer_index;            // index of the other ct_connection_t
    u64 segment_handle;        // shared fifo segment
    u32 seg_ctx_index;
    u32 ct_seg_index;
    svm_fifo_t *client_rx_fifo;
    svm_fifo_t *client_tx_fifo;
    transport_proto_t actual_tp;     // what the app *thinks* it's using
    ct_connection_flags_t flags;     // CLIENT | HALF_OPEN | RESET
} ct_connection_t;
```

**Each `ct_connection_t` is allocated per VPP-worker:**

```c
// application_local.c
static ct_connection_t *
ct_connection_alloc (clib_thread_index_t thread_index) {
    ct_worker_t *wrk = ct_worker_get (thread_index);
    pool_get_aligned_safe (wrk->connections, ct, CLIB_CACHE_LINE_BYTES);
    ...
    ct->c_thread_index = thread_index;
    ...
}
```

The matching server CT (`sct`) and client CT (`cct`) are placed on the **same** thread_index by `ct_program_connect_to_wrk`, which round-robins across VPP workers (skipping thread 0 if multiple workers exist).

### 6.3 The CT connect handshake

1.  **Client side**: `vls_connect` discovers (via session-table lookup) that the destination address matches a local app's listener with the right scope. Instead of allocating a TCP transport, it calls `ct_connect`.
2.  `ct_connect` allocates a half-open `ct_connection_t` (`ho`) on the "first worker" thread, fills in `peer_index = listen_session->session_index`, marks `CT_CONN_F_CLIENT`, then `ct_program_connect_to_wrk` enqueues `ho_index` for the chosen worker.
3.  The chosen worker runs `ct_accept_rpc_wrk_handler` → `ct_accept_one`:
    *   Allocates the real `cct` (client ct) and `sct` (server ct) on its own thread.
    *   Wires `cct->peer_index = sct->c_c_index` and vice versa.
    *   Allocates a `session_t` for the server side (with FIFOs from the server's segment manager). Server's `rx_fifo` becomes client's `tx_fifo` and vice versa.
    *   Calls `app_worker_accept_notify` → server's MQ gets an accept event → server app's next `vls_accept` returns the new vlsh.
4.  Server's `app_worker_init_accepted_ct` finishes setup.
5.  `ct_session_connect_notify` then runs on the same worker, allocates the client `session_t`, attaches the FIFOs to it (with `SVM_FIFO_F_CLIENT_CT` flag), notifies the client app via its MQ.

### 6.4 The CT data path

For sending:

```c
// src/vnet/session/application_local.c
int ct_session_tx (session_t *s) {
    ct = (ct_connection_t *) session_get_transport (s);
    peer_ct = ct_connection_get (ct->peer_index, ct->c_thread_index);
    peer_s = session_get (peer_ct->c_s_index, peer_ct->c_thread_index);
    ...
    peer_s->flags |= SESSION_F_RX_EVT;
    return session_enqueue_notify (peer_s);
}
```

For receiving (server side `vls_read`):

```text
vls_read -> vppcom_session_read -> svm_fifo_dequeue(s->rx_fifo, buf, n)
```

The dequeue is a **pure memcpy from the shared FIFO into the app's buffer**. No packets, no TCP, no kernel call.

### 6.5 Performance shape

On the same host, with cut-through:

| Path | Per-byte cost | Per-packet/per-msg cost |
| --- | --- | --- |
| TCP loopback (kernel) | memcpy x2 (user→kernel→kernel→user) | TCP/IP build, route, deliver, syscall round trip |
| TCP via VPP (over wire to ourselves) | memcpy x2 (FIFO ⇄ app, FIFO ⇄ NIC) | TCP/IP build in userspace, MQ events |
| **Cut-through** | **memcpy x1 (peer FIFO → reader buf)** | **MQ event only, no transport at all** |

Cut-through removes TCP framing and can reduce small-message overhead, but this repository does not contain a controlled kernel-versus-VPP benchmark from which to claim a multiplier.

### 6.6 When CT is chosen

The session layer picks CT when **all** of these are true:

* Both endpoints are on the same VPP.
* Both apps have `app-scope-local` enabled.
* App-namespace match (default namespace works out-of-the-box).
* The listening session can be looked up in the **local** session table.

Otherwise the connection falls back to the configured wire transport (TCP, etc.).

---

## 7. VCL — The Per-Thread Client Library

VCL lives in `src/vcl/vppcom.c`. It is the C library that talks to VPP from an application's address space.

### 7.1 The `vcl_worker_t`

A `vcl_worker_t` is the per-OS-thread view of VPP, kept in process-local memory:

```c
typedef struct vcl_worker_ {
    u32 wrk_index;
    vcl_session_t *sessions;       // per-worker session pool
    svm_msg_q_t  *app_event_queue; // the MQ that VPP writes to
    pthread_t     thread_id;       // owning pthread
    pid_t         current_pid;
    int           mqs_epfd;        // epoll on MQ eventfd
    uword        *session_index_by_vpp_handles;
    int           ep_lt_current;
    vcl_session_t *ep_lt_sessions;
    ...
} vcl_worker_t;
```

`vcl_worker_alloc_and_init` records `pthread_self()` as the owning thread:

```c
// src/vcl/vcl_private.c
vcl_worker_t *vcl_worker_alloc_and_init () {
    if (vcl_get_worker_index () != ~0) return 0;
    ...
    wrk = vcl_worker_alloc ();
    vcl_set_worker_index (wrk->wrk_index);
    wrk->api_client_handle = ~0;
    wrk->thread_id = pthread_self ();
    wrk->current_pid = getpid ();
    ...
}
```

### 7.2 The `__thread` worker index

Every public VCL entry point does some variant of:

```c
vcl_worker_t *wrk = vcl_worker_get_current ();
vcl_session_t *s  = vcl_session_get (wrk, session_index);
```

`vcl_worker_get_current` reads `__vcl_worker_index` from TLS. **All session state, MQ state, and epoll state is fetched via this single TLS index.** This is the central reason VCL is "per-thread" — it's not a design choice you can opt out of without modifying VCL itself.

### 7.3 Sessions in VCL

A `vcl_session_t` is the *app-side* mirror of a `session_t`. It tracks the rx/tx FIFO pointers (which point into shared memory the app has mapped), the session state machine, and metadata. The encoding of `vlsh` includes the worker index in the top bits:

```c
return (vcl_get_worker_index () << 24 | session_index);
```

So a vlsh embeds *which* worker owns the underlying session. This is how cross-worker calls can detect they need to migrate.

### 7.4 What `vppcom_worker_register` does

```c
int vppcom_worker_register (void) {
    if (!vcl_worker_alloc_and_init ()) return VPPCOM_EEXIST;
    if (vcl_worker_register_with_vpp ()) return VPPCOM_EEXIST;
    return VPPCOM_OK;
}
```

* Allocates a new `vcl_worker_t`, sets `__vcl_worker_index` for *this thread*.
* Registers with VPP via the SAPI (gets new MQs, new fifo segment context).
* The thread is now a first-class VCL worker with its own MQ pair to VPP.

This is only invoked in the `multi-thread-workers` mode (mode 2 below). In the default mode (mode 3), additional threads share the same `vcl_worker_t` as the main thread.

---

## 8. VLS — VCL Locked Sessions and Multi-Thread Modes

`src/vcl/vcl_locked.c` (2,300+ lines) wraps VCL with **synchronisation** so multiple threads / processes can use the same sessions safely. The file header documents three operating modes; the relevant ones are:

### 8.1 Mode 1: Per-process workers (fork-based, not used by vclnet)

Used when apps `fork()`. Each child becomes a new VCL worker. VLS uses `pthread_atfork` to clone shared sessions to the new worker; only "shared" sessions are locked.

### 8.2 Mode 2: Per-thread workers — `multi-thread-workers` config token

```c
// vls_mt_add()
if (vls_mt_wrk_supported ())
    vppcom_worker_register ();   // new VCL worker for this pthread
```

* Each newly seen pthread → new `vcl_worker_t` → new MQ pair to VPP.
* Sessions are **owned** by one worker (`vls->vcl_wrk_index`).
* If a thread tries to use a session owned by a different worker, VLS migrates/clones the session into the calling worker via RPC:

```c
static inline u8
vls_mt_session_should_migrate (vcl_locked_session_t *vls) {
    return (vls_mt_wrk_supported () &&
            vls->vcl_wrk_index != vcl_get_worker_index ());
}
```

* Per-session lock taken only when actually shared (`vls_is_shared`).

This mode is **the only one with real parallelism** — each worker has its own MQ and can independently drive sessions.

### 8.3 Mode 3: Single-worker multi-thread (vclnet's current mode)

This is the default when `multi-thread-workers` is *not* set in `vcl.conf`:

```c
// vls_mt_add(), the else branch
} else {
    vcl_set_worker_index (vlsl->vls_wrk_index);
}
```

* All threads share **the same** `vcl_worker_t` (worker 0).
* `vlsl->vls_mt_needs_locks = 1`, so VLS takes aggressive locks on every call:
    *   `vls_mt_mq_mlock` — a single pthread_mutex serialising all access to the worker's MQ.
    *   `vls_mt_spool_rwlock` — read/write lock around the session pool.
    *   per-`vls` spinlock — taken when the session is touched.
* Locks held by the current thread are tracked in another TLS struct:

```c
static __thread vls_mt_pthread_local_t vls_mt_pthread_local;
```

So you can release exactly the right locks on the way out.

**Correctness wise this mode is fine.** Performance wise it's a single VPP worker's worth of bandwidth, no matter how many goroutines you throw at it.

### 8.4 Why both modes still need `LockOSThread` from Go

Even in mode 3 — where all threads share one `__vcl_worker_index` — the **per-thread lock-tracking TLS** must stay coherent across a single VLS call. If the goroutine moves between Ms mid-call, the `vls_mt_rel_locks()` path on exit will release locks based on the *new* M's TLS bitmap, which is wrong. So `LockOSThread` is required in every mode.

### 8.5 The pthread cleanup destructor

`vls_app_create` registers `vls_mt_del` as a pthread destructor:

```c
if (pthread_key_create (&vls_mt_pthread_stop_key, vls_mt_del))
    return -1;
```

`vls_mt_del` runs when an M dies, decrements `vls_mt_n_threads`, releases held locks, and (in mode 2) calls `vppcom_worker_unregister()` to delete the VCL worker. This is *necessary* for cleanup but *dangerous* if Go's runtime trims Ms while VCL state still depends on them.

---

## 9. Why Goroutines Are Hostile to VCL by Default

Putting it all together, here are the **specific** ways naive goroutine + VCL code breaks:

| Failure mode | Mechanism |
| --- | --- |
| **Stale `__vcl_worker_index`** | Goroutine migrates between Ms; second VCL call reads `~0` or another worker's index → uses wrong session pool → SEGV or EBADFD. |
| **Lock-tracking corruption** | `vls_mt_pthread_local` is per-M. Goroutine starts call on M1 (sets `VLS_MT_LOCK_MQ` bit), moves to M2; `vls_mt_unguard` on M2 sees empty bitmap, never releases the mutex on M1 → deadlock for everyone else. |
| **MQ disappears under you** | Idle M trimmed by Go runtime → glibc runs `vls_mt_del` → `vppcom_worker_unregister` → MQ pair freed → any session that depended on it is orphaned. |
| **`clib_mem_init` vs Go stack allocator** | First call to `vls_app_create` runs `clib_mem_init` which installs an allocator at fixed addresses. If Go grows stacks afterwards into a colliding region, you get stack corruption / random panics. |
| **Cross-worker session access (mode 2)** | Two goroutines on different Ms touch the same vlsh; VLS triggers `vls_send_clone_and_share_rpc`; slow + may fail; race-prone. |
| **`__thread` invisibility to Go** | Go has no concept of TLS-per-OS-thread. The runtime cannot help you; you must enforce thread affinity yourself. |

**None of these are bugs in VCL.** VCL has an honest contract: *one pthread = one worker, lifetime-coherent, callable in serial unless the multi-thread modes are set up explicitly.* It is Go's M:N scheduler that violates this contract by default.

---

## 10. VCLNET Architecture

### 10.1 Three layers

```text
+------------------------------------------------------------------+
|  Go app: net.Listener / net.Conn / net/http                      |
+------------------------------------------------------------------+
|  vclnet/                                                          |
|    vclnet.go     Init(), Listen(), Dial(), DialTimeout()         |
|    listener.go   tcpListener (net.Listener)                       |
|    conn.go       tcpConn (net.Conn) — Read/Write/Close/...        |
|    addr.go       Parse + DNS via net.DefaultResolver              |
|    errors.go     *net.OpError wrapping                            |
+------------------------------------------------------------------+
|  vclnet/internal/vclpoll/cgo.go                                   |
|    Go-side:                                                       |
|      pin()  = LockOSThread + register worker once                 |
|      workerRegistry sync.Map[pthread_self() -> {}]                |
|      AppInit, ListenTCP4/6, ConnectTCP4/6Start, AcceptFull,               |
|      Read, Write, Close, GetLocalAddr, GetPeerAddr, SetV6Only     |
|    C-side (in cgo comment block):                                 |
|      vclpoll_app_create, vclpoll_register_worker                  |
|      vclpoll_listen_*_nb, vclpoll_connect_*_nb                    |
|      vclpoll_accept_nb_full                                       |
|      vclpoll_read, vclpoll_write, vclpoll_close                   |
|      vclpoll_epoll_* (persistent shared poller)                |
|      vclpoll_get_local/peer_addr, vclpoll_set_v6only              |
+------------------------------------------------------------------+
|  libvppcom.so (VPP shared library)                                |
|    vls_app_create, vls_register_vcl_worker, vls_create,           |
|    vls_bind, vls_listen, vls_accept, vls_connect,                 |
|    vls_read, vls_write, vls_close, vls_epoll_*, vls_attr, ...     |
+------------------------------------------------------------------+
|  vpp_main process (session layer, transports, NIC)                |
+------------------------------------------------------------------+
```

### 10.2 Build & link

`internal/vclpoll/cgo.go` opens with a CGo preamble that pulls in VPP's headers and links against `libvppcom.so`:

```go
/*
#cgo CFLAGS: -I/.../vpp/include
#cgo LDFLAGS: -L/.../vpp/lib/x86_64-linux-gnu -lvppcom \
              -Wl,-rpath,/.../vpp/lib/x86_64-linux-gnu

#include <vcl/vppcom.h>
#include <vcl/vcl_locked.h>
*/
import "C"
```

So the build picks up VPP's headers, links against `libvppcom`, and bakes an rpath so the runtime loader can find the shared library without `LD_PRELOAD` or `LD_LIBRARY_PATH`.

### 10.3 Surface area

The public package currently exposes initialization and shutdown, TCP
listen/dial APIs, provisional UDP `ListenPacket`, connected UDP dialing,
context-aware dialing and accept, an HTTP transport/client, and error
classification helpers. See the API list in [../README.md](../README.md).

The TCP connection implements `Read`, `Write`, `Close`, address access,
and resettable deadlines. Deadline changes wake an operation already parked in
the shared poller. The UDP connection implements connected `net.Conn`
behavior; its arbitrary-peer `PacketConn` behavior remains incomplete because
VPP accepts incoming UDP peers as separate sessions.

The listener implementation supports both `Accept` and
`AcceptContext`. Context expiry, listener close, and package shutdown remain
distinguishable through wrapped errors.

### 10.4 Why `vlsh` never escapes

In `conn.go`, `tcpConn.vlsh` is of type `vclpoll.VLSH` (an opaque handle type used only in internal fields and calls). It is not exposed in any public method. There is no `tcpConn.Fd()`, no `os.File`-style access, no way for Go's runtime poller to ever see it. This eliminates the *entire* class of "fake FD leaked into a non-hooked syscall" failures that plagued the Frida-based attempts.

---

## 11. The `pin()` Pattern and the Worker Registry

This is the **central correctness mechanism** of vclnet.

```go
// internal/vclpoll/cgo.go

var workerRegistry sync.Map // pthread id (uintptr) -> struct{}

func pin() func() {
    runtime.LockOSThread()
    tid := uintptr(C.vclpoll_pthread_self())
    if _, ok := workerRegistry.Load(tid); !ok {
        C.vclpoll_register_worker()
        workerRegistry.Store(tid, struct{}{})
    }
    return runtime.UnlockOSThread
}
```

And every public entry point begins with:

```go
defer pin()()
```

### 11.1 What this guarantees

| Property | Guaranteed because |
| --- | --- |
| The goroutine cannot migrate between Ms during a VLS call | `runtime.LockOSThread()` is held throughout |
| `__vcl_worker_index` is correct for this thread | The first VLS call on a fresh M calls `vls_register_vcl_worker` and `vcl_set_worker_index` |
| VLS lock-tracking TLS (`vls_mt_pthread_local`) is coherent | We never leave the M mid-call |
| `vls_register_vcl_worker` is called **at most once per pthread** | The `sync.Map` keyed by `pthread_self()` |
| AppInit runs on the right thread | `AppInit` itself uses `runtime.LockOSThread()` and records the initial pthread |

### 11.2 `AppInit` is special

```go
func AppInit(appName string) error {
    appOnce.Do(func() {
        runtime.LockOSThread()
        defer runtime.UnlockOSThread()
        cname := C.CString(appName)
        defer C.free(unsafe.Pointer(cname))
        rv := C.vclpoll_app_create(cname)
        if rv != 0 {
            appErr = fmt.Errorf("vls_app_create failed: rv=%d", int(rv))
            return
        }
        workerRegistry.Store(uintptr(C.vclpoll_pthread_self()), struct{}{})
    })
    return appErr
}
```

* `sync.Once` makes it idempotent (re-calling Init is a no-op).
* Pre-records the main pthread as already registered — `vls_app_create` itself implicitly creates worker 0 on the calling thread.
* MUST be called **once at program start, on the main goroutine, before any other goroutine is spawned**. This avoids `clib_mem_init` colliding with Go's stack growth on other Ms.

### 11.3 What `pin()` does not solve

- **Mode 3 serialization:** calls are correct across registered threads, but
  all application-side VLS work shares one worker and its locks.
- **Mode 2 ownership:** `multi-thread-workers` assigns sessions to individual
  VCL workers. The current single poller cannot touch every session without
  crossing ownership boundaries.
- **Portable deployment:** pinning does not address the hard-coded build tree
  or VPP ABI/version validation.
- **Full lifecycle draining:** Shutdown now gates new work and wakes poller
  waiters, but a registry-driven graceful drain remains pending.

## 12. Non-Blocking I/O and the Shared Poller

### 12.1 Why non-blocking is required

If a Go goroutine calls a *blocking* `vls_read` and the FIFO is empty, that goroutine pins its M inside CGo for an unbounded time. The Go runtime sees the M is blocked → spawns another M to keep other goroutines running. Under concurrent load this leads to **M-count inflation** — hundreds of Ms each parked in CGo, each tying up a pthread, each potentially registered as a VCL worker.

So **every VCL session in vclnet is created non-blocking** (`vls_create(proto, /*is_nonblocking=*/1)`), and `vls_read`/`vls_write`/`vls_accept`/`vls_connect` all return `-EAGAIN` (or `-EINPROGRESS` for connect) when not immediately satisfiable.

### 12.2 The shared poller goroutine

A single background goroutine owns one persistent `vls_epoll` handle (`internal/vclpoll/poller.go`). When `Read`, `Write`, or `Accept` get EAGAIN:

1. The calling goroutine releases its OS thread (`runtime.UnlockOSThread()`).
2. It sends a `waiter{vlsh, events, ready}` struct to the poller's registration channel.
3. It blocks on `<-ready` (a Go channel).
4. The poller adds the vlsh to its persistent epoll via `vls_epoll_ctl(ADD)`.
5. On the next `vls_epoll_wait` iteration that reports the vlsh ready, the poller closes `ready`.
6. The calling goroutine wakes, re-locks an OS thread, registers the worker, and retries the VLS operation.

```go
// poller.go — core loop (simplified)
func (p *poller) loop() {
    runtime.LockOSThread() // permanent
    registerThisThread()

    ep, _ := pollerEpollCreate()
    p.epVLSH = ep

    for {
        p.drainRegistrations()   // add new waiters to epoll
        p.drainUnregistrations() // remove closed sessions
        n := pollerEpollWait(ep, eventBuf)
        for i := 0; i < n; i++ {
            close(p.waiters[eventBuf[i].Vlsh].ready) // wake goroutine
        }
    }
}
```

Critically, `vls_epoll_wait` also **drains the worker's MQ** as a side effect. The poller's continuous 100ms polling loop ensures session events (accepts, closes, RX events) are always delivered, even when no application goroutine happens to be retrying I/O.

### 12.3 Connect uses the shared poller

The public TCP dial path initiates a non-blocking connect with
`ConnectTCP4Start` / `ConnectTCP6Start` and waits for EPOLLOUT through
`PollWaitContext`. Connected UDP now uses the equivalent split-connect
helpers. This lets context cancellation remove the precise connect waiter and
close the in-flight session.

The low-level `DialTCP4`, `DialTCP6`, `ConnectUDP4`, and
`ConnectUDP6` compatibility helpers still contain a one-shot temporary epoll
wait, but the public vclnet dialer does not use those paths.

VPP's reliable post-connect error query is still an open item in this target
build; today readiness is treated as completion after immediate hard failures
have been handled.

### 12.4 Benefits of the Shared Poller

| Metric | Temp-epoll-per-call (original) | Shared poller (current) |
| --- | --- | --- |
| Epoll handles created/destroyed per second | Thousands (one per EAGAIN) | 1 total (persistent) |
| MQ drain frequency | Only when a goroutine retries | Continuous (100ms loop) |
| OS threads held during I/O wait | One per waiting goroutine | Zero (released before parking) |
| M-count inflation under load | High (each EAGAIN holds an M for up to 1s) | Minimal (Ms released immediately) |

---

## 13. Earlier Attempts: Why Frida-Based Approaches Failed

For historical context, two earlier sister projects tried other strategies. Both worked for one connection and failed for many.

### 13.0 At-a-glance capability matrix

| Capability | `frida-vpp` (A) | `go-frida-vpp` (B) | **vclnet** |
| --- | :---: | :---: | :---: |
| Goroutine support | partial (1 active) | none | full |
| Concurrent connections | serialised | 1 at a time | tested concurrently |
| `net.Conn` interface | no | no | yes |
| `net.Listener` interface | no | no | yes |
| `net/http` compatible | no | no | yes |
| `crypto/tls` compatible | no | no | yes (layered) |
| IPv4 | yes | yes | yes |
| IPv6 | partial | partial | yes (V6ONLY) |
| No fake-FD leakage | no | no | yes |
| Independent of Go runtime poller | no (conflicts) | no (conflicts) | yes (separate VLS epoll) |
| Handles MPTCP probe | manual hack | broken | N/A by construction |
| No single-threaded bottleneck | no | no | yes |
| Non-network syscall overhead | ~0 | ~3–5 µs / call | 0 |
| Source-code changes required in target app | none | none | **import-path change only** |
| Binary modification at runtime | Frida attach | Frida attach | none |
| External runtime dependency | Frida agent | Frida agent | `libvppcom.so` |
| Static-binary friendly | no | no | no (CGo + libvppcom) |
| Debuggable with delve / gdb | no (JS frames) | no (JS frames) | yes |
| Cut-through (CT) sessions accessible | yes-if-1-conn | yes-if-1-conn | yes |
| Production direction | no | no | yes, with documented hardening gaps |

### 13.1 `frida-vpp` — per-function syscall hooks via LDP

* **Strategy:** Use Frida to JIT-overwrite Go's `syscall.socket`, `syscall.bind`, `syscall.connect`, `syscall.accept4`, etc. with `ret`-shims, then in `onLeave` callbacks call the corresponding LDP function (`ldp.socket(...)`, etc.).
* **Worked:** Single-connection TCP echo, both client and server.
* **Why it failed at scale:**
    * Frida's JavaScript engine is **single-threaded**. All hooks serialise through one V8 isolate. With N goroutines doing I/O, you get O(N) tail latency.
    * `accept4` had to spin-wait inside the hook (100% CPU), blocking every other hooked syscall.
    * Each hook used `epoll_wait` as an MQ pump → thundering herd.
    * Go's runtime poller was bypassed → hooks couldn't return `EAGAIN`, had to block inside.
    * LDP returned "fake FDs" like `vlsh + 32`, which leaked into non-hooked syscalls (`fstat`, `dup`, `splice`) → EBADF crashes.
    * 11 syscalls hooked; new Go versions kept adding wrappers — a moving target.
    * Go error returns required heap-allocating `{itab, data}` interface pairs from JS.

### 13.2 `go-frida-vpp` — single hook on `Syscall6` via VLS

* **Strategy:** Hook the single entry point `internal/runtime/syscall.Syscall6`. Dispatch by syscall number, replace `RAX` with `SYS_GETPID` (a harmless no-op) so the kernel doesn't act, then call `vls_*` directly in `onLeave` and write the result back to `RAX`/`RCX`.
* **Worked:** Echo + HTTP, one connection at a time.
* **Why it failed:**
    * `clib_mem_init` (called by `vls_app_create`) conflicted with Go's stack allocator when goroutines were spawned afterwards → stack corruption.
    * Every syscall (not just network) paid 3-5 µs of interception cost — `futex`, `mmap`, `epoll_pwait` all dragged.
    * Only one connection in flight at any time.
    * Fixed iteration counts for polling were brittle (500×10ms here, 50×10ms there).
    * No MPTCP handling. Go 1.21+ probes for MPTCP (`socket(AF_INET, SOCK_STREAM, IPPROTO_MPTCP=262)`) and the unhandled probe caused EADDRINUSE on fallback.

### 13.3 The structural problem common to both

> They emulate a kernel inside a single-threaded JS interpreter while pretending to Go that everything is synchronous. O(1) for one conn, O(N) tail latency for N, and never thread-safe.

### 13.4 Why vclnet's CGo approach is structurally different

| Problem | Frida approach | vclnet |
| --- | --- | --- |
| Single-threaded bottleneck | Fatal | N/A — native threads, just CGo |
| Go ABI ↔ SysV ABI bridge | Manual register manipulation in JS | CGo handles automatically |
| Trampoline / ret shim | Required | None |
| Fake FD leakage | Constant EBADF | `vlsh` stays inside `internal/vclpoll`; never a Go FD |
| Runtime poller conflict | Hooks can't return EAGAIN | Separate VLS epoll; Go runtime poller never sees vlsh |
| Thread model mismatch | Cannot support goroutines | `LockOSThread` + per-thread worker registry |
| Hook maintenance | 11+ syscalls to track per Go release | Zero hooks |
| Error construction | Build Go itab/data pairs | Standard Go `error` returns |
| MPTCP | Manual fix required | N/A — direct `vls_create(PROTO_TCP)`, no kernel probe |

### 13.5 Side-by-Side: Issues With Each Previous Approach

The following four tables are designed to drop straight into a slide deck. Each row is a specific concrete failure, with its mechanism and consequence — not abstract design opinions.

#### 13.5.1 Approach A — `frida-vpp` (per-function syscall hooks → LDP)

**Strategy:** Frida JavaScript hooks individual `syscall.*` functions in Go's runtime (`syscall.socket`, `syscall.bind`, `syscall.connect`, `syscall.accept4`, `syscall.read`, `syscall.write`, `syscall.close`, `syscall.epoll_*`, etc.). Each hook's `onEnter` overwrites the original function body with `ret`; `onLeave` then calls the corresponding **LD_PRELOAD shim** (`ldp.socket`, `ldp.bind`, …) and writes the result back to the Go registers.

| # | Issue | Mechanism | Consequence |
| --- | --- | --- | --- |
| A1 | **Single-threaded JS isolate** | Frida's `Interceptor` runs all callbacks on one V8 isolate | Every hook serialises; N concurrent goroutines → O(N) tail latency |
| A2 | **`accept4` busy-spins inside the hook** | LDP's `accept4` returns immediately when no pending conn; hook loops with sleep | 100% CPU on the JS thread; blocks *every* other hooked syscall in the process |
| A3 | **`epoll_wait` doubles as MQ pump per call** | Each `epoll_wait` hook drains the MQ before returning | Thundering herd when many connections want to wake |
| A4 | **Go runtime poller is bypassed** | The hook returns the LDP result synchronously; cannot register vlsh with kernel epoll | Goroutines cannot park in Go — they spin inside the hook holding the JS thread |
| A5 | **Fake FD leakage (`vlsh + 32`)** | LDP returns "fake FDs" with a numeric offset; non-hooked syscalls (`fstat`, `dup`, `splice`) take them as real | EBADF crashes; `os.NewFile` corruption; deadlocks in `runtime.netpoll` |
| A6 | **11+ syscalls to hook** | Each Go release reshuffles `syscall.*` wrappers and adds new ones (e.g. MPTCP probe) | Moving target; hooks break on Go upgrade; surface area grows monotonically |
| A7 | **Go error interface construction** | Hooks must build `{itab, data}` interface pairs from JS to return `error` | Requires heap allocation from inside Frida; ABI-fragile |
| A8 | **MPTCP probe (Go ≥ 1.21)** | Go probes `socket(AF_INET, SOCK_STREAM, IPPROTO_MPTCP=262)` first; falls back to TCP | Without explicit reject, fallback TCP `bind` hits `EADDRINUSE` |
| A9 | **`accept4` flags translation drift** | LDP's accept4 flag handling differs subtly from kernel | `SOCK_CLOEXEC` / `SOCK_NONBLOCK` propagation bugs |
| A10 | **Single connection scales; many do not** | Items A1–A4 compound | Works for echo demos; collapses under any realistic load |

**Bottom line on A:** Each hook is locally correct but globally serialised through Frida's single JS thread, and the fake-FD-via-LDP path leaks into the rest of Go's runtime in ways that cannot be plugged.

#### 13.5.2 Approach B — `go-frida-vpp` (single hook on `Syscall6` → VLS direct)

**Strategy:** Hook **only** the single entry point `internal/runtime/syscall.Syscall6`. In `onEnter`, inspect `RDI` (the syscall number); for network-related syscalls, replace `RAX` with `SYS_GETPID` (so the kernel does a harmless no-op) and remember what was wanted. In `onLeave`, call the corresponding `vls_*` function directly and write the result back to `RAX` / `RCX`. No LDP involvement; one hook covers everything.

| # | Issue | Mechanism | Consequence |
| --- | --- | --- | --- |
| B1 | **No goroutine support, period** | `vls_app_create` runs `clib_mem_init`; later goroutine stack growth collides with VPP's mheap baseva | Random SIGSEGV / corrupted goroutine stacks; only safe with `GOMAXPROCS=1` and zero extra goroutines |
| B2 | **Every syscall pays interception cost** | The hook fires on *all* `Syscall6` calls, not just network — `futex`, `mmap`, `nanosleep`, `epoll_pwait` all trampoline through JS | ~3–5 µs added to every syscall in the process |
| B3 | **One connection at a time** | The hook is synchronous, inline; while it runs, no other syscall can complete | Server handles one conn, closes it, accepts the next |
| B4 | **Hardcoded polling iteration counts** | Wait loops like `500 × 10ms` or `50 × 10ms` baked into hook | Misses fast events; over-waits slow ones; spurious timeouts |
| B5 | **MPTCP probe still breaks** | `Syscall6` hook sees `SYS_SOCKET` but doesn't know about IPPROTO_MPTCP semantics | Same `EADDRINUSE` symptom as A8 |
| B6 | **`clib_mem_init` allocator clash** | VPP installs its own allocator at fixed addresses during `vls_app_create` | Cannot safely call any Go function that grows the stack after Init |
| B7 | **Direct register manipulation is ABI-fragile** | Hook reads/writes `RAX`/`RDI`/`RSI`/`RDX`/`R10`/`R8`/`R9` directly from JS | Breaks on any Go runtime change to syscall calling convention |
| B8 | **Cannot return EAGAIN to Go runtime** | If hook returned EAGAIN, Go's netpoller would try to register the (non-existent) FD with kernel epoll → EBADF | Hook must block inside `vls_epoll_wait` indefinitely |
| B9 | **Frida JS GC pauses the hook** | V8's GC stalls the isolate; in-flight network operations stall too | Periodic latency spikes correlated with V8 GC |
| B10 | **Cannot scale beyond `GOMAXPROCS=1`** | Items B1, B3, B6 jointly forbid multi-M operation | Throughput bounded by one core regardless of machine size |

**Bottom line on B:** Cleaner than A (one hook, no LDP, no fake FDs in the LDP sense), but the structural blocker is `clib_mem_init` + Go's stack allocator. You cannot run goroutines safely once VLS is initialised — and a Go program without goroutines isn't really a Go program.

#### 13.5.3 Why **Frida** is not the right solution (architecturally)

Beyond the specific issues above, Frida itself is the wrong tool for this job. The reasons are not "Frida is bad" — Frida is excellent at its actual purpose (reverse-engineering, dynamic analysis, instrumentation). The reasons are about *mismatch with the problem*:

| # | Reason | Detail |
| --- | --- | --- |
| 1 | **Wrong threading model** | Frida exposes one JS isolate per process. The networking workload we need to support is N concurrent goroutines × M cores. There is no Frida configuration that makes the isolate multi-threaded. |
| 2 | **Dynamic instrumentation overhead is paid forever** | Frida hooks live for the program's lifetime. Every covered call instruction goes through JIT trampoline → JS isolate → JS callback → trampoline back. There is no way to "compile out" the hook. |
| 3 | **Adversarial to Go's runtime invariants** | Go's compiler / runtime assume `syscall.*` wrappers and `runtime·entersyscall` / `runtime·exitsyscall` have specific stack and register layouts. Frida overwrites those prologues. Any Go runtime change can break the hooks silently. |
| 4 | **No path to integrate with `runtime.netpoll`** | Frida cannot synthesize a kernel FD whose readiness comes from VPP's MQ. Without that, hooks must block in-place, defeating the entire reason Go uses an M:N scheduler. |
| 5 | **Brittle across Go versions** | Symbol names (`syscall.socket` → `internal/syscall/unix.socket` → `runtime/internal/syscall.Syscall6` …) change every few Go releases. Hooks must track. There is no compile-time check. |
| 6 | **No symmetric distribution story** | To deploy, every target host needs Frida agent + Frida JS file + the right Go binary built with debug symbols (or with the right symbol-resolution strategy). For an actual VPP deployment, this is operationally hostile. |
| 7 | **Cannot honour VCL's `__thread` contract** | The hooks fire on whatever M Go's scheduler picked; they cannot ensure the `__thread __vcl_worker_index` is the right one. The two earlier projects worked around this by serialising — at which point you're back to one-thread performance. |
| 8 | **Debugging story is poor** | A crash inside a Frida hook produces a JS stack on top of a Go stack on top of a C stack. `gdb` cannot walk it; `delve` cannot walk it; you get hex addresses and a guess. |
| 9 | **Cannot work in fully-static builds** | Static Go binaries (a common deployment target) cannot have Frida attached at all, because Frida relies on dynamic loader cooperation. |
| 10 | **Wrong abstraction boundary** | Frida says "intercept syscalls", but the real problem is "Go's `net.Conn` interface needs to be backed by VCL". The right boundary is `net.Conn`, not `syscall.Syscall6`. |

#### 13.5.4 Why we need **vclnet** (a VCL wrapper in Go)

| # | Why vclnet | Detail |
| --- | --- | --- |
| 1 | **Right abstraction boundary** | The intersection point between Go and VCL is `net.Conn`/`net.Listener`, not individual syscalls. vclnet draws the line exactly there. |
| 2 | **Honours VCL's per-pthread contract** | `runtime.LockOSThread` makes a goroutine *be* a pthread for the duration of a VLS call; `sync.Map` worker registry calls `vls_register_vcl_worker` exactly once per M. |
| 3 | **No fake FDs** | `vlsh` is an internal type (`vclpoll.VLSH` aliased to `int32`) that never escapes `internal/vclpoll`. The Go runtime poller never sees it; EBADF risk is structurally zero. |
| 4 | **Standard Go errors** | `*net.OpError` wrapping; deadlines via `atomic.Value`; `net.Error.Timeout()` semantics. `crypto/tls`, `net/http`, gRPC all "just work" on top. |
| 5 | **Zero hook maintenance** | No syscalls intercepted. Go version upgrades don't break the bridge. New Go syscall wrappers don't matter. |
| 6 | **Native CGo ABI** | The Go ↔ C ABI is the supported, maintained path. CGo handles register conventions, stack switching, GC interaction. |
| 7 | **Build-time linkage** | Linked against `libvppcom.so` at build time with rpath baked in. No runtime injection, no `LD_PRELOAD`, no agent. Deployable as a single binary + shared lib. |
| 8 | **Supports goroutine concurrency** | Mode 3 is correct under concurrent goroutines but serializes inside VLS. Mode 2 parallelism requires the pending session-affinity redesign. |
| 9 | **Controlled initialization** | Calling `AppInit` once, early, avoids the unsafe in-hook initialization pattern seen in the prototype; it is a deployment requirement, not proof against every allocator interaction. |
| 10 | **Cut-through-ready** | The vcl.conf already enables `app-scope-local`; vclnet's `vls_connect` / `vls_accept` exercise the CT transport path automatically when both peers are local. |
| 11 | **Composable with existing libs** | Because the output is `net.Conn`, any library that takes a `net.Conn` (TLS, HTTP/2, gRPC over HTTP/2, Kafka client, Redis client, …) works unmodified. |
| 12 | **Debuggable** | Stack traces are pure Go + cgo + C. `delve` walks the Go side; `gdb` walks the C side. No JS layer. |
| 13 | **MPTCP / new probes don't matter** | vclnet's `vls_create` is called with `VPPCOM_PROTO_TCP` directly. The kernel is never asked anything about MPTCP, so probes never fire. |
| 14 | **No application busy-wait** | EAGAIN parks the goroutine on a Go channel while the shared VLS epoll poller owns readiness and MQ draining. |
| 15 | **True netpoller architecture** | A single shared poller goroutine parks waiters on Go channels — structurally analogous to Go's own `runtime.netpoll`. |
| 16 | **Honest contract with VPP** | vclnet uses VLS exactly as VLS was designed to be used: one pthread = one worker, lifetime-coherent, called serially per session. No deception. |

**One-line summary:** Frida tries to *make Go pretend syscalls went to VCL*. vclnet *implements `net.Conn` on top of VCL*. The second is what was supposed to exist from day one.

---

## 14. Deep-Dive Q&A: Goroutines, FDs, and Cut-Through

### Q1. What happens when you start goroutines and use Frida for VCL?

Two failures, simultaneously, both structural:

#### (a) Frida's JS runtime is single-threaded

Frida's `Interceptor` callbacks all execute on Frida's V8 isolate, which has **one** execution context. With N goroutines hammering hooked syscalls, every hook serialises through that single JS thread. Worse, if any hook blocks (e.g. spin-waiting in `accept4`, or pumping the MQ in `epoll_pwait`), **every other hooked syscall in the process — including ones unrelated to networking — queues behind it**. Go's runtime cannot do useful work because its own internal syscalls (`futex`, `mmap`, `epoll_pwait`, etc.) go through the same chokepoint. Tail latency becomes O(N).

#### (b) The Go runtime breaks Frida's caller assumptions

Frida resolves `syscall.Syscall6` as a code address and rewrites its prologue. It expects a stable mapping between "caller goroutine" and "thread of execution" for things like constructing a Go error interface. But:

* Go's M:N scheduler can preempt a goroutine and resume it on a different M.
* Go can synthesise new Ms when a goroutine blocks (`newm` / `handoffp`).
* Some syscalls run *very* early in goroutine setup, before the runtime is willing to be intercepted.

On top of that, `go-frida-vpp` had to call `vls_app_create` from inside the process, which runs `clib_mem_init`. That installs VPP's allocator state at fixed addresses, and if Go subsequently asks the kernel for a stack region that interferes with VPP's mheap, you get **corrupted goroutine stacks** and random panics.

**Honest answer:** for one connection, mostly fine. For >1 concurrent goroutine doing I/O, you get non-deterministic latency spikes, EBADF storms from fake FDs leaking into non-hooked syscalls, and eventually stack/memory corruption. The approach is structurally O(N) at best and not safe under goroutine scheduling at all.

### Q2. Can you have 2 different goroutines working on a common file descriptor (vlsh)?

**Yes, with caveats — and the answer depends on which VLS mode you're in.**

In vclnet's vclpoll layer, every entry point begins with `defer pin()()`. So:

* Goroutine A and goroutine B both call `tcpConn.Read(b)` on the same `vlsh`.
* Each one pins to whatever M it currently runs on. If A's M and B's M are different, both Ms get registered with VLS (each once, via the `sync.Map`).
* Inside VPP/VLS the per-vls spinlock serialises the actual operation:

```c
static inline void vls_lock (vcl_locked_session_t *vls) {
    if (vlsl->vls_mt_needs_locks || vls_is_shared (vls))
        clib_spinlock_lock (&vls->lock);
}
```

**Correctness is preserved** — two goroutines on the same vlsh will not corrupt VLS state.

**Caveats:**

1.  **Semantics are POSIX-like** — two readers on the same socket race for bytes; you get whatever interleaving the scheduler hands you. Same as stdlib `net.Conn`.
2.  **Deadline state is shared** — both goroutines see the same `readDeadline` / `writeDeadline` on the `tcpConn` (stored in `atomic.Value`).
3.  **Close races as usual** — vclnet uses `closeOnce` + `closed atomic.Bool` so close is idempotent, but a Read in flight on goroutine A while B closes can still observe `-EBADFD` on the way back. That error is wrapped and returned cleanly.
4.  **In mode 3 (vclnet's current mode), all VCL traffic globally serialises on `vls_mt_mq_mlock`** anyway. Two goroutines on *different* vlshes aren't really parallel either; they both queue on the MQ mutex.

So yes, sharing a vlsh between goroutines is safe; the design just doesn't gain anything from doing so because there's no parallel work to be done on a single TCP/CT session.

### Q3. Golang VCL needs cut-through. VCL has per-thread state. What do we do with goroutines?

This is *the* hard question. Let me lay out why it bites and what vclnet does.

#### Why it bites

VCL state is keyed to **pthread**, via two `__thread` globals:

* `__vcl_worker_index` (`vcl_private.h`) — picks the VCL worker, which owns the session pool and the MQ pair with VPP.
* `vls_mt_pthread_local` (`vcl_locked.c`) — tracks lock ownership for the current thread.

Goroutines do not own a pthread. Go's runtime freely:

* Moves a goroutine between Ms during normal scheduling.
* Spawns new Ms when an existing one blocks in cgo / syscall.
* Destroys Ms when they've been idle.

If a goroutine started a VCL call on M1 and resumed on M2:

* `__vcl_worker_index` on M2 has the wrong value, or `~0` if M2 has never touched VCL.
* The per-thread lock bitmap on M1 has bits set; on M2 it's zeroed; the `vls_mt_unguard()` path on M2 releases the wrong locks (or none, leaking M1's locks forever).

This is **immediately fatal**.

Plus a second, subtler killer: when an M dies, glibc runs the `vls_mt_del` destructor, which calls `vppcom_worker_unregister()` — **deleting the VCL worker the thread was using, including its MQ pair with VPP**. If you let Go destroy Ms freely, VCL workers blink in and out of existence.

Cut-through doesn't change much: the *transport* is in shared memory, but the *control path* (creating the session, getting FIFO pointers, accept notifications, EOF notifications) still flows over the MQ owned by a specific VCL worker keyed by `__thread`. Lose the thread → lose the MQ → lose the session.

#### What vclnet does about it

```go
func pin() func() {
    runtime.LockOSThread()
    tid := uintptr(C.vclpoll_pthread_self())
    if _, ok := workerRegistry.Load(tid); !ok {
        C.vclpoll_register_worker()
        workerRegistry.Store(tid, struct{}{})
    }
    return runtime.UnlockOSThread
}
```

The contract:

1.  `runtime.LockOSThread()` on entry, `UnlockOSThread()` on exit. While locked, the Go scheduler guarantees the goroutine stays on this M *and the M won't be killed under it*.
2.  `workerRegistry sync.Map[pthread_self()]struct{}` ensures the first time any pthread touches VCL we call `vls_register_vcl_worker()` exactly once.
3.  Per-call locking (not per-goroutine-lifetime) keeps the model simple: pin for the duration of one VLS call, release the M, the scheduler can pick a different goroutine next.
4.  Worker-0 is eagerly recorded in `AppInit`, which runs once on the main goroutine before others are spawned.

This solves **correctness** in two scenarios:

* N goroutines, each calling VCL on whichever M is free. Each call pins, may register that M as a new VCL worker (once), does its work, releases the M.
* A goroutine doing several VCL calls in succession. Each `pin()` pins it for one call; between calls it can float.

#### How the shared poller addresses this

The implemented poller owns one VLS epoll handle on a goroutine pinned for its
lifetime. Ordinary connection goroutines pin only around immediate VLS calls.
On EAGAIN they release the OS thread and wait on Go channels.

The poller maintains one epoll registration per session, with a union mask and
independent waiter objects. This matters for a `net.Conn`, where one reader
and one writer are allowed concurrently. Deadline cancellation removes only
the affected waiter.

In current mode 3, all registered threads share worker 0. The poller can
therefore touch a session created by another thread without crossing a VCL
worker ownership boundary, although VLS locks serialize application-side work.

#### Future mode-2 picture

A mode-2 design cannot simply enable `multi-thread-workers` in the existing
configuration. It needs a fixed set of permanently pinned worker event loops:

```text
Go callers
   |
   +-- submit operation to owner W0 -- pinned thread, VCL worker 0, epoll 0
   |
   `-- submit operation to owner W1 -- pinned thread, VCL worker 1, epoll 1
```

Every session operation, including create, read/write, epoll control, and
close, must execute on its owning worker. This avoids migration of active
sessions and gives each worker its own message queue. Task processing and
epoll waiting must be integrated into each event loop without starving either.

That architecture is pending and is not exercised by
`test/run_multiworker.sh`; that script configures multiple VPP workers while
retaining application-side VLS mode 3.

---

## 15. Known Bugs, Workarounds, and the VPP Debug-Build Race

### 15.1 The cut-through cleanup race (debug builds)

**Symptom:** With multiple CT sessions in the same VLS app doing overlapping I/O, debug-build VPP intermittently resets sessions with `ECONNRESET` or returns spurious EOF; rarely crashes with poison values like `0xdeadd9ee`.

**Stack pattern:**

```text
ct_handle_cleanups -> ct_session_postponed_cleanup
                  -> segment_manager_dealloc_fifos_ct (assertion: cnt >= 0)
```

**Root cause:** Cleanup of one CT session runs asynchronously on a VPP worker while other CT sessions in the same segment context are still enqueueing/dequeueing. Shared segment-context state gets touched concurrently; in debug builds, additional assertions widen the window and surface the corruption.

**Trigger conditions (all required):**

1.  Multiple CT sessions in the same VLS app.
2.  Two or more sessions performing I/O overlapping in time.
3.  VPP built with `-DCMAKE_BUILD_TYPE=Debug` (release builds make the timing window too narrow to hit in practice).

**Workarounds applied in the test suite:**

* Sequential dials (no concurrent `connect()`).
* Sequential I/O across sessions (one session in flight at a time).
* 1-second sleep between tests (`skipIfNoVPP` helper) to let `ct_handle_cleanups` complete.
* Full VPP restart between test suites (`test/run_integration.sh`).
* Explicit removal of both CLI and app-namespace Unix sockets before VPP restart (prevents stale-socket-detection races).

**Production recommendation:** Run against a **release** VPP build before declaring this a real bug. The vclnet library itself is not at fault here.

### 15.2 Test infrastructure issues that were resolved

* **Bufio reader race in vclpoll tests** — two goroutines reading the same `bufio.Reader` (one for "READY", one to drain stdout) raced; fixed by merging into a single goroutine that reads READY then drains.
* **VPP app-registration cleanup lag** — VPP debug builds are slow to detect a previous child's disconnect; new children's `vls_app_create` could fail. Fixed with a 1s sleep before each test.
* **Stale socket detection race** — script removed only `cli.sock`, not `app_ns_sockets/default`, so the "wait for VPP to be ready" loop succeeded against a dead socket. Fixed by removing both.

### 15.3 Architectural caveats (not bugs)

* **Single VLS app per process** — VPP cannot route a `connect()` to a `listen()` in the same VLS app. Client and server **must** be separate processes (tests use a self-reexec pattern with `VCLNET_TEST_SERVER_MODE=1`).
* **No TLS in vclnet itself** — TLS must layer on top via `crypto/tls.Server(conn, cfg)`. The underlying VCL also supports TLS as a transport (`VPPCOM_PROTO_TLS`); a future phase can expose that.

---

## 16. Current Status and Pending Work

The audited implementation covers TCP and connected UDP on IPv4/IPv6,
context-aware connection and accept paths, live deadlines, HTTP/1.1, layered
TLS, shared-poller concurrency, shutdown, and multi-VPP-worker stress.

It is not accurate to describe every networking feature as complete.
Unconnected UDP `PacketConn` semantics are not implemented end to end, VLS
mode 2 is incompatible with the shared poller, build paths are
workstation-specific, and the repository lacks automated VPP version-matrix CI
and a checked-in comparative performance baseline.

The canonical prioritized list, including connect-error verification,
lifecycle hardening, native TLS, half-close, and protocol coverage, is
[../summary.md](../summary.md#3-pending-work). That list supersedes older
phase/roadmap statements in this historical report.

**Current validation entry points:**

- `go test -count=1 ./...`
- `go test -race -count=1 ./...`
- `go vet ./...`
- `sudo bash test/run_integration.sh`
- `sudo bash test/run_multiworker.sh 4`
- `make build`

## Appendix A: vcl.conf Tokens and Their Effects

| Token | Effect |
| --- | --- |
| `rx-fifo-size N` | Default rx FIFO size (bytes) for new sessions. vclnet uses 4 MB. |
| `tx-fifo-size N` | Default tx FIFO size. vclnet uses 4 MB. |
| `app-scope-local` | Enable cut-through transport for sessions to local apps. **Required for CT.** |
| `app-scope-global` | Allow sessions to non-local destinations (TCP/wire). |
| `use-mq-eventfd` | Back MQ wakeups with an `eventfd` (cheaper than condvar; enables epoll on the MQ). |
| `app-socket-api <path>` | Use the SAPI Unix socket at `<path>`. **Required for vclnet's attach flow.** |
| `multi-thread-workers` | **VLS Mode 2**: each pthread that touches VLS becomes a real VCL worker with its own MQ. Required for true parallelism in Go. *Not* set today. |
| `huge_page` | Use hugepages for the segment baseva (perf). |
| `tls-engine N` | Select TLS engine (mbedtls / openssl / etc). |
| `app_original_dst` | Expose SO_ORIGINAL_DST (for transparent proxies). |
| `event_log_path PATH` | Where to write the VCL event log. |

Corresponding VPP startup config:

```text
session { enable use-app-socket-api }
```

This enables the session layer and the SEQPACKET app-socket-api endpoint.

---

## Appendix B: Key Source Locations (Cross-Reference)

### vclnet itself

| File | Purpose |
| --- | --- |
| `vclnet/vclnet.go` | Public `Init`, `Listen`, `ListenContext`, `ListenPacket`, `Dial`, `DialContext`, `DialTimeout`, `TCPListener`, `Shutdown`, `InstallSignalHandler` |
| `vclnet/dialer.go` | `Dialer` struct, `DialContext`, Happy Eyeballs (`dialHappyEyeballs`, `interleaveAddrs`) |
| `vclnet/conn.go` | `tcpConn` (`net.Conn`) — Read/Write/Close/deadlines |
| `vclnet/udpconn.go` | `udpConn` (`net.Conn`; provisional `net.PacketConn`) — connected UDP is validated |
| `vclnet/listener.go` | `tcpListener` (`net.Listener` with `AcceptContext`) — Accept/Close/doneCh |
| `vclnet/shutdown.go` | `Shutdown()`, `ShutdownDone()`, `InstallSignalHandler()` |
| `vclnet/transport.go` | `Transport()`, `DefaultTransport`, `NewHTTPClient()` — HTTP connection pooling |
| `vclnet/addr.go` | Network parsing, DNS resolution, `resolveAddrs`, `resolveUDPAddr`, `isUDP` |
| `vclnet/errors.go` | `*net.OpError` wrapping, `IsTimeout`, `IsConnectionRefused`, `IsConnectionReset` |
| `vclnet/internal/vclpoll/cgo.go` | CGo bridge; TCP + UDP C helpers, split-connect, `pin()`, `workerRegistry`, `VCLError` |
| `vclnet/internal/vclpoll/poller.go` | Shared poller goroutine, `pollWait`, `PollWaitContext`, `pollUnregister` |
| `vclnet/examples/*` | Echo + HTTP servers/clients (drop-in for stdlib `net`) |
| `vclnet/test/run_integration.sh` | Full integration harness (starts VPP, runs tests) |
| `vclnet/docs/architecture.md` | Architecture + design rationale |
| `vclnet/summary.md` | Implementation summary, bug fixes, gaps |

### VPP — VCL / VLS

| File | Contents |
| --- | --- |
| `src/vcl/vppcom.h` | Public VCL API surface — `vppcom_proto_t`, errors, attrs |
| `src/vcl/vppcom.c` | VCL implementation — `vppcom_app_create`, session ops, MQ pump |
| `src/vcl/vcl_private.h` | `__thread __vcl_worker_index`, `vcl_worker_t`, `vcl_session_t` |
| `src/vcl/vcl_private.c` | `vcl_worker_alloc_and_init` (records `pthread_self`) |
| `src/vcl/vcl_locked.h` | VLS public API — `vls_create`, `vls_*`, `vls_register_vcl_worker` |
| `src/vcl/vcl_locked.c` | VLS implementation — 3 multi-thread modes, locks, RPC clone-and-share |
| `src/vcl/vcl_sapi.c` | App socket API: attach, MQ + segment fd exchange |
| `src/vcl/vcl_cfg.c` | `vcl.conf` parser — `app-scope-local`, `multi-thread-workers`, etc. |
| `src/vcl/ldp.c` | LD_PRELOAD shim (the C-app path, not used by Go) |

### VPP — Session layer

| File | Contents |
| --- | --- |
| `src/vnet/session/session.h` | `session_t`, `session_worker_t` (per-worker pool + MQ) |
| `src/vnet/session/session.c` | Session lifecycle, enqueue/dequeue notify |
| `src/vnet/session/session_node.c` | The session worker node — runs per VPP worker |
| `src/vnet/session/application.h` | `application_t`, `app_worker_t`, `app_listener_t` |
| `src/vnet/session/application.c` | App lifecycle, worker management |
| `src/vnet/session/application_interface.h` / `.c` | Attach/detach API; binding/listen/connect entrypoints |
| `src/vnet/session/application_local.h` | `ct_connection_t` (cut-through) |
| `src/vnet/session/application_local.c` | Cut-through transport: ct_connect, ct_accept_one, ct_session_tx, ct_handle_cleanups |
| `src/vnet/session/segment_manager.h` / `.c` | Per-app-worker fifo segment lifecycle |
| `src/vnet/session/transport.h` / `.c` | Transport vtable + registry |

### VPP — Shared-memory primitives

| File | Contents |
| --- | --- |
| `src/svm/svm_fifo.h` / `.c` | SVM FIFO — SPSC ring with chunks, OOO tracking, notifications |
| `src/svm/fifo_types.h` | `svm_fifo_t`, `svm_fifo_shared_t`, flags (`SVM_FIFO_F_SERVER_CT`, `SVM_FIFO_F_CLIENT_CT`) |
| `src/svm/fifo_segment.h` / `.c` | Segment containing many FIFOs; `FIFO_SEGMENT_F_CUSTOM_USE` for CT |
| `src/svm/message_queue.h` / `.c` | `svm_msg_q_t` — the app⇄VPP MQ (pthread_mutex + condvar / eventfd) |
| `src/svm/ssvm.h` / `.c` | Shared SVM bookkeeping |

---

## Closing Note

The three deep-dive questions are *the* questions for vclnet because they correspond to the three places where the Go runtime's contract and VCL's contract collide:

| Collision | Go's contract | VCL's contract | vclnet's mediation |
| --- | --- | --- | --- |
| **Threading** | Goroutines float across Ms; Ms come and go | All state keyed by `__thread`; pthread death triggers worker teardown | `runtime.LockOSThread` per call, `sync.Map` worker registry, eager worker-0 registration in `AppInit` |
| **FD identity** | `net.Conn` deliberately hides the FD; runtime poller owns FDs | `vlsh` is not a kernel FD and must never be passed to kernel syscalls | `vlsh` lives only inside `internal/vclpoll`; `tcpConn` never exposes it; no Go FD leakage possible |
| **Blocking semantics** | `net.Conn.Read` blocks via `runtime.poller` (epoll on real FDs) which schedules other goroutines | `vls_read` blocks the calling pthread inside VCL/SHM polling | Non-blocking + shared poller parks goroutines on Go channels so Ms aren't held |

The Frida attempts failed because they tried to *lie* to the Go runtime — claim a kernel FD that wasn't real, claim a syscall returned EAGAIN when the runtime poller couldn't process it, claim work was synchronous when really it queued on a single JS thread. Every lie eventually got called.

**vclnet doesn't lie.** It accepts that:

* The `vlsh` is not an FD → `net.Conn` is the right abstraction; expose that, hide vlsh.
* The pthread is the unit of VCL identity → `LockOSThread` makes a goroutine *be* a pthread for the duration of a VCL call.
* VPP's MQ events are not kernel events → don't try to feed them to `runtime.netpoll`; have your own poller and park goroutines on Go channels.

That is why vclnet is the production path and Frida wasn't: it draws the boundary between Go and VCL at exactly the place where both APIs are willing to cooperate (`net.Conn` ⇄ `LockOSThread`-pinned cgo), instead of trying to fuse them by deception.

The normal VCL path can select cut-through when both apps and scopes permit it. The shared poller is implemented today. The remaining scaling work is a mode-2, session-affine worker architecture; performance claims still require a reproducible baseline.
