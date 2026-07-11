# Technical Case: Why vclnet Is the Correct Engineering Direction Over go-frida-vpp

*Addressed to: the author of go-frida-vpp*

---

## Preface: Acknowledging What go-frida-vpp Accomplished

Before anything else — go-frida-vpp demonstrated something real and important. It proved that a Go process can reach VPP's VLS session layer and exercise cut-through paths. The single-entry-point Syscall6 hook design is cleaner than the per-function approach (frida-vpp) — one hook, no LDP layer, direct VLS calls, and a clever SYS_GETPID substitution that neutralizes the kernel without breaking the Go return path. The VCL endpoint structure encoding, the epoll-based connect-wait, and the non-network-syscall passthrough show genuine VPP expertise. That work directly informed the architecture that followed.

This document explains why, despite that working proof-of-concept, the go-frida-vpp approach cannot be made production-safe, and why a CGo wrapper at the `net.Conn` boundary is the structurally correct replacement.

---

## 1. The Core Argument in One Paragraph

go-frida-vpp works because it serializes everything through one hook on one JavaScript thread, for one connection at a time. The moment you need concurrent goroutines, multiple connections, or sustained load, five contracts are violated simultaneously: (1) VLS pthread-local worker ownership, (2) Go's cgocall scheduler/stack/ABI protocol, (3) kernel FD identity expectations from Go's runtime netpoller, (4) goroutine-to-M stability across operations, and (5) VLS lock-bitmap coherence. No Frida-level refinement can fix these because they are structural mismatches between the injection model and the two runtimes' contracts.

---

## 2. VLS Pthread Ownership Is Not Advisory — It Is the Execution Model

This is the critical technical point. Let's trace exactly what happens when `vls_read(vlsh, buf, n)` is called.

### What VLS actually does on entry

```c
// vcl_locked.c — every VLS entry point starts here
static inline void vls_mt_detect(void) {
    if (PREDICT_FALSE(__vcl_worker_index == ~0))
        vls_mt_add();
}
```

`__vcl_worker_index` is an ELF `__thread` variable — 64-bit on amd64, initialized to all-ones. It is not a marker you can copy or spoof from JavaScript. It is a GCC TLS variable whose storage is allocated per-pthread by the dynamic linker, and its value selects a `vcl_worker_t` that owns:

- A `vcl_session_t` **pool** (worker-local index space)
- A `svm_msg_q_t` **pair** to VPP (the only way this worker receives events)
- An epoll bookkeeping table
- A `pthread_t` field recorded at registration time
- Worker-local bitmaps and scratch buffers

In Mode 3, VLS additionally maintains per-pthread lock state:

```c
static __thread vls_mt_pthread_local_t vls_mt_pthread_local;
```

This tracks which locks (MQ mutex, pool read/write lock, per-session spinlock) the current pthread has acquired. Guard/unguard paths update this TLS bitmap. Transferring a call mid-flight to another pthread means:

1. The new pthread's lock bitmap is zero — unguard attempts unlock nothing (leak).
2. The old pthread's bitmap retains set bits — if it enters another VLS call, it believes it already holds locks (double-lock or corruption).
3. The worker index on the new pthread may be `~0` (unregistered) or point to a different worker entirely.

### Why go-frida-vpp's NativeFunction call doesn't fix this

When the Frida hook calls `vpp.vls_read(vlsh, bufPtr, count)` via `NativeFunction`, the call executes on whatever pthread the Go scheduler assigned to that goroutine at hook-fire time. If that goroutine previously initialized VLS on a different pthread (or if a different goroutine previously initialized VLS on *this* pthread), the TLS state is wrong.

Under single-connection testing, this doesn't manifest because:
- One goroutine stays on one M (no scheduling pressure)
- VLS registers the first pthread and always finds it on re-entry
- No concurrent VLS calls race on lock state

Under production load with N goroutines and M-count changes, it is immediately fatal.

---

## 3. NativeFunction Is Not a CGo Call — The Go Runtime Doesn't Know

This is the second fundamental issue. When Go code enters a C function through CGo, the runtime executes a precise protocol in `runtime.cgocall`:

1. **Marks the G as entering foreign code** (`_Gcgo` state)
2. **Releases the P** so the scheduler can reuse it for other goroutines
3. **Switches to the M's g0 system stack** (via `asmcgocall`) — a fixed, large stack that cannot grow or move
4. **Executes the C code on that stable stack**
5. **Returns via a saved depth offset** that remains valid even if Go callbacks caused stack movement
6. **Re-enters schedulable Go state**

A Frida `NativeFunction` call does none of this. The consequences:

| CGo protocol step | What happens without it |
|---|---|
| G marked as foreign | Scheduler may preempt mid-native-call; stack scanner sees invalid frames |
| P released | P stays occupied; if native call blocks, scheduler cannot reuse this P — goroutine starvation |
| Switch to g0 stack | Native code runs on the goroutine's small (8 KB initial) movable stack |
| Stable stack | If GC triggers stack growth concurrently, pointers passed to C become dangling |
| Pointer-rule checks | No CGo pointer-rule validation; a Go-heap pointer passed to VLS can be moved by GC |

The g0 stack switch is especially critical. Go goroutine stacks are:
- **Small** (8 KB initially, growable to 1 GB)
- **Movable** (the GC and `copystack` relocate them and adjust pointers by the stack map)
- **Not suitable for arbitrary C code** (which may use more stack than available, or retain pointers)

VLS internally can perform significant work — MQ drain, session lookup, FIFO memcpy, lock acquisition. A deep VLS call chain on an 8 KB goroutine stack is a stack overflow waiting to happen. This is precisely why frida-vpp documented `SIGSEGV in stackpoolalloc` as a known bug for accept's epoll path — they hit the goroutine stack limit from NativeFunction depth.

---

## 4. The Fake FD Problem Is Structural, Not Surface-Level

go-frida-vpp maps VLS handles to fake file descriptors: `fd = vlsh + 10000`. This seems clean — the hook intercepts `read(fd)`, recognizes `fd >= 10000` as a VPP session, and routes to `vls_read`.

The problem is that Go's runtime and standard library interact with file descriptors in ways the hook cannot fully control:

### 4.1 The runtime netpoller

After `socket()` returns fd=10042, Go's `net` package calls:
```go
// internal to net package
pd.init(fd)  // registers with runtime.pollDesc
runtime_pollOpen(fd)  // calls epoll_ctl(runtime_epollfd, EPOLL_CTL_ADD, 10042, ...)
```

The kernel returns `EBADF` because fd 10042 doesn't exist in the kernel. go-frida-vpp handles this by also intercepting `epoll_ctl` and `epoll_pwait`, but:

1. **The hook clamps epoll_pwait timeout to 50ms** (line 1001 of the script). This means every Go runtime poller iteration that would sleep indefinitely instead returns after 50ms — turning the runtime poller into a 20Hz busy-loop, burning CPU on every idle period.

2. **The hook appends VPP events to kernel epoll results** by copying VLS epoll_wait results into the events buffer. But Go's runtime poller expects `epoll_event.data` to be a `*pollDesc` pointer, not a VLS handle. The mapping between VLS event data and Go's internal poller descriptors is not bridged — the hook writes raw VLS data values into a buffer Go's runtime interprets as pointer-sized control data.

### 4.2 Escape through the standard library

Even if the hook catches all current paths, the fake fd can escape through:
- `fcntl(fd, F_DUPFD)` — the hook handles F_GETFL/F_SETFL but not all fcntl commands
- `fstat(fd)` — Go's `net.Conn` doesn't call this, but diagnostic code might
- `splice(fd, ...)` — used by `io.Copy` for zero-copy on Linux
- `sendfile(fd, ...)` — used by `http.ServeContent`
- `getsockopt(fd, SOL_SOCKET, SO_ERROR)` — Go's connect path uses this for async connect verification

Each unhandled escape is an EBADF crash. The maintenance burden grows with every Go release and every library that touches raw FDs.

### 4.3 vclnet's structural elimination

vclnet declares `vlsh` as an unexported `int32` field inside `internal/vclpoll`. There is no `Fd() int` method, no `SyscallConn()`, no `os.File` conversion. The Go runtime poller never sees it. `splice`, `sendfile`, `dup`, and `fstat` can never receive it. The entire class of problems disappears by construction.

---

## 5. The JavaScript Engine Serialization Bottleneck

Frida's Interceptor serializes JavaScript entry and exit through the script's V8 isolate lock. While `NativeFunction` with the default cooperative scheduling releases this lock during the native call itself, the JS dispatch on both sides of every intercepted syscall is single-threaded.

For go-frida-vpp, **every syscall in the process** passes through this path:
- `onEnter`: acquire JS lock, read RAX (syscall number), decide if it's networking, possibly replace RAX with SYS_GETPID, save state, release JS lock
- `onLeave`: acquire JS lock, if networking: call VLS via NativeFunction, write results to RAX/RCX, release JS lock

Under a realistic Go HTTP server with 100 goroutines:
- Each request involves ~6 networking syscalls (accept + read + write + close on server side)
- Each goroutine also generates ~20-50 non-networking syscalls per request (futex, mmap, clock_gettime, epoll_pwait, nanosleep, sigaltstack, etc.)
- **Every one** of these 2600-5600 syscalls/second per 100 concurrent connections passes through Frida's JS engine

At 10-20µs per intercepted syscall, you're burning 26-112ms of serialized JS time per second per 100 connections — and that's assuming zero contention. Under contention, tail latency degrades to O(N) where N is concurrent goroutines hitting syscalls.

vclnet's CGo transition costs ~100ns per call, runs fully in parallel across goroutines (Mode 3 serializes VLS state but not the Go scheduling), and non-networking code paths pay zero overhead.

---

## 6. Go Version and Architecture Fragility

go-frida-vpp depends on:

1. **Go's internal register ABI** — `RAX=num, RBX=a1, RCX=a2, RDI=a3, RSI=a4, R8=a5, R9=a6` for `Syscall6`. This is internal to `internal/runtime/syscall` and not part of Go's stability promise.

2. **The symbol name `internal/runtime/syscall.Syscall6`** — Go has moved the syscall trampoline before (from `syscall.Syscall6` → `runtime/internal/syscall.Syscall6` → `internal/runtime/syscall.Syscall6`).

3. **Return value convention** — `r1=RAX, r2=RBX, err=RCX`. The hook writes these specific registers.

4. **x86_64 only** — ARM64 uses a completely different register assignment.

5. **Debug symbols present** — Frida uses `DebugSymbol.findFunctionsMatching` to locate the hook target. Stripped binaries cannot be hooked.

Any one of these changing (which happens across Go releases) silently breaks the hook. There is no compile-time signal, no test that catches it before deployment, and no supported API guaranteeing stability.

vclnet links through CGo — the only supported, stable, maintained Go-to-C boundary. The Go team guarantees CGo's ABI across releases and architectures.

---

## 7. Addressing the "But It's Transparent" Argument

The strongest argument for go-frida-vpp is transparency: no application code changes. You attach Frida, and existing Go binaries route through VPP.

This is genuinely appealing for prototyping and exploration. But for production:

| Transparency benefit | Production cost |
|---|---|
| No code change | No type safety — fake FDs can escape at runtime without compile-time detection |
| Works on any Go binary | Only works on non-stripped binaries with specific symbol layout on x86_64 |
| Runtime attachment | Requires Frida runtime on every deployment target; cannot containerize cleanly |
| No import path change | No explicit dependency — breakage is silent, detected only by runtime crash |

vclnet requires changing `net.Listen` → `vclnet.Listen` and `net.Dial` → `vclnet.Dial`. This is a one-line diff per call site. In exchange:

- Compile-time verification that the VPP integration exists
- No runtime injection dependency
- Type-safe interfaces (no fake FDs, no ABI assumptions)
- Works on all architectures CGo supports
- Works with stripped binaries
- Debuggable with standard Go and C tooling

For the service-mesh / sidecar use case where you control the application code, the transparency cost is negligible and the safety gain is decisive.

---

## 8. Could go-frida-vpp Be Fixed?

The analysis in `docs/frida_goroutine_tracking_analysis.md` answers this question directly. A safe Frida-based architecture would need:

1. Long-lived native pthreads registered as VCL workers
2. A per-session dispatcher that routes operations to the owning worker
3. A readiness bridge (eventfd proxies) into Go's kernel epoll
4. Reference-counted session lifecycle management
5. A supported Go-to-native transition (CGo or equivalent)
6. Continuous MQ drain on owner threads
7. Full FD escape-path coverage

At that point, Frida is only the injection mechanism. The actual concurrency solution — native worker pool, session affinity, channel-based readiness, CGo transition — is exactly what vclnet already implements. You end up with vclnet injected via Frida rather than linked via CGo, which adds complexity and removes debuggability for no architectural benefit.

---

## 9. The Production Engineering Case

| Property | go-frida-vpp | vclnet |
|---|---|---|
| Correctness under concurrency | Unverified; serialized by JS engine | Verified by 165 unit + 33 integration + stress tests |
| VLS ownership model | Violated (goroutine migration crosses pthreads) | Honored (Mode 3 pins per-call; Mode 2 pins per-lifetime) |
| Go runtime safety | Bypassed (no cgocall protocol) | Respected (standard CGo boundary) |
| Architecture support | x86_64 only | Any CGo platform |
| Binary requirements | Must retain debug symbols | Works stripped |
| Deployment model | Frida agent + JS file per host | Single binary linked against libvppcom.so |
| Debugging | JS + Go + C interleaved; no tool walks it | delve (Go) + gdb (C); standard stacks |
| Failure mode | Silent corruption, nondeterministic EBADF | Explicit Go errors with *net.OpError wrapping |
| Cut-through access | Yes (single connection) | Yes (concurrent, both modes) |
| Protocol coverage | TCP only | TCP + UDP (connected and unconnected) + TLS + HTTP/1.1 |
| Maintenance burden | Tracks Go runtime internals per release | Zero — CGo ABI is stable |

---

## 10. Conclusion

go-frida-vpp proved the concept. It showed that VLS is reachable from a Go process and that cut-through sessions can be exercised. That proof-of-concept directly shaped the requirements for vclnet.

But the Frida approach has five structural problems that no amount of hook refinement can fix:

1. **VLS's TLS state requires pthread identity** — goroutines don't have one.
2. **Go's cgocall protocol is not optional** — stack safety, pointer lifetime, and scheduler coordination require it.
3. **Fake FDs leak into uncontrolled paths** — the Go runtime and standard library interact with FDs in ways no finite set of hooks can fully cover.
4. **JavaScript serialization bounds throughput** — production workloads need parallel syscall handling.
5. **Internal ABI dependencies break silently across Go versions** — no compile-time safety net.

vclnet solves all five by drawing the boundary at `net.Conn` — the exact point where Go's networking contract and VCL's session contract can honestly cooperate:

- Go owns scheduling, deadlines, cancellation, and interface semantics.
- CGo owns the supported stack/ABI/scheduler transition.
- LockOSThread holds pthread identity across multi-step VLS sequences.
- VLS owns worker state using the pthread contract it was designed for.
- The VLS handle never masquerades as anything it isn't.

The one trade-off is a one-line code change (`vclnet.Listen` instead of `net.Listen`). Every other property — correctness, safety, performance, portability, debuggability, and maintainability — favors the CGo wrapper.

**The Frida prototypes were the right exploration tool. vclnet is the right production tool.**
