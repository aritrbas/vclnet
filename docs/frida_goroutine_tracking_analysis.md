# Frida Goroutine Tracking vs. VLS Thread Ownership

## Status and scope

This document answers a narrow design question:

> Can Frida identify the goroutine entering a hooked networking call and use
> that identity as a shim equivalent to VLS's pthread tracking?

The short answer is **not by itself**. A Frida agent can be made to recognize
the current goroutine for one specific Go release, architecture, and hook
location. That solves an observability problem. VLS has an ownership problem:
its worker selection, lock bookkeeping, session pools, message queues, and
cleanup are tied to the current **pthread**. A Go goroutine is intentionally
not tied to a pthread.

A sufficiently large native Frida agent could still work. It would need
long-lived native VLS owner threads, a per-session dispatcher, a readiness
bridge into kernel epoll, reference-counted close/cancellation handling, and a
Go-ABI-safe entry path. At that point Frida is only the injection mechanism;
the concurrency solution is effectively the same native worker/poller layer
that the CGo design makes explicit and testable.

This analysis distinguishes:

1. facts verified in the pinned VPP and Go sources;
2. failure mechanisms that follow from those sources;
3. historical crash hypotheses that are not established by the current
   source.

Source baseline: VPP commit
`39b2a4eca5527466eb862a96d9dd9d608eb15f6f`, the local Go 1.26.1 amd64
runtime sources, and Frida's official JavaScript API documentation. Internal
Go register/layout observations must be revalidated for any other toolchain.

The current repository uses VLS mode 3, pins around immediate VLS operations,
and has one lifetime-pinned VLS epoll poller. Mode 2 requires session-to-worker
affinity and is a separate design step.

## 1. Executive conclusion

There are three independent contracts. A goroutine tracker addresses only the
first:

| Contract | Required identity | Does a G tracker solve it? |
| --- | --- | :---: |
| Identify the logical Go task | goroutine (G) | Yes, with unsupported runtime-specific techniques |
| Execute VLS against the correct state | pthread/VCL worker/session owner | No |
| Park and wake Go I/O efficiently | kernel-pollable readiness plus Go scheduler state | No |

The key distinction is:

> **Identity is not authority.** Knowing which goroutine made a call does not
> make that goroutine's current pthread own the VLS worker or session.

The safe choices for unmodified Go and current VPP are:

- call VLS through CGo and keep VLS handles behind a Go-native
  `net.Conn`/`net.Listener` abstraction; or
- build an equivalent compiled native bridge with explicit owner-thread
  dispatch and a poller.

A pure Frida JavaScript goroutine map cannot provide either contract. CGo is
not the only solution that is theoretically possible, but it is the supported
Go runtime boundary and the smallest production-shaped solution here.

## 2. What VLS actually checks

### 2.1 It is not a 12-bit zero marker

The VCL source declares:

~~~c
__thread uword __vcl_worker_index = ~0;
~~~

This is an ELF pthread-local variable. On a 64-bit build, `~0` is
64 one-bits; on a 32-bit build it is 32 one-bits. It is neither twelve bits nor
a value found by examining the stack.

On entry to a VLS operation, `vls_mt_detect()` tests whether the
current pthread's `__vcl_worker_index` is still `~0`. If
so, `vls_mt_add()` initializes that pthread for the configured VLS
multi-thread mode.

The relevant source is:

- [`__vcl_worker_index` declaration](https://github.com/FDio/vpp/blob/39b2a4eca5527466eb862a96d9dd9d608eb15f6f/src/vcl/vppcom.c#L11)
- [TLS accessors](https://github.com/FDio/vpp/blob/39b2a4eca5527466eb862a96d9dd9d608eb15f6f/src/vcl/vcl_private.h#L27-L38)
- [VLS detection and registration](https://github.com/FDio/vpp/blob/39b2a4eca5527466eb862a96d9dd9d608eb15f6f/src/vcl/vcl_locked.c#L1310-L1315)

### 2.2 The TLS value selects real worker state

The index is not merely a diagnostic label. It selects a
`vcl_worker_t` containing, among other state:

- that worker's VCL session pool;
- VPP worker and application-worker indices;
- control and event message queues;
- epoll bookkeeping;
- worker-local bitmaps and buffers;
- the recorded `pthread_t`.

In mode 2, a newly observed pthread calls
`vppcom_worker_register()` and receives a real VCL worker. In mode
3, the pthread's TLS is pointed at the shared initial worker and VLS locks
protect that shared state.

~~~mermaid
flowchart LR
    Call[VLS API entry] --> Detect{TLS worker index is all ones?}
    Detect -- no --> Select[Select vcl_worker_t]
    Detect -- yes --> Add[vls_mt_add]
    Add --> Mode{VLS mode}
    Mode -- mode 2 --> Register[Register real VCL worker]
    Mode -- mode 3 --> Share[Use initial shared worker]
    Register --> TLS[Set pthread TLS worker index]
    Share --> TLS
    TLS --> Select
    Select --> Pool[Worker session pool]
    Select --> MQ[Control and event MQs]
    Select --> Epoll[VCL epoll state]
~~~

### 2.3 VLS has more pthread TLS than the worker index

VLS also declares a pthread-local lock record. Its bits track whether this
pthread currently owns the message-queue lock and pool/spool read or write
locks. Guard and unguard paths update that record.

That detail matters: copying only `__vcl_worker_index` to another
pthread does not copy an in-progress lock state, and treating a different
pthread as the same owner can cause an unlock omission, a wrong-thread unlock,
or concurrent access to worker-local state.

See [the VLS pthread-local lock record and lock bits](https://github.com/FDio/vpp/blob/39b2a4eca5527466eb862a96d9dd9d608eb15f6f/src/vcl/vcl_locked.c#L152-L179).

### 2.4 Cleanup follows pthread lifetime

VLS installs a `pthread_key_create` destructor. When the actual
pthread exits, `vls_mt_del()` releases its VLS lock state and, in
mode 2, unregisters its VCL worker.

This gives VLS a coherent lifecycle:

~~~text
pthread created
  -> TLS starts at ~0
  -> first VLS call registers/selects a worker
  -> every VLS call on that pthread sees the same TLS
  -> pthread exits
  -> pthread-key destructor drops locks and unregisters the worker
~~~

A goroutine may live across many pthreads, and a pthread may execute thousands
of goroutines. A pthread destructor therefore cannot be used as a goroutine
destructor, and Go exposes no supported goroutine-destructor hook.

### 2.5 Session handles already encode worker ownership at the VCL layer

A `vppcom_session_handle_t` is 32 bits. In this VPP source it is
partitioned as:

~~~text
31                    24 23                                0
+-----------------------+-----------------------------------+
| VCL worker index (8)  | worker-local session index (24)   |
+-----------------------+-----------------------------------+
~~~

The public VLS handle is a locked-session pool index, not simply that packed
VCL handle. VLS keeps the owner/current-worker information and, in mode 2, can
clone or share a session into another worker through an RPC. This is why
cross-worker use is a state transition rather than an ID-table lookup.

Relevant source:

- [VCL handle bit layout](https://github.com/FDio/vpp/blob/39b2a4eca5527466eb862a96d9dd9d608eb15f6f/src/vcl/vcl_private.h#L446-L470)
- [VLS locked-session structure](https://github.com/FDio/vpp/blob/39b2a4eca5527466eb862a96d9dd9d608eb15f6f/src/vcl/vcl_locked.c#L102-L113)
- [mode-2 migration decision](https://github.com/FDio/vpp/blob/39b2a4eca5527466eb862a96d9dd9d608eb15f6f/src/vcl/vcl_locked.c#L1205-L1225)

## 3. Why pthread identity works for VLS

For native C applications, the following identities normally line up for the
duration of an operation:

~~~text
call stack -> pthread -> ELF TLS -> VCL worker -> worker-local session state
~~~

The OS does not migrate a running pthread's stack to another pthread. Function
entry and exit execute with the same pthread TLS. VLS can therefore use
`__thread` state both to select a worker and to record nested lock
ownership.

This is not because pthread stacks contain a magic recognizable signature. It
works because pthread identity and ELF TLS are part of the platform ABI, and
their lifetime is controlled by the same object.

## 4. Go's G/M/P model at memory level

### 4.1 The three objects

Go schedules many goroutines (G) over a smaller set of OS threads (M), with a
logical processor token (P) required to execute Go code.

~~~mermaid
flowchart TB
    G1[G1: user goroutine] --> P1[P0]
    G2[G2: user goroutine] -. runnable queue .-> P1
    P1 --> M1[M7: pthread 1017]
    G3[G3: user goroutine] --> P2[P1]
    P2 --> M2[M12: pthread 1024]
    M1 --> G0A[M7.g0 system stack]
    M2 --> G0B[M12.g0 system stack]
~~~

- **G** owns scheduler state and a small, growable user stack.
- **M** represents an OS thread and owns a fixed system goroutine
  (`g0`) and system stack.
- **P** owns execution resources and run queues. It can move between Ms when an
  M blocks in a known syscall or CGo call.

The runtime definitions are in
[`runtime2.go`](https://go.dev/src/runtime/runtime2.go).

### 4.2 A goroutine stack is movable memory

A goroutine starts with a small runtime-managed stack. When more space is
needed, `copystack`:

1. allocates a larger stack region;
2. copies live frames;
3. adjusts pointers described by Go stack maps;
4. installs new `stack.lo`/`stack.hi` bounds;
5. frees the old stack region.

~~~text
before growth                         after growth

old stack [0x7000..0x7800]            old region: reusable
+-----------------------+             new stack [0x9100..0xa100]
| frame A: &frame B ----|---+         +-----------------------+
| frame B: buffer       |   | copy    | frame A: &frame B'    |
+-----------------------+   +------->  | frame B': buffer      |
                                      +-----------------------+
~~~

Consequences:

- the current stack pointer is not a stable goroutine identifier;
- stack-low/high is not a stable identity tuple;
- a pointer into a G stack becomes stale if foreign code retains it beyond a
  supported call boundary and the stack later grows;
- two goroutines executing the same function have indistinguishable code
  return addresses in a backtrace.

See [`copystack` in the Go runtime](https://go.dev/src/runtime/stack.go).

### 4.3 The internal G pointer is better than the stack, but still unsupported

On current amd64 Go builds, generated Go code reserves `R14` for the
current `*runtime.g`. Other architectures use different mechanisms.
Frida can inspect the intercepted CPU context and, at selected Go-code hook
points, read that register.

This is useful for diagnostics, but not a stable API:

- the register and structure layout are internal ABI details;
- offsets change between Go releases and architectures;
- runtime, signal, `g0`, and foreign-code contexts do not all carry
  the user G in the same way;
- a `g` object is returned to a runtime free list and reused for a
  later goroutine;
- `g.goid` is internal, has no supported “current goroutine ID”
  accessor, and still provides identity rather than VLS ownership.

The allocation/reuse paths are visible in
[`newproc1`, `gfget`, and `gfput`](https://go.dev/src/runtime/proc.go).

### 4.4 Goroutine and pthread lifetimes cross each other

~~~mermaid
sequenceDiagram
    participant G as goroutine G42
    participant M1 as M7 / pthread A
    participant M2 as M12 / pthread B
    participant VLS as VLS pthread TLS

    G->>M1: run socket operation
    M1->>VLS: TLS selects worker WA
    Note over G,M1: G blocks or is preempted
    G->>M2: later resumes on another M
    M2->>VLS: TLS is uninitialized or selects WB
    Note over VLS: same G, different worker context
~~~

The reverse is equally important: after G42 blocks, pthread A may immediately
run G99. A map from pthread to “current goroutine owner” becomes stale as soon
as the scheduler switches Gs.

## 5. What Frida can and cannot observe

Frida's Interceptor provides the target thread's register context at the hook.
Its thread backtrace reports return addresses in executable code. It does not
provide a supported Go scheduler identity.

A version-pinned agent can infer a G using `R14` on amd64 and a
hard-coded `runtime.g` layout. That inference has four limitations:

1. it must be validated for every Go version, build mode, and architecture;
2. it may observe `g0`, `gsignal`, or no valid user G at
   runtime/foreign-code boundaries;
3. a raw `*g` key is reused after the goroutine dies;
4. the result says nothing about which VLS worker owns a session.

Backtrace hashing is weaker still. A backtrace is a sequence of code PCs, so
many goroutines running the same call path produce the same value. Stack
addresses split one G into multiple identities after stack growth and can be
reused by a different G later.

Frida also has a per-script JavaScript lock. Hook callbacks entering JavaScript
must acquire it. A `NativeFunction` uses cooperative scheduling by
default, which releases that lock during the native call, so it is too broad
to say that all native work is always serialized. The JavaScript dispatch
before and after the native call is serialized, while native VLS calls may
overlap and race unless the design adds its own ownership/locking.

See Frida's official
[JavaScript API documentation](https://frida.re/docs/javascript-api/),
including `NativeFunction` scheduling and Interceptor performance
guidance.

## 6. Why a G-to-worker map does not fix VLS

Assume a Frida tracker reliably obtains `goid=42`.

### 6.1 Mapping G to the current pthread is instantly stale

G42 can call on pthread A now and pthread B later. VLS reads pthread B's ELF
TLS; it never consults the Frida map.

The agent could forcibly write B's `__vcl_worker_index` to A's
worker index, but that is unsafe:

- A may concurrently be using the worker;
- B's VLS lock bitmap is not A's lock bitmap;
- the worker records and depends on pthread-local execution state;
- message-queue and session-pool operations now run from an unregistered owner;
- A's pthread destructor may unregister the worker while G42 still refers to
  it.

### 6.2 Mapping one worker per G is the wrong cardinality

The relationships are many-to-many:

~~~text
one G -> many connections
one connection -> many Gs (concurrent Read, Write, Close, deadline update)
one G -> many Ms over time
one M -> many Gs over time
~~~

Go explicitly permits one goroutine to read while another writes the same
`net.Conn`. A connection can outlive the goroutine that created it.
Therefore session ownership must be keyed by session/fake-FD, not by the
currently executing G.

### 6.3 A tracker cannot manufacture VLS lock ownership

VLS's pthread-local lock bits answer “which locks has this pthread acquired in
this nested VLS path?” They are not a mutex that Frida can replace with a
goroutine ID. Changing the key would require rewriting every guard/unguard
path, the worker lookup paths, nested RPC handling, and cleanup semantics.

### 6.4 A tracker cannot create a goroutine destructor

VLS knows when a pthread ends. The Go runtime does not publish a hook when a G
is recycled. A finalizer is not equivalent: finalizers attach to heap objects,
run nondeterministically on another goroutine, and do not identify the VLS
calls made by the dead G.

### 6.5 Mode 3 already gives the safe form of sharing

In mode 3, VLS deliberately points initialized pthreads at the initial worker
and uses VLS locks. Tracking Gs adds no correctness. The CGo wrapper still pins
across its registration-plus-operation sequence so the pthread registered
before the call is the pthread that performs it.

In mode 2, true parallelism comes from multiple real VCL workers. Correctness
then requires a stable **session-to-owner-worker** route. A goroutine ID is not
part of that route.

## 7. Why the direct Frida call path is memory-unsafe

The concern is broader than VLS's TLS. A call from Go to arbitrary C code has a
runtime protocol.

### 7.1 The supported CGo transition

The Go runtime's `cgocall` path:

1. marks the G as entering foreign/blocking execution;
2. lets the scheduler detach or reuse the P;
3. switches from the small movable G stack to the M's system
   `g0` stack in `asmcgocall`;
4. invokes code using the platform C ABI;
5. restores the Go stack using a depth offset that remains valid even if a Go
   callback caused stack movement;
6. re-enters schedulable Go state.

~~~mermaid
flowchart LR
    Go[Go frame on movable G stack] --> Cgo[runtime.cgocall]
    Cgo --> State[enter foreign-call scheduler state]
    State --> G0[switch SP to M.g0 stack]
    G0 --> C[C ABI: VLS]
    C --> Restore[restore Go stack by saved depth]
    Restore --> Exit[exit foreign-call state]
    Exit --> Go2[resume Go]
~~~

Source:

- [`runtime/cgocall.go`](https://go.dev/src/runtime/cgocall.go)
- [amd64 `asmcgocall`](https://go.dev/src/runtime/asm_amd64.s)
- [CGo pointer rules](https://pkg.go.dev/cmd/cgo#hdr-Passing_pointers)

`runtime.LockOSThread` serves a separate purpose in this repository.
It holds the same M across multiple Go/CGo crossings—for example, checking the
pthread registry and then making the VLS call. CGo itself keeps one native call
on its executing M; the explicit lock closes the migration window between
separate calls.

### 7.2 A Frida NativeFunction is not a CGo call

A Frida `NativeFunction` invocation uses Frida's native-call
machinery. It does not automatically execute Go's
`runtime.cgocall` protocol.

Depending on the hook and Frida trampoline, foreign frames may execute inline
on the intercepted thread or on an agent-managed stack. Neither possibility
is a supported Go transition unless an actual compiled CGo entry point is
called:

- the Go scheduler may still believe the G is executing Go;
- the P may remain occupied while the native operation blocks;
- stack scanning and traceback do not have the CGo state they expect;
- pointers passed from intercepted Go arguments get no CGo lifetime,
  pinning, or pointer-rule checks;
- a retained pointer into a movable G stack can point at freed/reused stack
  memory after growth;
- a callback into Go lacks the generated CGo callback transition.

This does not prove that every direct native call crashes immediately. It
means the call violates the runtime's supported memory and scheduling
contract, so success under light load is not evidence of safety.

### 7.3 Hook placement can bypass syscall scheduler state

Go's high-level syscall wrappers call `entersyscall` and
`exitsyscall` around the raw operation. Replacing the wrapper can
bypass those transitions. Hooking below `entersyscall` is not a
general escape hatch either: the G may be in `_Gsyscall`, where
stack growth is forbidden, while the Frida callback performs arbitrary work
the runtime never expected at that PC.

The exact Go ABI also changes over time. On amd64, Go's internal ABI uses a
Go-defined register assignment; the Linux syscall ABI and SysV C ABI use
different assignments. Reading a syscall number from the wrong register or
writing only one of a multi-value Go return can corrupt control data long
before the visible crash.

### 7.4 Why corruption often appears “inside VPP”

An ownership or ABI violation can first damage:

- a VCL pool header or free-list link;
- an SVM FIFO pointer;
- an event-queue element;
- a Go stack slot interpreted with the wrong ABI;
- a buffer whose Go lifetime ended while C still used it.

The crash occurs later, when an allocator or queue follows the damaged
pointer. The faulting instruction may be in VPP, libc, the Go allocator, or
the garbage collector even though the corrupting event happened much earlier.

### 7.5 Correction: the current VCL heap is not mapped at a fixed address

The inspected VPP build's `vcl_heap_alloc()` calls
`mmap(0, heapsize, ...)`, then initializes that returned region with
`clib_mem_init`. Passing zero asks the kernel to choose a free,
non-overlapping virtual address.

Therefore the claim “VCL initializes a fixed-address heap that collides with
new goroutine stacks” is **not established for this source**. An older build or
different component could use explicit mappings, but that must be demonstrated
from that exact binary and its `/proc/PID/maps`.

The source-audited risks are the unsupported Go-to-C transition, ABI
manipulation, stale Go pointers, and wrong VLS pthread/worker ownership. Those
are sufficient to explain nondeterministic corruption without a fixed-address
collision.

See [`vcl_heap_alloc()`](https://github.com/FDio/vpp/blob/39b2a4eca5527466eb862a96d9dd9d608eb15f6f/src/vcl/vcl_private.c#L960-L995).

## 8. Goroutine tracking does not solve readiness or fake FDs

VLS handles are process-local user-space handles, not kernel file descriptors.
Go's standard networking path expects a real FD that can be registered with
the runtime's epoll instance.

If a hook returns `EAGAIN` for a fake VLS FD, normal Go code may
eventually execute:

~~~text
epoll_ctl(runtime_epoll_fd, ADD, fake_vlsh, ...)
~~~

The kernel correctly returns `EBADF`. If the hook blocks instead,
the goroutine cannot park through normal netpoll, and the hook occupies a
thread/agent path while waiting. A fake FD can also escape through
`fcntl`, `dup`, `fstat`,
`getsockopt`, `splice`, or `close`.

A complete transparent shim must either intercept the entire FD semantic
surface or create a real kernel readiness proxy such as
`eventfd`. In the latter design, a native VLS poller must drain VPP
message queues and signal the proxy. That is a poller architecture, not a
goroutine tracker.

## 9. Could a serious Frida shim be made to work?

Yes, but its safe architecture looks like this:

~~~mermaid
flowchart LR
    Hook[Frida syscall hooks] --> Ingress[ABI-safe native ingress]
    Ingress --> Dir[fake FD to session/owner directory]
    Dir --> Q1[worker 0 command queue]
    Dir --> Q2[worker 1 command queue]
    Q1 --> W1[pinned pthread / VCL worker 0]
    Q2 --> W2[pinned pthread / VCL worker 1]
    W1 --> VLS[VLS and VPP MQs]
    W2 --> VLS
    VLS --> Poll[dedicated VLS poller]
    Poll --> Event[eventfd readiness proxies]
    Event --> Epoll[Go runtime kernel epoll]
~~~

It would require all of the following:

1. Keep JavaScript on the control plane; put hot-path dispatch in C/Rust or a
   Frida CModule.
2. Create long-lived native pthreads and register one VCL worker per owner.
3. Bind each session to an owner and route every operation to that owner.
4. Serialize or define ordering for concurrent read, write, close, deadline,
   shutdown, and cancellation operations on one session.
5. Maintain refcounts so a close cannot free a session while a queued command
   or readiness event still references it.
6. Drain VLS/VPP message queues continuously.
7. Provide real kernel-pollable proxies or replace the complete Go netpoll
   behavior.
8. Cover every FD escape path, including duplication and process lifecycle.
9. Use a compiled Go/CGo bridge or version-specific runtime integration so
   entry, exit, callbacks, and pointer lifetimes obey Go's rules.
10. Revalidate hooks and ABI decoding for every supported Go release and
    architecture.

Notice that goroutine identity is not required in this architecture. The
stable key is `session -> owner worker`. Goroutines submit commands
and wait for completion; they may migrate freely.

This design is not “Frida plus a map.” It is an injected user-space networking
runtime. It is possible, but it is at least as complex as the explicit CGo
worker/poller design and is harder to test, version, debug, and deploy.

## 10. Would adding a goroutine field to VPP help?

### 10.1 A field is metadata, not a synchronization model

Adding `uint64_t goid` to `vcl_worker_t` or
`vcl_session_t` would record a number. It would not:

- change the current pthread's `__vcl_worker_index`;
- transfer a session pool or message queue to another worker;
- make VLS lock ownership goroutine-local;
- create a goroutine lifecycle callback;
- integrate a VLS handle with Go's netpoller;
- perform the Go-to-C stack and scheduler transition.

### 10.2 Server-side VPP cannot inspect a Go stack

VPP normally runs in a separate process. It sees application-worker
registrations and shared-memory messages, not the Go runtime's address space.
The application-local `libvppcom`/VCL code is the only VPP-side
component that could inspect a local Go G, and doing so would make VCL depend
on private Go runtime layouts.

### 10.3 Storing a Go pointer is worse than storing a number

VPP/VCL must not retain a `*runtime.g` or a pointer into a Go stack:

- `runtime.g` is private and reused;
- stack pointers can move;
- retaining Go pointers in C memory is restricted by CGo pointer rules;
- a VPP process could not dereference the pointer at all.

A numeric `goid` avoids the raw pointer but still needs unsupported
extraction, has no cleanup API, and says nothing about worker ownership.

### 10.4 A useful upstream API would be language-neutral

If VCL were redesigned, the useful abstraction would be an explicit execution
context, not a Go-specific field. Conceptually:

~~~c
vcl_context_t vcl_context_create(const vcl_context_cfg_t *);
int vcl_session_create_on(vcl_context_t, int proto, int nonblocking);
ssize_t vcl_session_read_on(vcl_context_t, vcl_session_handle_t, void *, size_t);
ssize_t vcl_session_write_on(vcl_context_t, vcl_session_handle_t,
                             const void *, size_t);
void vcl_context_destroy(vcl_context_t);
~~~

For this to help, VCL would need to remove implicit pthread-TLS lookups from
the relevant paths and make session/worker ownership explicit and validated.
Handles would need stable cross-context semantics, or calls would need
mandatory dispatch to the owning context.

That would benefit Go, Rust async executors, fibers, and user-level threading
runtimes. It is also a substantial VCL redesign, not one new struct member.
Go would still need an adapter for `net.Conn`, deadlines,
cancellation, and readiness.

## 11. Why the CGo wrapper is the current design boundary

The wrapper solves all three contracts at their natural boundaries:

~~~mermaid
flowchart LR
    App[Go application] --> Net[net.Conn / net.Listener]
    Net --> Wait[Go channels and deadlines]
    Wait --> Poller[pinned VLS epoll poller]
    Net --> Pin[LockOSThread across registration and operation]
    Pin --> Cgo[CGo scheduler, stack, ABI transition]
    Cgo --> VLS[VLS pthread TLS and locks]
    VLS --> VPP[VPP session layer]
~~~

- Go owns goroutine scheduling, deadlines, cancellation, and public network
  semantics.
- CGo owns the supported Go/C ABI, stack, scheduler, and pointer transition.
- `LockOSThread` holds one pthread across the multi-step VLS entry
  sequence.
- VLS owns worker/session/MQ state using the pthread contract it implements.
- The VLS handle never masquerades as a kernel FD.
- A dedicated VLS epoll loop converts readiness into Go wakeups.

For mode 3, VLS shares one application worker and serializes its protected
state. For a future mode-2 worker pool, each owner must remain on a
lifetime-pinned pthread and operations must route by session affinity. That
pool changes VLS ownership; it does not need to identify the calling G.

## 12. Decision matrix

| Design | VLS ownership correct | Go scheduler/stack supported | Go netpoll compatible | Go-version stability | Transparent to app |
| --- | :---: | :---: | :---: | :---: | :---: |
| Frida JS plus G-ID map | No | No | No | Low | Mostly |
| Native Frida owner-worker runtime | Possible | Only with an additional supported bridge | Only with FD proxies/full emulation | Low | Possible |
| Current CGo `net.Conn` wrapper, mode 3 | Yes | Yes | Separate VLS poller | High | Import/API change |
| CGo session-affine worker pool, mode 2 | Yes, if routing/lifecycle are complete | Yes | Per-owner poller/aggregation required | High | Import/API change |
| Upstream explicit VCL context API plus Go adapter | Potentially best | Adapter still required | Adapter still required | High after upstreaming | Import/API change |

## 13. How to validate these claims experimentally

The following experiments can distinguish identity bugs from allocator
hypotheses:

1. At every hook and VLS entry, log:
   `pthread_self()`, inferred `*g`/`goid`,
   `__vcl_worker_index`, VLS handle, and operation sequence.
   Demonstrate one G on multiple pthreads and one session used by multiple Gs.
2. Force stack growth with deep recursion and large live stack frames. Show
   that SP and stack bounds change while `goid` remains the same.
3. Compare `/proc/PID/maps` immediately before and after
   `vls_app_create`. The current VCL heap should occupy a
   kernel-selected, non-overlapping mapping.
4. Run the same VLS call through CGo and direct Frida invocation while tracing
   Go scheduler state. The CGo path should enter the runtime's foreign-call
   transition; the direct path will not.
5. Under mode 2, log session owner worker and executing pthread. Intentionally
   issue the same session from two workers and observe migration/clone paths or
   ownership failures.
6. Return `EAGAIN` for a fake VLS handle and trace the subsequent
   `epoll_ctl`/`fcntl` calls. This demonstrates that G
   tracking is independent of readiness integration.

Any claim of a fixed-address collision should include the exact VPP commit,
the requested mapping address, and the relevant process maps. Without that
evidence it should remain a hypothesis.

## 14. Final answers to the original questions

**Can Frida maintain a goroutine tracker?** Yes, as a version- and
architecture-specific diagnostic. It can inspect internal G state at carefully
chosen Go-code hooks. Stack/backtrace hashing is not reliable.

**Can that tracker emulate VLS pthread tracking?** No. VLS's TLS is the entry
point to real pthread-owned worker, lock, session-pool, and MQ state. A G ID
does not transfer or serialize that state.

**Can Frida be made safe anyway?** Only by adding native owner threads,
per-session dispatch, lifecycle management, readiness proxies, and a supported
Go/native transition. That is an alternative packaging of a complex wrapper,
not a lightweight shim.

**Can VPP add a goroutine member?** It can add metadata, but the field would
not solve execution ownership, cleanup, stack safety, or netpoll. A
language-neutral explicit-context VCL API would be a meaningful upstream
direction.

**Is CGo mathematically mandatory?** No: a patched Go runtime, a redesigned
VCL API, or a complete native injected runtime could also work. For current
unmodified Go plus current VPP, CGo (or a compiled bridge that ultimately uses
the same runtime protocol) is the supported and substantially safer boundary.

## 15. Source map

### VPP/VCL

- [VCL worker TLS](https://github.com/FDio/vpp/blob/39b2a4eca5527466eb862a96d9dd9d608eb15f6f/src/vcl/vppcom.c#L11)
- [VCL worker structure and handles](https://github.com/FDio/vpp/blob/39b2a4eca5527466eb862a96d9dd9d608eb15f6f/src/vcl/vcl_private.h)
- [VCL worker allocation/registration](https://github.com/FDio/vpp/blob/39b2a4eca5527466eb862a96d9dd9d608eb15f6f/src/vcl/vcl_private.c#L270-L307)
- [VLS multi-thread detection, locks, migration, and cleanup](https://github.com/FDio/vpp/blob/39b2a4eca5527466eb862a96d9dd9d608eb15f6f/src/vcl/vcl_locked.c)
- [VCL heap allocation](https://github.com/FDio/vpp/blob/39b2a4eca5527466eb862a96d9dd9d608eb15f6f/src/vcl/vcl_private.c#L960-L995)

### Go runtime

- [runtime data structures](https://go.dev/src/runtime/runtime2.go)
- [goroutine stack copying](https://go.dev/src/runtime/stack.go)
- [goroutine allocation and reuse](https://go.dev/src/runtime/proc.go)
- [CGo call protocol](https://go.dev/src/runtime/cgocall.go)
- [amd64 CGo stack transition](https://go.dev/src/runtime/asm_amd64.s)
- [runtime implementation conventions](https://go.dev/src/runtime/HACKING)

### Frida

- [Frida JavaScript API](https://frida.re/docs/javascript-api/)
- [Frida Stalker and Interceptor best practices](https://frida.re/docs/best-practices/)
