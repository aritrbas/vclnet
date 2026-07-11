# Enabling VPP Cut-Through Sessions for Go: Three Approaches Compared

## The Challenge

VPP's cut-through transport connects application FIFOs via shared memory, bypassing the kernel entirely. For C applications, VPP's `LD_PRELOAD` shim intercepts libc socket calls transparently. Go programs, however, issue syscalls directly through the runtime, bypassing libc. VCL handles are not kernel file descriptors and cannot enter Go's network poller. Most critically, VCL stores worker state, session pools, and lock ownership in pthread-local storage, while Go goroutines migrate freely across OS threads. These three mismatches — libc bypass, fake FD incompatibility, and pthread/goroutine identity divergence — define the engineering problem.

Three approaches were evaluated: **vclnet** (a CGo wrapper library), **go-frida-vpp** (a single Frida hook on Go's Syscall6 trampoline), and **frida-vpp** (per-function Frida hooks routing through VPP's LDP shim).

## vclnet: The Production Path

vclnet is a Go package that wraps VPP's VCL/VLS API through CGo, implementing `net.Conn`, `net.Listener`, and `net.PacketConn`. Every VLS call executes on a valid pthread boundary using `runtime.LockOSThread`, and a dedicated VLS epoll poller converts VPP readiness into Go channel wakeups. VLS handles remain entirely internal — they never masquerade as kernel file descriptors.

The package supports two threading modes: Mode 3 (default, shared VCL worker with VLS locks) and Mode 2 (opt-in, N session-affine pinned workers with per-worker epoll and sharded listeners via SO_REUSEPORT). Protocol coverage spans TCP and UDP on IPv4/IPv6, HTTP/1.1, layered `crypto/tls`, native VCL TLS, context-aware dialing with Happy Eyeballs, resettable deadlines, half-close, and graceful shutdown. The test suite includes 165 unit tests, 33 integration tests, multi-worker stress tests, and benchmarks. Applications adopt vclnet by importing the package and calling `vclnet.Listen` / `vclnet.Dial` instead of `net.Listen` / `net.Dial` — all existing Go networking idioms work unchanged.

## Frida Prototypes: Valuable Exploration, Not Production-Viable

**go-frida-vpp** hooks Go's `internal/runtime/syscall.Syscall6` — the single entry point for all Go syscalls on linux/amd64. It intercepts networking syscall numbers, replaces RAX with SYS_GETPID (a no-op), then calls VLS directly via Frida `NativeFunction` in `onLeave`. **frida-vpp** takes a different route, hooking 17 individual Go syscall wrapper functions (syscall.socket, syscall.read, etc.), replacing each function body with a single-byte RET trampoline, and routing calls through VPP's LDP shim layer. Both use fake file descriptors mapped in a JavaScript table.

Both prototypes demonstrated basic TCP echo and HTTP flows over VPP, proving reachability. However, they share five fundamental limitations that cannot be patched:

1. **Unsafe Go runtime transition.** Frida's `NativeFunction` does not invoke Go's `runtime.cgocall` protocol. The scheduler remains unaware of the foreign call; goroutine stacks may grow while C pointers reference stale addresses; the P token is not released.
2. **VCL pthread ownership violation.** VCL's worker index, lock bitmap, session pools, and message queues are pthread-local. Goroutine migration between OS threads causes VLS operations to execute against the wrong (or uninitialized) worker state.
3. **Fake FD leakage.** VLS handles posing as kernel FDs can escape to `epoll_ctl`, `fcntl`, `dup`, `fstat`, or `splice`, all returning EBADF. Every FD surface must be intercepted — an unbounded maintenance obligation.
4. **JavaScript engine serialization.** Frida's single-threaded JS event loop serializes all hook callbacks, imposing 10–20 microseconds of overhead per intercepted syscall and creating a concurrency ceiling.
5. **Go-version and architecture fragility.** Both approaches depend on Go's internal register ABI, symbol naming, and error interface layout — details that change across releases. x86_64 only; binaries must retain debug symbols.

## Recommendation

**Adopt vclnet as the production VPP integration path for Go services.** It is the only approach that correctly addresses VLS pthread ownership, Go scheduler/stack safety, and VPP readiness integration through supported runtime mechanisms. The Frida prototypes should be archived as reference — their documented failure modes directly informed vclnet's architecture.
