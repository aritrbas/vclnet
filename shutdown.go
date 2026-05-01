package vclnet

import (
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aritrbas/vclnet/internal/vclpoll"
)

var (
	shutdownOnce    sync.Once
	shutdownStarted atomic.Bool
	shutdownDone    = make(chan struct{})
	signalInstalled atomic.Bool

	// defaultDrainTimeout bounds how long Shutdown waits for tracked
	// connections and pending dials to finish naturally before it forces
	// them closed. Chosen to be small enough for tests, large enough for a
	// service to flush in-flight HTTP responses.
	defaultDrainTimeout = 5 * time.Second
)

// Shutdown performs process-final teardown of the VCL application layer.
//
// Ordering:
//
//  1. Mark shutdownStarted so new public operations fail fast.
//  2. Close every tracked listener (stops admitting new work; wakes blocked
//     Accept calls).
//  3. Wait up to defaultDrainTimeout for tracked connections, PacketConns,
//     and in-flight dials to finish; if the timeout elapses, force-close any
//     stragglers so their goroutines observe ErrClosed and unpark.
//  4. Ask the dispatcher to reject further parked-operation re-entry,
//     stop the readiness poller, and destroy the VCL application.
//
// Safe to call multiple times; subsequent calls are no-ops.
func Shutdown() {
	ShutdownWithTimeout(defaultDrainTimeout)
}

// ShutdownWithTimeout is Shutdown with an explicit drain deadline. A zero
// timeout waits indefinitely; a negative timeout skips the drain entirely
// and force-closes immediately.
func ShutdownWithTimeout(drainTimeout time.Duration) {
	shutdownOnce.Do(func() {
		shutdownStarted.Store(true)
		close(shutdownDone)

		// 1. Close listeners first so no new connections or accepts land
		//    after this point. Each Close wakes blocked AcceptContext
		//    callers, which observe shutdownStarted and return ErrClosed.
		for _, l := range live.snapshotListeners() {
			_ = l.Close()
		}

		// 2. Give in-flight dials, active reads, and active writes a chance
		//    to finish. Force-close anything still tracked once the window
		//    closes.
		if drainTimeout >= 0 {
			live.waitDrain(drainTimeout)
		}
		for _, c := range live.snapshotConns() {
			_ = c.Close()
		}
		for _, pc := range live.snapshotPacketConns() {
			_ = pc.Close()
		}

		// 3. Now that no application goroutine will re-enter VLS with a
		//    live handle, tear down the dispatcher and destroy the app.
		vclpoll.BeginShutdown()
		vclpoll.StopPoller()
		vclpoll.AppDestroy()
	})
}

// ShutdownDone returns a channel that is closed when Shutdown has been called.
func ShutdownDone() <-chan struct{} {
	return shutdownDone
}

// InstallSignalHandler registers a signal handler for SIGINT and SIGTERM
// that calls Shutdown() before exiting. This ensures VPP is notified of
// the application's departure and can clean up session state immediately
// rather than waiting for timeout-based detection.
//
// Safe to call multiple times; subsequent calls are no-ops.
func InstallSignalHandler() {
	if !signalInstalled.CompareAndSwap(false, true) {
		return
	}
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ch
		Shutdown()
		os.Exit(0)
	}()
}
