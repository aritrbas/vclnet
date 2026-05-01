package vclnet

import (
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"vclnet/internal/vclpoll"
)

var (
	shutdownOnce    sync.Once
	shutdownStarted atomic.Bool
	shutdownDone    = make(chan struct{})
	signalInstalled atomic.Bool
)

// Shutdown performs graceful teardown of the VCL application layer.
// It stops the shared poller goroutine first (so no VCL calls are in-flight),
// then calls vppcom_app_destroy() to deregister from VPP.
// Safe to call multiple times; subsequent calls are no-ops.
//
// After Shutdown is called, all vclnet operations will fail.
func Shutdown() {
	shutdownOnce.Do(func() {
		shutdownStarted.Store(true)
		close(shutdownDone)
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
