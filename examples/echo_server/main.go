// Low-level echo server using the internal VLS bridge.
//
// Each accepted connection runs in its own goroutine. Immediate VLS calls pin
// to an OS thread; EAGAIN waits are multiplexed by the shared poller.
//
// Run:
//
//	VCL_CONFIG=/tmp/vclnet-share/vcl.conf go run ./examples/echo_server
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"vclnet/internal/vclpoll"
)

func main() {
	port := flag.Uint("port", 9876, "TCP port to listen on (via VPP)")
	flag.Parse()

	if os.Getenv("VCL_CONFIG") == "" {
		log.Fatal("VCL_CONFIG env var must be set (e.g. /tmp/server-share/vcl.conf)")
	}

	if err := vclpoll.AppInit("vclnet-echo-server"); err != nil {
		log.Fatalf("AppInit: %v", err)
	}

	// Bind to 0.0.0.0:port
	listener, err := vclpoll.ListenTCP4([4]byte{0, 0, 0, 0}, uint16(*port), 32)
	if err != nil {
		log.Fatalf("ListenTCP4: %v", err)
	}
	log.Printf("[vclnet/echo_server] listening on 0.0.0.0:%d via VPP (vlsh=%d)",
		*port, listener)

	for {
		conn, peerIP, peerPort, err := vclpoll.Accept(listener)
		if err != nil {
			log.Printf("Accept error: %v — exiting", err)
			vclpoll.Close(listener)
			return
		}
		log.Printf("[vclnet/echo_server] accepted conn vlsh=%d from %d.%d.%d.%d:%d",
			conn, peerIP[0], peerIP[1], peerIP[2], peerIP[3], peerPort)

		// Each connection is handled on its own goroutine.
		go serveConn(conn)
	}
}

func serveConn(conn vclpoll.VLSH) {
	defer vclpoll.Close(conn)

	buf := make([]byte, 4096)
	for {
		n, err := vclpoll.Read(conn, buf)
		if err != nil {
			log.Printf("read error on vlsh=%d: %v", conn, err)
			return
		}
		if n == 0 {
			log.Printf("peer closed vlsh=%d", conn)
			return
		}

		// Loop until all bytes are written.
		written := 0
		for written < n {
			w, err := vclpoll.Write(conn, buf[written:n])
			if err != nil {
				log.Printf("write error on vlsh=%d: %v", conn, err)
				return
			}
			written += w
		}
		fmt.Printf("[vclnet/echo_server] echoed %d bytes back to vlsh=%d\n", n, conn)
	}
}
