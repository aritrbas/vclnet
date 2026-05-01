// Echo server using the vclnet public API (net.Listener interface).
//
// This demonstrates the drop-in replacement: the only difference from
// a standard net.Listen() program is the import path.
//
// Run:
//
//	VCL_CONFIG=/tmp/vclnet-share/vcl.conf go run ./examples/echo_server_net
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"vclnet"
)

func main() {
	port := flag.Int("port", 9876, "TCP port to listen on via VPP")
	flag.Parse()

	if os.Getenv("VCL_CONFIG") == "" {
		log.Fatal("VCL_CONFIG env var must be set")
	}

	if err := vclnet.Init("echo-server-net"); err != nil {
		log.Fatalf("vclnet.Init: %v", err)
	}

	addr := fmt.Sprintf(":%d", *port)
	ln, err := vclnet.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("vclnet.Listen: %v", err)
	}
	defer ln.Close()
	log.Printf("[echo_server_net] listening on %s via VPP", ln.Addr())

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			return
		}
		log.Printf("[echo_server_net] accepted from %s", conn.RemoteAddr())
		go handleEcho(conn)
	}
}

func handleEcho(conn net.Conn) {
	defer conn.Close()
	n, err := io.Copy(conn, conn)
	if err != nil {
		log.Printf("echo error from %s: %v", conn.RemoteAddr(), err)
		return
	}
	log.Printf("[echo_server_net] echoed %d bytes to %s", n, conn.RemoteAddr())
}
