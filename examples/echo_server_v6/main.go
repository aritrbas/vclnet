// IPv6 echo server using the vclnet public API.
//
// Demonstrates IPv6 support through VPP's VCL layer.
//
// Run:
//
//	VCL_CONFIG=/tmp/vclnet-share/vcl.conf go run ./examples/echo_server_v6 \
//	  -port 9877
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
	port := flag.Int("port", 9877, "TCP port to listen on (IPv6) via VPP")
	flag.Parse()

	if os.Getenv("VCL_CONFIG") == "" {
		log.Fatal("VCL_CONFIG env var must be set")
	}

	if err := vclnet.Init("echo-server-v6"); err != nil {
		log.Fatalf("vclnet.Init: %v", err)
	}

	addr := fmt.Sprintf("[::1]:%d", *port)
	ln, err := vclnet.Listen("tcp6", addr)
	if err != nil {
		log.Fatalf("vclnet.Listen tcp6: %v", err)
	}
	defer ln.Close()
	log.Printf("[echo_server_v6] listening on %s via VPP (tcp6)", ln.Addr())

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			return
		}
		log.Printf("[echo_server_v6] accepted from %s", conn.RemoteAddr())
		go func(c net.Conn) {
			defer c.Close()
			n, err := io.Copy(c, c)
			if err != nil {
				log.Printf("echo error: %v", err)
				return
			}
			log.Printf("[echo_server_v6] echoed %d bytes", n)
		}(conn)
	}
}
