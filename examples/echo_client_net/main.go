// Echo client using the vclnet public API (net.Conn interface).
//
// Run:
//
//	VCL_CONFIG=/tmp/vclnet-share/vcl.conf go run ./examples/echo_client_net \
//	  -addr 127.0.0.1:9876 -msg "hello vclnet"
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"vclnet"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9876", "server address (host:port)")
	msg := flag.String("msg", "hello vclnet", "message to send")
	flag.Parse()

	if os.Getenv("VCL_CONFIG") == "" {
		log.Fatal("VCL_CONFIG env var must be set")
	}

	if err := vclnet.Init("echo-client-net"); err != nil {
		log.Fatalf("vclnet.Init: %v", err)
	}

	conn, err := vclnet.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("vclnet.Dial: %v", err)
	}
	defer conn.Close()
	log.Printf("[echo_client_net] connected to %s", conn.RemoteAddr())

	// Send.
	out := []byte(*msg)
	if _, err := conn.Write(out); err != nil {
		log.Fatalf("Write: %v", err)
	}
	log.Printf("[echo_client_net] sent %d bytes", len(out))

	// Receive echo.
	in := make([]byte, len(out))
	read := 0
	for read < len(in) {
		n, err := conn.Read(in[read:])
		if err != nil {
			log.Fatalf("Read: %v", err)
		}
		read += n
	}
	fmt.Printf("[echo_client_net] echoed: %q\n", string(in))
}
