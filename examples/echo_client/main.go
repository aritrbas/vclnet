// Low-level echo client: connects to a TCP server through VPP via the VLS
// (vls_*) API in the vclpoll cgo bridge, sends a message, reads the echo,
// prints it, and exits.
//
// Run:
//
//	VCL_CONFIG=/tmp/client-share/vcl.conf go run ./examples/echo_client \
//	  -addr 127.0.0.1:9876 -msg "hello vcl"
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"vclnet/internal/vclpoll"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9876", "server address (host:port, IPv4 only)")
	msg := flag.String("msg", "hello vcl", "message to send")
	flag.Parse()

	if os.Getenv("VCL_CONFIG") == "" {
		log.Fatal("VCL_CONFIG env var must be set (e.g. /tmp/client-share/vcl.conf)")
	}

	ip4, port, err := parseIPv4HostPort(*addr)
	if err != nil {
		log.Fatalf("invalid -addr %q: %v", *addr, err)
	}

	if err := vclpoll.AppInit("vclnet-echo-client"); err != nil {
		log.Fatalf("AppInit: %v", err)
	}

	start := time.Now()
	conn, err := vclpoll.DialTCP4(ip4, port)
	if err != nil {
		log.Fatalf("DialTCP4: %v", err)
	}
	defer vclpoll.Close(conn)
	log.Printf("[vclnet/echo_client] connected vlsh=%d in %s", conn, time.Since(start))

	// Send.
	out := []byte(*msg)
	written := 0
	for written < len(out) {
		n, err := vclpoll.Write(conn, out[written:])
		if err != nil {
			log.Fatalf("Write: %v", err)
		}
		written += n
	}
	log.Printf("[vclnet/echo_client] sent %d bytes", written)

	// Receive until we have len(out) bytes back, or timeout.
	in := make([]byte, len(out))
	read := 0
	for read < len(in) {
		n, err := vclpoll.Read(conn, in[read:])
		if err != nil {
			log.Fatalf("Read: %v", err)
		}
		if n == 0 {
			log.Fatalf("peer closed before full echo (got %d/%d)", read, len(in))
		}
		read += n
	}
	fmt.Printf("[vclnet/echo_client] echoed: %q\n", string(in))
}

// parseIPv4HostPort parses "1.2.3.4:80" into (4-byte IP, host-order port).
func parseIPv4HostPort(s string) ([4]byte, uint16, error) {
	host, portStr, err := net.SplitHostPort(s)
	if err != nil {
		return [4]byte{}, 0, err
	}
	ip := net.ParseIP(host).To4()
	if ip == nil {
		return [4]byte{}, 0, errors.New("not an IPv4 address")
	}
	p, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return [4]byte{}, 0, err
	}
	var out [4]byte
	copy(out[:], ip)
	return out, uint16(p), nil
}
