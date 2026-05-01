// HTTP server using vclnet as the transport layer.
//
// Demonstrates that Go's standard net/http package works over VPP's VCL
// when vclnet.Listen is used as the listener.
//
// Run:
//
//	VCL_CONFIG=/tmp/vclnet-share/vcl.conf go run ./examples/http_server \
//	  -port 8080
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"vclnet"
)

func main() {
	port := flag.Int("port", 8080, "HTTP port to serve on via VPP")
	flag.Parse()

	if os.Getenv("VCL_CONFIG") == "" {
		log.Fatal("VCL_CONFIG env var must be set")
	}

	if err := vclnet.Init("http-server-vclnet"); err != nil {
		log.Fatalf("vclnet.Init: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello from vclnet HTTP server!\nMethod: %s\nPath: %s\nHost: %s\n",
			r.Method, r.URL.Path, r.Host)
	})
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.Copy(w, r.Body)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok","transport":"vclnet"}`)
	})

	addr := fmt.Sprintf(":%d", *port)
	ln, err := vclnet.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("vclnet.Listen: %v", err)
	}
	log.Printf("[http_server] serving HTTP on %s via VPP", ln.Addr())

	server := &http.Server{Handler: mux}
	if err := server.Serve(ln); err != nil {
		log.Fatalf("server.Serve: %v", err)
	}
}
