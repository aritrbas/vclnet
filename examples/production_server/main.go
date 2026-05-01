// production_server demonstrates a multi-goroutine HTTP server using vclnet.
//
// Run:
//
//	VCL_CONFIG=/tmp/vclnet-share/vcl.conf go run ./examples/production_server
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sync/atomic"

	"vclnet"
)

var requestCount atomic.Int64

func main() {
	if err := vclnet.Init("production-server"); err != nil {
		log.Fatalf("vclnet.Init: %v", err)
	}
	vclnet.InstallSignalHandler()

	ln, err := vclnet.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("vclnet.Listen: %v", err)
	}
	defer ln.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		count := requestCount.Add(1)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status":   "ok",
			"requests": count,
		})
	})
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		io.Copy(w, r.Body)
	})

	log.Printf("Listening on :8080 via VPP (PID %d)", os.Getpid())
	fmt.Println("READY")

	if err := (&http.Server{Handler: mux}).Serve(ln); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Serve: %v", err)
	}
}
