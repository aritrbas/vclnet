// HTTP client using vclnet as the transport layer.
//
// Demonstrates that Go's standard net/http client works over VPP's VCL
// when vclnet.Dial is used as the transport dialer.
//
// Run:
//
//	VCL_CONFIG=/tmp/vclnet-share/vcl.conf go run ./examples/http_client \
//	  -url http://127.0.0.1:8080/health
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/aritrbas/vclnet"
)

func main() {
	url := flag.String("url", "http://127.0.0.1:8080/", "URL to GET")
	flag.Parse()

	if os.Getenv("VCL_CONFIG") == "" {
		log.Fatal("VCL_CONFIG env var must be set")
	}

	if err := vclnet.Init("http-client-vclnet"); err != nil {
		log.Fatalf("vclnet.Init: %v", err)
	}

	// Use the package transport so request contexts reach DialContext.
	client := vclnet.NewHTTPClient()
	client.Timeout = 30 * time.Second

	start := time.Now()
	resp, err := client.Get(*url)
	if err != nil {
		log.Fatalf("GET %s: %v", *url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("reading body: %v", err)
	}

	elapsed := time.Since(start)
	fmt.Printf("[http_client] %s %s (%s)\n", resp.Status, *url, elapsed)
	fmt.Printf("[http_client] body:\n%s\n", body)
}
