// production_client demonstrates a multi-goroutine HTTP client using vclnet.
//
// Run (with production_server already running):
//
//	VCL_CONFIG=/tmp/vclnet-share/vcl.conf go run ./examples/production_client
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"vclnet"
)

func main() {
	if err := vclnet.Init("production-client"); err != nil {
		log.Fatalf("vclnet.Init: %v", err)
	}

	client := vclnet.NewHTTPClient()

	const numWorkers = 10
	const requestsPerWorker = 5

	var (
		wg        sync.WaitGroup
		successes atomic.Int64
		failures  atomic.Int64
	)

	start := time.Now()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < requestsPerWorker; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				req, _ := http.NewRequestWithContext(ctx, "GET", "http://127.0.0.1:8080/health", nil)
				resp, err := client.Do(req)
				cancel()
				if err != nil {
					failures.Add(1)
					log.Printf("[worker %d] request %d: %v", workerID, i, err)
					continue
				}
				io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode == 200 {
					successes.Add(1)
				} else {
					failures.Add(1)
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("\nResults: %d ok, %d failed, %.1f req/s (%v)\n",
		successes.Load(), failures.Load(),
		float64(successes.Load()+failures.Load())/elapsed.Seconds(),
		elapsed.Round(time.Millisecond))
}
