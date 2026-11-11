// Command bench measures dump latency, throughput and server RSS under load, against either
// implementation.
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var (
		url         = flag.String("url", "http://localhost:3001/v1/keys", "endpoint to hit")
		concurrency = flag.Int("concurrency", 1, "parallel requests")
		requests    = flag.Int("requests", 10, "total requests")
		warmup      = flag.Int("warmup", 1, "warmup requests excluded from statistics")
		rssPID      = flag.String("rss-pid", "", "sample the resident set of this pid while running")
		label       = flag.String("label", "", "label printed with the results")
	)
	flag.Parse()

	client := &http.Client{Timeout: 30 * time.Minute}

	for i := 0; i < *warmup; i++ {
		if _, _, err := do(client, *url); err != nil {
			fmt.Printf("warmup failed: %v\n", err)
			return
		}
	}

	var (
		stopRSS = make(chan struct{})
		rssDone = make(chan int64)
		peakRSS int64
		baseRSS int64
	)
	if *rssPID != "" {
		baseRSS = readRSS(*rssPID)
		go func() {
			var peak int64
			t := time.NewTicker(50 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-stopRSS:
					rssDone <- peak
					return
				case <-t.C:
					if v := readRSS(*rssPID); v > peak {
						peak = v
					}
				}
			}
		}()
	}

	var (
		mu        sync.Mutex
		latencies []time.Duration
		bytesRead atomic.Int64
		failures  atomic.Int64
		wg        sync.WaitGroup
	)

	jobs := make(chan struct{}, *requests)
	for i := 0; i < *requests; i++ {
		jobs <- struct{}{}
	}
	close(jobs)

	started := time.Now()
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				n, d, err := do(client, *url)
				if err != nil {
					failures.Add(1)
					continue
				}
				bytesRead.Add(n)
				mu.Lock()
				latencies = append(latencies, d)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(started)

	if *rssPID != "" {
		close(stopRSS)
		peakRSS = <-rssDone
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	if *label != "" {
		fmt.Printf("== %s\n", *label)
	}
	fmt.Printf("url            %s\n", *url)
	fmt.Printf("requests       %d ok, %d failed, concurrency %d\n", len(latencies), failures.Load(), *concurrency)
	fmt.Printf("wall clock     %s\n", elapsed.Round(time.Millisecond))
	if len(latencies) > 0 {
		fmt.Printf("latency        p50 %s  p90 %s  p95 %s  max %s\n",
			pct(latencies, 0.50).Round(time.Millisecond),
			pct(latencies, 0.90).Round(time.Millisecond),
			pct(latencies, 0.95).Round(time.Millisecond),
			latencies[len(latencies)-1].Round(time.Millisecond))
		fmt.Printf("response size  %.1f MiB\n", float64(bytesRead.Load())/float64(len(latencies))/(1<<20))
		fmt.Printf("throughput     %.1f MiB/s  %.2f req/s\n",
			float64(bytesRead.Load())/(1<<20)/elapsed.Seconds(),
			float64(len(latencies))/elapsed.Seconds())
	}
	if *rssPID != "" {
		fmt.Printf("server rss     %.0f MiB before, %.0f MiB peak\n",
			float64(baseRSS)/1024, float64(peakRSS)/1024)
	}
}

func do(client *http.Client, url string) (int64, time.Duration, error) {
	started := time.Now()
	resp, err := client.Get(url)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()

	n, err := io.Copy(io.Discard, resp.Body)
	if err != nil {
		return 0, 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return n, time.Since(started), fmt.Errorf("status %d", resp.StatusCode)
	}
	return n, time.Since(started), nil
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(float64(len(sorted)-1) * p)
	return sorted[i]
}

// readRSS returns the resident set of a pid in kilobytes.
func readRSS(pid string) int64 {
	out, err := exec.Command("ps", "-o", "rss=", "-p", pid).Output()
	if err != nil {
		return 0
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return 0
	}
	return v
}
