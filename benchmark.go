package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func getLeaderURL() string {
	resp, err := http.Get("http://localhost:8001/health")
	if err != nil {
		return "http://localhost:8001"
	}
	defer resp.Body.Close()
	
	var result struct {
		Raft struct {
			LeaderID string `json:"leader_id"`
		} `json:"raft"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	
	switch result.Raft.LeaderID {
	case "node2":
		return "http://localhost:8002"
	case "node3":
		return "http://localhost:8003"
	default:
		return "http://localhost:8001"
	}
}

func main() {
	// Configuration
	totalRequests := 10000
	concurrency := 100
	
	leaderBaseURL := getLeaderURL()
	targetURL := leaderBaseURL + "/kv/benchkey%d?val=benchval"

	// To follow redirects correctly across threads
	client := &http.Client{
		Timeout: 2 * time.Second,
	}

	fmt.Printf("Starting benchmark: %d total requests, %d concurrent workers...\n", totalRequests, concurrency)

	var successCount atomic.Uint64
	var errorCount atomic.Uint64
	var totalLatency time.Duration
	var latencyMu sync.Mutex

	// Create a buffered channel to distribute work
	workCh := make(chan int, totalRequests)
	for i := 0; i < totalRequests; i++ {
		workCh <- i
	}
	close(workCh)

	var wg sync.WaitGroup
	startTime := time.Now()

	// Spawn workers
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for reqID := range workCh {
				url := fmt.Sprintf(targetURL, reqID)
				
				reqStart := time.Now()
				
				req, err := http.NewRequest(http.MethodPut, url, nil)
				if err != nil {
					errorCount.Add(1)
					continue
				}

				resp, err := client.Do(req)
				if err != nil {
					errorCount.Add(1)
					if errorCount.Load() <= 5 {
						fmt.Printf("Request error: %v\n", err)
					}
					continue
				}
				
				// Read and close body to reuse connection
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				latency := time.Since(reqStart)
				
				if resp.StatusCode == http.StatusOK {
					successCount.Add(1)
					latencyMu.Lock()
					totalLatency += latency
					latencyMu.Unlock()
				} else {
					errorCount.Add(1)
					if errorCount.Load() <= 5 {
						fmt.Printf("HTTP status error: %d\n", resp.StatusCode)
					}
				}
			}
		}()
	}

	// Wait for all workers to finish
	wg.Wait()
	duration := time.Since(startTime)

	// Calculate metrics
	successful := successCount.Load()
	rps := float64(successful) / duration.Seconds()
	var avgLatency time.Duration
	if successful > 0 {
		avgLatency = time.Duration(int64(totalLatency) / int64(successful))
	}

	fmt.Println("========================================")
	fmt.Println("BENCHMARK RESULTS (Raft 2-Phase Commits)")
	fmt.Println("========================================")
	fmt.Printf("Total Time:      %v\n", duration)
	fmt.Printf("Successful:      %d\n", successful)
	fmt.Printf("Errors:          %d\n", errorCount.Load())
	fmt.Printf("Throughput:      %.2f Requests / Second\n", rps)
	fmt.Printf("Average Latency: %v\n", avgLatency)
	fmt.Println("========================================")
}
