package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type payload struct {
	Source     string            `json:"source"`
	ExternalID string            `json:"external_id"`
	Service    string            `json:"service"`
	Region     string            `json:"region"`
	Signal     string            `json:"signal"`
	Message    string            `json:"message"`
	StatusCode int               `json:"status_code"`
	LatencyMS  float64           `json:"latency_ms"`
	ErrorRate  float64           `json:"error_rate"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

func main() {
	url := flag.String("url", "http://localhost:8080/v1/incidents", "gateway endpoint")
	rps := flag.Int("rps", 10000, "target requests per second across all workers; 0 means unlimited")
	workers := flag.Int("workers", 256, "number of concurrent workers")
	duration := flag.Duration("duration", 30*time.Second, "test duration")
	flag.Parse()

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          *workers * 4,
		MaxIdleConnsPerHost:   *workers * 4,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   3 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var sent, ok, fail atomic.Int64
	var wg sync.WaitGroup
	perWorkerRPS := 0
	if *rps > 0 && *workers > 0 {
		perWorkerRPS = max(1, *rps / *workers)
	}
	started := time.Now()
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			var ticker *time.Ticker
			if perWorkerRPS > 0 {
				interval := time.Second / time.Duration(perWorkerRPS)
				if interval <= 0 {
					interval = time.Nanosecond
				}
				ticker = time.NewTicker(interval)
				defer ticker.Stop()
			}
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if ticker != nil {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
					}
				}
				body, _ := json.Marshal(samplePayload(worker, sent.Add(1)))
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, *url, bytes.NewReader(body))
				req.Header.Set("content-type", "application/json")
				resp, err := client.Do(req)
				if err != nil {
					fail.Add(1)
					continue
				}
				_ = resp.Body.Close()
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					ok.Add(1)
				} else {
					fail.Add(1)
				}
			}
		}(i)
	}

	reporterDone := make(chan struct{})
	go func() {
		defer close(reporterDone)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var lastOK int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				currentOK := ok.Load()
				fmt.Printf("ok/s=%d total_ok=%d fail=%d sent=%d\n", currentOK-lastOK, currentOK, fail.Load(), sent.Load())
				lastOK = currentOK
			}
		}
	}()
	wg.Wait()
	<-reporterDone
	elapsed := time.Since(started).Seconds()
	log.Printf("finished: sent=%d ok=%d fail=%d avg_ok_per_sec=%.0f elapsed=%.1fs", sent.Load(), ok.Load(), fail.Load(), float64(ok.Load())/elapsed, elapsed)
}

func samplePayload(worker int, seq int64) payload {
	services := []string{"checkout-api", "payments", "catalog", "orders", "identity"}
	regions := []string{"ap-south-1", "us-east-1", "eu-west-1"}
	service := services[rand.Intn(len(services))]
	status := 200
	errorRate := rand.Float64() * 0.2
	if errorRate > 0.08 {
		status = 503
	}
	return payload{
		Source:     "loadgen",
		ExternalID: fmt.Sprintf("%s-%d-%d", service, worker, seq%100000),
		Service:    service,
		Region:     regions[rand.Intn(len(regions))],
		Signal:     "synthetic_health_check",
		Message:    "synthetic incident signal",
		StatusCode: status,
		LatencyMS:  rand.Float64() * 6000,
		ErrorRate:  errorRate,
		Attributes: map[string]string{"worker": fmt.Sprint(worker)},
	}
}
