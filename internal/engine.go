package internal

import (
	"benchmark/internal/common"
	"context"
	"net/http"
	"sync"
	"time"
)

type WorkerConfig struct {
	WorkerFunc WorkerFunc
	Threads    int
	Params     map[string]interface{}
}

type BenchmarkEngine struct {
	ExperimentID string
	threads      int
	duration     time.Duration
	Catalog      Catalog
	client       *http.Client
}

func NewBenchmarkEngine(experimentID string, catalog Catalog, threads int, duration time.Duration) *BenchmarkEngine {
	return &BenchmarkEngine{
		ExperimentID: experimentID,
		Catalog:      catalog,
		threads:      threads,
		duration:     duration,
		client: &http.Client{
			Timeout: time.Second * 30,
			Transport: &http.Transport{
				MaxIdleConns:        10000,
				MaxIdleConnsPerHost: 1000,
				MaxConnsPerHost:     1000,
				DisableKeepAlives:   false,
				IdleConnTimeout:     90 * time.Second,
				TLSHandshakeTimeout: 10 * time.Second,
			},
		},
	}
}

func (e *BenchmarkEngine) RunBenchmark(ctx context.Context, workers []WorkerConfig) error {
	ctx, cancel := context.WithTimeout(ctx, e.duration)
	defer cancel()
	var wg sync.WaitGroup
	threadAllocated := 0

	for _, worker := range workers {
		for t := 0; t < worker.Threads; t++ {
			threadID := threadAllocated
			wg.Add(1)

			go func(threadID int, config WorkerConfig) {
				defer wg.Done()
				logger, _ := common.NewRoutineBatchLogger("./output/logs/tmp", e.ExperimentID, threadID, 20)
				defer logger.Close()

				w := NewWorker(
					e.client, e.Catalog, logger, config.Params, config.WorkerFunc)

				w.Run(ctx)

			}(threadID, worker)
			threadAllocated++
		}
	}

	wg.Wait()
	return nil
}
