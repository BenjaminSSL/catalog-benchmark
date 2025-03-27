package execution

import (
	"benchmark/internal/common"
	"context"
	"github.com/google/uuid"
	"net/http"
	"sync"
	"time"
)

type BenchmarkEngine struct {
	ExperimentID string
	threads      int
	duration     time.Duration
	wg           sync.WaitGroup
	catalog      string
}

func NewBenchmarkEngine(experimentID string, catalog string, threads int, duration time.Duration) *BenchmarkEngine {
	return &BenchmarkEngine{
		ExperimentID: experimentID,
		catalog:      catalog,
		threads:      threads,
		duration:     duration,
	}
}

func (e *BenchmarkEngine) RunWorkers(ctx context.Context, configs []WorkerConfig) error {
	ctx, cancel := context.WithTimeout(ctx, e.duration)
	defer cancel()

	client := getHttpClient()

	for _, config := range configs {
		for thread := 0; thread < config.Threads; thread++ {
			e.wg.Add(1)
			go func(thread int, workerFunc func(ctx context.Context, client *http.Client, logger *common.RoutineBatchLogger)) {
				logger, _ := common.NewRoutineBatchLogger("./output/logs/tmp", e.ExperimentID, thread, 20)
				defer logger.Close()
				defer e.wg.Done()
				workerFunc(ctx, client, logger)
			}(thread, config.Func)
		}
	}

	e.wg.Wait()

	return nil
}
func (e *BenchmarkEngine) CreateCatalogWorker(ctx context.Context, client *http.Client, logger *common.RoutineBatchLogger) {
	for step := 1; ; step++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		name := uuid.New().String()
		resp, err := createCatalogRequest(e.catalog, client, name)
		if err != nil {
			handleRequestError(err, logger, step)
			continue
		}

		handleResponse(resp, logger, step)
	}
}

func (e *BenchmarkEngine) CreateDeleteCatalogWorker(ctx context.Context, client *http.Client, logger *common.RoutineBatchLogger) {
	for step := 1; ; step++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		name := uuid.New().String()

		resp, err := createCatalogRequest(e.catalog, client, name)
		if err != nil {
			handleRequestError(err, logger, step)
		} else {
			handleResponse(resp, logger, step)
		}

		resp, err = deleteCatalogRequest(e.catalog, client, name)
		if err != nil {
			handleRequestError(err, logger, step)
		} else {
			handleResponse(resp, logger, step)

		}

	}
}
