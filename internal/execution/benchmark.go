package execution

import (
	"benchmark/internal/common"
	"context"
	"github.com/google/uuid"
	"log"
	"net/http"
	"sync"
	"time"
)

type WorkerFunc func(ctx context.Context, client *http.Client, logger *common.RoutineBatchLogger, params ...string)
type WorkerConfig struct {
	workerFunc  WorkerFunc
	threadRatio float64
	params      []string
}

type BenchmarkEngine struct {
	ExperimentID string
	threads      int
	duration     time.Duration
	catalog      string
	client       *http.Client
}

func NewBenchmarkEngine(experimentID string, catalog string, threads int, duration time.Duration) *BenchmarkEngine {
	return &BenchmarkEngine{
		ExperimentID: experimentID,
		catalog:      catalog,
		threads:      threads,
		duration:     duration,
		client:       getHttpClient(),
	}
}
func (e *BenchmarkEngine) RunCreateCatalog(ctx context.Context) error {
	return e.runSingleBenchmark(ctx, e.createCatalogWorker)
}

// RunCreatePrincipal runs the create principal benchmark
func (e *BenchmarkEngine) RunCreatePrincipal(ctx context.Context) error {
	return e.runSingleBenchmark(ctx, e.createPrincipalWorker)
}

// RunCreateSchema runs the create schema benchmark
func (e *BenchmarkEngine) RunCreateSchema(ctx context.Context) error {
	catalogName := uuid.NewString()
	log.Println("Creating catalog", catalogName)

	_, err := createCatalogRequest(e.catalog, e.client, catalogName)
	if err != nil {
		return err
	}

	return e.runSingleBenchmark(ctx, e.createSchemaOnCatalogWorker, catalogName)
}

// RunCreateUpdateCatalog runs the create and update catalog benchmark
func (e *BenchmarkEngine) RunCreateUpdateCatalog(ctx context.Context) error {
	catalogName := uuid.NewString()
	_, err := createCatalogRequest(e.catalog, e.client, catalogName)
	if err != nil {
		return err
	}

	return e.runSingleBenchmark(ctx, e.updateCatalogWorker, catalogName)
}

// RunCreateUpdatePrincipal runs the create and update principal benchmark
func (e *BenchmarkEngine) RunCreateUpdatePrincipal(ctx context.Context) error {
	catalogName := uuid.NewString()
	_, err := createCatalogRequest(e.catalog, e.client, catalogName)
	if err != nil {
		return err
	}

	return e.runSingleBenchmark(ctx, e.updatePrincipalWorker, catalogName)
}

// RunCreateDeleteCatalog runs the create and delete catalog benchmark
func (e *BenchmarkEngine) RunCreateDeleteCatalog(ctx context.Context) error {
	return e.runSingleBenchmark(ctx, e.createDeleteCatalogWorker)
}

// RunCreateDeletePrincipal runs the create and delete principal benchmark
func (e *BenchmarkEngine) RunCreateDeletePrincipal(ctx context.Context) error {
	return e.runSingleBenchmark(ctx, e.createDeletePrincipalWorker)
}

// RunCreateDeleteSchema runs the create and delete schema benchmark
func (e *BenchmarkEngine) RunCreateDeleteSchema(ctx context.Context) error {
	catalogName := uuid.NewString()
	log.Println("Creating catalog", catalogName)
	_, err := createCatalogRequest(e.catalog, e.client, catalogName)
	if err != nil {
		return err
	}

	return e.runSingleBenchmark(ctx, e.createDeleteSchemaWorker, catalogName)
}

// RunCreateDeleteListCatalog runs the create, delete, and list catalog benchmark
func (e *BenchmarkEngine) RunCreateDeleteListCatalog(ctx context.Context) error {
	return e.runMixedBenchmark(ctx, []WorkerConfig{
		{e.listCatalogsWorker, 0.1, make([]string, 0)},        //  // 10% of threads list catalogs
		{e.createDeleteCatalogWorker, 0.9, make([]string, 0)}, // 90% create and delete
	})
}

// RunCreateDeleteListPrincipal runs the create, delete, and list principal benchmark
func (e *BenchmarkEngine) RunCreateDeleteListPrincipal(ctx context.Context) error {
	return e.runMixedBenchmark(ctx, []WorkerConfig{
		{e.listPrincipalsWorker, 0.1, make([]string, 0)},        // 10% of threads list principals
		{e.createDeletePrincipalWorker, 0.9, make([]string, 0)}, // 90% create and delete
	})
}

// RunCreateDeleteListSchema runs the create, delete, and list schema benchmark
func (e *BenchmarkEngine) RunCreateDeleteListSchema(ctx context.Context) error {
	catalogName := uuid.NewString()
	log.Println("Creating catalog", catalogName)
	_, err := createCatalogRequest(e.catalog, e.client, catalogName)
	if err != nil {
		return err
	}

	return e.runMixedBenchmark(ctx, []WorkerConfig{
		{e.listSchemasWorker, 0.1, []string{catalogName}},        // 10% of threads list schemas
		{e.createDeleteSchemaWorker, 0.9, []string{catalogName}}, // 90% create and delete
	})
}

func (e *BenchmarkEngine) runSingleBenchmark(ctx context.Context, workerFunc WorkerFunc, params ...string) error {
	ctx, cancel := context.WithTimeout(ctx, e.duration)
	defer cancel()

	var wg sync.WaitGroup
	for thread := 0; thread < e.threads; thread++ {
		wg.Add(1)
		go func(threadID int) {
			logger, _ := common.NewRoutineBatchLogger("./output/logs/tmp", e.ExperimentID, threadID, 20)
			defer logger.Close()
			defer wg.Done()
			workerFunc(ctx, e.client, logger, params...)
		}(thread)
	}

	wg.Wait()
	return nil
}

func (e *BenchmarkEngine) runMixedBenchmark(ctx context.Context, workers []WorkerConfig) error {
	ctx, cancel := context.WithTimeout(ctx, e.duration)
	defer cancel()

	var wg sync.WaitGroup
	threadAssigned := 0

	for i, worker := range workers {
		numThreads := 0
		if i == len(workers)-1 {
			numThreads = e.threads - threadAssigned
		} else {
			numThreads = int(float64(e.threads) * worker.threadRatio)
			if worker.threadRatio > 0 && numThreads == 0 {
				numThreads = 1
			}
		}

		for t := 0; t < numThreads; t++ {
			threadID := threadAssigned // Capture the correct value for the goroutine
			wg.Add(1)
			go func(threadID int, wFunc WorkerFunc, params []string) {
				defer wg.Done()
				logger, _ := common.NewRoutineBatchLogger("./output/logs/tmp", e.ExperimentID, threadID, 20)
				defer logger.Close()
				wFunc(ctx, e.client, logger, params...)
			}(threadID, worker.workerFunc, worker.params)
			threadAssigned++
		}
	}

	wg.Wait()
	return nil
}

func (e *BenchmarkEngine) createCatalogWorker(ctx context.Context, client *http.Client, logger *common.RoutineBatchLogger, _ ...string) {
	for step := 1; ; step++ {
		if ctx.Err() != nil {
			return
		}

		name := uuid.NewString()
		resp, err := createCatalogRequest(e.catalog, client, name)

		if err != nil {
			handleRequestError(err, logger, step)
			continue
		}
		handleResponse(resp, logger, step)
	}
}

func (e *BenchmarkEngine) createPrincipalWorker(ctx context.Context, client *http.Client, logger *common.RoutineBatchLogger, _ ...string) {
	for step := 1; ; step++ {
		if ctx.Err() != nil {
			return
		}

		name := uuid.NewString()
		resp, err := createPrincipalRequest(e.catalog, client, name)
		if err != nil {
			handleRequestError(err, logger, step)
			continue
		}
		handleResponse(resp, logger, step)
	}
}

func (e *BenchmarkEngine) createSchemaOnCatalogWorker(ctx context.Context, client *http.Client, logger *common.RoutineBatchLogger, params ...string) {
	catalogName := params[0]
	for step := 1; ; step++ {
		if ctx.Err() != nil {
			return
		}

		name := uuid.NewString()
		resp, err := createSchemaRequest(e.catalog, client, name, catalogName)
		if err != nil {
			handleRequestError(err, logger, step)
			continue
		}
		handleResponse(resp, logger, step)
	}
}

func (e *BenchmarkEngine) updateCatalogWorker(ctx context.Context, client *http.Client, logger *common.RoutineBatchLogger, params ...string) {
	name := params[0]
	entityVersion := 1
	for step := 1; ; step++ {
		if ctx.Err() != nil {
			return
		}

		resp, err := updateCatalogRequest(e.catalog, client, name, entityVersion)
		if err != nil {
			handleRequestError(err, logger, step)
		} else {
			handleResponse(resp, logger, step)
			entityVersion++
		}
	}
}

func (e *BenchmarkEngine) updatePrincipalWorker(ctx context.Context, client *http.Client, logger *common.RoutineBatchLogger, params ...string) {
	name := params[0]
	entityVersion := 1
	for step := 1; ; step++ {
		if ctx.Err() != nil {
			return
		}

		resp, err := updatePrincipalRequest(e.catalog, client, name, entityVersion)
		if err != nil {
			handleRequestError(err, logger, step)
		} else {
			handleResponse(resp, logger, step)
			entityVersion++
		}
	}
}

func (e *BenchmarkEngine) createDeleteCatalogWorker(ctx context.Context, client *http.Client, logger *common.RoutineBatchLogger, _ ...string) {
	for step := 1; ; step += 2 {
		if ctx.Err() != nil {
			return
		}
		name := uuid.NewString()

		resp, err := createCatalogRequest(e.catalog, client, name)
		if err != nil {
			handleRequestError(err, logger, step)
			continue
		}
		handleResponse(resp, logger, step)

		resp, err = deleteCatalogRequest(e.catalog, client, name)
		if err != nil {
			handleRequestError(err, logger, step+1)
			continue
		}
		handleResponse(resp, logger, step+1)
	}
}

func (e *BenchmarkEngine) createDeletePrincipalWorker(ctx context.Context, client *http.Client, logger *common.RoutineBatchLogger, _ ...string) {
	for step := 1; ; step += 2 {
		if ctx.Err() != nil {
			return
		}
		name := uuid.NewString()

		resp, err := createPrincipalRequest(e.catalog, client, name)
		if err != nil {
			handleRequestError(err, logger, step)
			continue
		}
		handleResponse(resp, logger, step)

		resp, err = deletePrincipalRequest(e.catalog, client, name)
		if err != nil {
			handleRequestError(err, logger, step+1)
			continue
		}
		handleResponse(resp, logger, step+1)
	}
}

func (e *BenchmarkEngine) createDeleteSchemaWorker(ctx context.Context, client *http.Client, logger *common.RoutineBatchLogger, params ...string) {
	catalogName := params[0]
	for step := 1; ; step += 2 {
		if ctx.Err() != nil {
			return
		}
		name := uuid.NewString()

		resp, err := createSchemaRequest(e.catalog, client, name, catalogName)
		if err != nil {
			handleRequestError(err, logger, step)
			continue
		}
		handleResponse(resp, logger, step)

		resp, err = deleteSchemaRequest(e.catalog, client, name, catalogName)
		if err != nil {
			handleRequestError(err, logger, step+1)
			continue
		}
		handleResponse(resp, logger, step+1)
	}
}

func (e *BenchmarkEngine) listCatalogsWorker(ctx context.Context, client *http.Client, logger *common.RoutineBatchLogger, _ ...string) {
	for step := 1; ; step++ {
		if ctx.Err() != nil {
			return
		}
		resp, err := listCatalogsRequest(e.catalog, client)
		if err != nil {
			handleRequestError(err, logger, step)
			continue
		}
		handleResponse(resp, logger, step)
	}
}

func (e *BenchmarkEngine) listPrincipalsWorker(ctx context.Context, client *http.Client, logger *common.RoutineBatchLogger, _ ...string) {
	for step := 1; ; step++ {
		if ctx.Err() != nil {
			return
		}
		resp, err := listPrincipalsRequest(e.catalog, client)
		if err != nil {
			handleRequestError(err, logger, step)
			continue
		}
		handleResponse(resp, logger, step)
	}
}

func (e *BenchmarkEngine) listSchemasWorker(ctx context.Context, client *http.Client, logger *common.RoutineBatchLogger, params ...string) {
	catalogName := params[0]
	for step := 1; ; step++ {
		if ctx.Err() != nil {
			return
		}
		resp, err := listSchemasRequest(e.catalog, client, catalogName)
		if err != nil {
			handleRequestError(err, logger, step)
			continue
		}
		handleResponse(resp, logger, step)
	}
}
