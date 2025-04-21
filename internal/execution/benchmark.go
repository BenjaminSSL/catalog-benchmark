package execution

import (
	"benchmark/internal/catalog/polaris"
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

func (e *BenchmarkEngine) RunCreatePrincipal(ctx context.Context) error {
	return e.runSingleBenchmark(ctx, e.createPrincipalWorker)
}

func (e *BenchmarkEngine) RunCreateSchema(ctx context.Context) error {
	catalogName, _ := createCatalog(e.catalog, e.client)

	return e.runSingleBenchmark(ctx, e.createSchemaWorker, catalogName)
}

func (e *BenchmarkEngine) RunCreateTable(ctx context.Context) error {
	catalogName, namespaceName, _ := createCatalogAndSchema(e.catalog, e.client)
	if e.catalog == "polaris" {
		err := polaris.GrantCatalogPermissions(ctx, catalogName)
		if err != nil {
			return err
		}
	}

	return e.runSingleBenchmark(ctx, e.createTableWorker, catalogName, namespaceName)
}
func (e *BenchmarkEngine) RunCreateView(ctx context.Context) error {
	catalogName, namespaceName, err := createCatalogAndSchema(e.catalog, e.client)
	if err != nil {
		panic(err)
	}
	return e.runSingleBenchmark(ctx, e.createViewWorker, catalogName, namespaceName)
}

func (e *BenchmarkEngine) RunCreateFunction(ctx context.Context) error {
	catalogName, schemaName, err := createCatalogAndSchema(e.catalog, e.client)
	if err != nil {
		panic(err)
	}

	return e.runSingleBenchmark(ctx, e.createFunctionWorker, catalogName, schemaName)
}
func (e *BenchmarkEngine) RunCreateModel(ctx context.Context) error {
	catalogName, schemaName, err := createCatalogAndSchema(e.catalog, e.client)
	if err != nil {
		panic(err)
	}

	return e.runSingleBenchmark(ctx, e.createModelWorker, catalogName, schemaName)
}
func (e *BenchmarkEngine) RunCreateVolume(ctx context.Context) error {
	catalogName, schemaName, err := createCatalogAndSchema(e.catalog, e.client)
	if err != nil {
		panic(err)
	}

	return e.runSingleBenchmark(ctx, e.createVolumeWorker, catalogName, schemaName)
}

func (e *BenchmarkEngine) RunCreateDeleteCatalog(ctx context.Context) error {
	return e.runSingleBenchmark(ctx, e.createDeleteCatalogWorker)
}

func (e *BenchmarkEngine) RunCreateDeleteSchema(ctx context.Context) error {
	catalogName := uuid.NewString()
	log.Println("Creating catalog", catalogName)
	_, err := createCatalogRequest(e.catalog, e.client, catalogName)
	if err != nil {
		return err
	}

	return e.runSingleBenchmark(ctx, e.createDeleteSchemaWorker, catalogName)
}

func (e *BenchmarkEngine) RunCreateDeletePrincipal(ctx context.Context) error {
	return e.runSingleBenchmark(ctx, e.createDeletePrincipalWorker)
}

func (e *BenchmarkEngine) RunCreateDeleteTable(ctx context.Context) error {
	catalogName, schemaName, _ := createCatalogAndSchema(e.catalog, e.client)
	if e.catalog == "polaris" {
		err := polaris.GrantCatalogPermissions(ctx, catalogName)
		if err != nil {
			return err
		}
	}

	return e.runSingleBenchmark(ctx, e.createDeleteTableWorker, catalogName, schemaName)
}

func (e *BenchmarkEngine) RunCreateDeleteView(ctx context.Context) error {
	catalogName, schemaName, _ := createCatalogAndSchema(e.catalog, e.client)

	return e.runSingleBenchmark(ctx, e.createDeleteViewWorker, catalogName, schemaName)
}

func (e *BenchmarkEngine) RunCreateDeleteFunction(ctx context.Context) error {
	catalogName, schemaName, _ := createCatalogAndSchema(e.catalog, e.client)

	return e.runSingleBenchmark(ctx, e.createDeleteFunctionWorker, catalogName, schemaName)
}

func (e *BenchmarkEngine) RunCreateDeleteModel(ctx context.Context) error {
	catalogName, schemaName, _ := createCatalogAndSchema(e.catalog, e.client)

	return e.runSingleBenchmark(ctx, e.createDeleteModelWorker, catalogName, schemaName)
}
func (e *BenchmarkEngine) RunCreateDeleteVolume(ctx context.Context) error {
	catalogName, schemaName, _ := createCatalogAndSchema(e.catalog, e.client)
	return e.runSingleBenchmark(ctx, e.createDeleteVolumeWorker, catalogName, schemaName)
}

func (e *BenchmarkEngine) RunUpdateCatalog(ctx context.Context) error {
	catalogName := uuid.NewString()
	_, err := createCatalogRequest(e.catalog, e.client, catalogName)
	if err != nil {
		return err
	}

	return e.runSingleBenchmark(ctx, e.updateCatalogWorker, catalogName)
}

func (e *BenchmarkEngine) RunUpdatePrincipal(ctx context.Context) error {
	principalName, _ := createPrincipal(e.catalog, e.client)

	return e.runSingleBenchmark(ctx, e.updatePrincipalWorker, principalName)
}

func (e *BenchmarkEngine) RunUpdateSchema(ctx context.Context) error {
	catalogName, schemaName, _ := createCatalogAndSchema(e.catalog, e.client)

	return e.runSingleBenchmark(ctx, e.updateSchemaWorker, catalogName, schemaName)
}

func (e *BenchmarkEngine) RunUpdateTable(ctx context.Context) error {
	catalogName, schemaName, _ := createCatalogAndSchema(e.catalog, e.client)
	if e.catalog == "polaris" {
		err := polaris.GrantCatalogPermissions(ctx, catalogName)
		if err != nil {
			return err
		}
	}

	tableName, _ := createTable(e.catalog, e.client, catalogName, schemaName)

	return e.runSingleBenchmark(ctx, e.updateTableWorker, catalogName, schemaName, tableName)

}

func (e *BenchmarkEngine) RunUpdateView(ctx context.Context) error {
	catalogName, namespaceName, err := createCatalogAndSchema(e.catalog, e.client)
	if err != nil {
		panic(err)
	}
	viewName, err := createView(e.catalog, e.client, catalogName, namespaceName)
	if err != nil {
		panic(err)
	}

	return e.runSingleBenchmark(ctx, e.updateViewWorker, catalogName, namespaceName, viewName)
}
func (e *BenchmarkEngine) RunUpdateModel(ctx context.Context) error {
	catalogName, schemaName, err := createCatalogAndSchema(e.catalog, e.client)
	if err != nil {
		panic(err)
	}
	modelName, err := createModel(e.catalog, e.client, catalogName, schemaName)
	if err != nil {
		panic(err)

	}
	return e.runSingleBenchmark(ctx, e.updateModelWorker, catalogName, schemaName, modelName)
}
func (e *BenchmarkEngine) RunUpdateVolume(ctx context.Context) error {
	catalogName, schemaName, err := createCatalogAndSchema(e.catalog, e.client)
	if err != nil {
		panic(err)
	}
	volumeName, err := createVolume(e.catalog, e.client, catalogName, schemaName)
	if err != nil {
		panic(err)
	}

	return e.runSingleBenchmark(ctx, e.updateVolumeWorker, catalogName, schemaName, volumeName)
}

func (e *BenchmarkEngine) RunCreateDeleteListSchema(ctx context.Context) error {
	catalogName := uuid.NewString()
	log.Println("Creating catalog", catalogName)
	_, err := createCatalogRequest(e.catalog, e.client, catalogName)
	if err != nil {
		return err
	}

	return e.runMixedBenchmark(ctx, []WorkerConfig{
		{e.listSchemasWorker, 0.1, []string{catalogName}},
		{e.createDeleteSchemaWorker, 0.9, []string{catalogName}},
	})
}

func (e *BenchmarkEngine) RunCreateDeleteListCatalog(ctx context.Context) error {
	return e.runMixedBenchmark(ctx, []WorkerConfig{
		{e.listCatalogsWorker, 0.1, make([]string, 0)},
		{e.createDeleteCatalogWorker, 0.9, make([]string, 0)},
	})
}

func (e *BenchmarkEngine) RunCreateDeleteListPrincipal(ctx context.Context) error {
	return e.runMixedBenchmark(ctx, []WorkerConfig{
		{e.listPrincipalsWorker, 0.1, make([]string, 0)},
		{e.createDeletePrincipalWorker, 0.9, make([]string, 0)},
	})
}

func (e *BenchmarkEngine) RunCreateDeleteListTable(ctx context.Context) error {
	catalogName, schemaName, _ := createCatalogAndSchema(e.catalog, e.client)
	if e.catalog == "polaris" {
		err := polaris.GrantCatalogPermissions(ctx, catalogName)
		if err != nil {
			return err
		}
	}

	return e.runMixedBenchmark(ctx, []WorkerConfig{
		{e.listTablesWorker, 0.1, []string{catalogName, schemaName}},
		{e.createDeleteTableWorker, 0.9, []string{catalogName, schemaName}},
	})
}

func (e *BenchmarkEngine) RunCreateDeleteListView(ctx context.Context) error {
	catalogName, namespaceName, err := createCatalogAndSchema(e.catalog, e.client)
	if err != nil {
		panic(err)
	}

	return e.runMixedBenchmark(ctx, []WorkerConfig{
		{e.listViewsWorker, 0.1, []string{catalogName, namespaceName}},
		{e.createDeleteViewWorker, 0.9, []string{catalogName, namespaceName}},
	})
}
func (e *BenchmarkEngine) RunCreateDeleteListFunction(ctx context.Context) error {
	catalogName, schemaName, err := createCatalogAndSchema(e.catalog, e.client)
	if err != nil {
		panic(err)
	}

	return e.runMixedBenchmark(ctx, []WorkerConfig{
		{e.listFunctionsWorker, 0.1, []string{catalogName, schemaName}},
		{e.createDeleteFunctionWorker, 0.9, []string{catalogName, schemaName}},
	})
}
func (e *BenchmarkEngine) RunCreateDeleteListModel(ctx context.Context) error {
	catalogName, schemaName, err := createCatalogAndSchema(e.catalog, e.client)
	if err != nil {
		panic(err)
	}

	return e.runMixedBenchmark(ctx, []WorkerConfig{
		{e.listModelsWorker, 0.1, []string{catalogName, schemaName}},
		{e.createDeleteModelWorker, 0.9, []string{catalogName, schemaName}},
	})
}
func (e *BenchmarkEngine) RunCreateDeleteListVolume(ctx context.Context) error {
	catalogName, schemaName, err := createCatalogAndSchema(e.catalog, e.client)
	if err != nil {
		panic(err)
	}

	return e.runMixedBenchmark(ctx, []WorkerConfig{
		{e.listVolumesWorker, 0.1, []string{catalogName, schemaName}},
		{e.createDeleteVolumeWorker, 0.9, []string{catalogName, schemaName}},
	})
}

func (e *BenchmarkEngine) RunCreateUpdateGetCatalog(ctx context.Context) error {
	return e.runSingleBenchmark(ctx, e.createUpdateGetCatalogWorker)
}

func (e *BenchmarkEngine) RunCreateUpdateGetPrincipal(ctx context.Context) error {
	principalName := uuid.NewString()
	log.Println("Creating principal", principalName)
	_, err := createPrincipalRequest(e.catalog, e.client, principalName)
	if err != nil {
		return err
	}

	return e.runSingleBenchmark(ctx, e.createUpdateGetPrincipalWorker, principalName)

}

func (e *BenchmarkEngine) RunCreateUpdateGetSchema(ctx context.Context) error {
	catalogName := uuid.NewString()
	namespaceName := uuid.NewString()
	log.Println("Creating catalog", catalogName)
	_, err := createCatalogRequest(e.catalog, e.client, catalogName)
	if err != nil {
		return err
	}

	log.Println("Creating namespace", namespaceName)
	_, err = createSchemaRequest(e.catalog, e.client, catalogName, namespaceName)
	if err != nil {
		return err
	}

	return e.runSingleBenchmark(ctx, e.createUpdateGetSchemaWorker, catalogName, namespaceName)
}

func (e *BenchmarkEngine) RunCreateUpdateGetTable(ctx context.Context) error {
	catalogName, schemaName, _ := createCatalogAndSchema(e.catalog, e.client)

	if e.catalog == "polaris" {
		err := polaris.GrantCatalogPermissions(ctx, catalogName)
		if err != nil {
			return err
		}
	}

	tableName, _ := createTable(e.catalog, e.client, catalogName, schemaName)

	return e.runSingleBenchmark(ctx, e.createUpdateGetTableWorker, catalogName, schemaName, tableName)
}

func (e *BenchmarkEngine) RunCreateUpdateGetView(ctx context.Context) error {
	catalogName, namespaceName, err := createCatalogAndSchema(e.catalog, e.client)
	if err != nil {
		panic(err)
	}
	viewName, err := createView(e.catalog, e.client, catalogName, namespaceName)

	return e.runSingleBenchmark(ctx, e.updateGetViewWorker, catalogName, namespaceName, viewName)
}

func (e *BenchmarkEngine) RunCreateUpdateGetModel(ctx context.Context) error {
	catalogName, schemaName, err := createCatalogAndSchema(e.catalog, e.client)
	if err != nil {
		panic(err)
	}
	modelName, err := createModel(e.catalog, e.client, catalogName, schemaName)
	if err != nil {
		panic(err)
	}

	return e.runSingleBenchmark(ctx, e.updateGetModelWorker, catalogName, schemaName, modelName)
}
func (e *BenchmarkEngine) RunCreateUpdateGetVolume(ctx context.Context) error {
	catalogName, schemaName, err := createCatalogAndSchema(e.catalog, e.client)
	if err != nil {
		panic(err)
	}
	volumeName, err := createVolume(e.catalog, e.client, catalogName, schemaName)
	if err != nil {
		panic(err)
	}

	return e.runSingleBenchmark(ctx, e.updateGetVolumeWorker, catalogName, schemaName, volumeName)
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
			threadID := threadAssigned
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
