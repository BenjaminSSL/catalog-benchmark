package internal

import (
	"benchmark/internal/common"
	"context"
	"github.com/google/uuid"
	"net/http"
	"sync"
	"time"
)

type WorkerConfig struct {
	workerFunc WorkerFunc
	threads    int
	params     map[string]interface{}
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

func (e *BenchmarkEngine) GrantPermissionCatalog(ctx context.Context, catalogName string) error {
	_, err := e.Catalog.GrantPermissionCatalog(ctx, catalogName, map[string]interface{}{
		"privilege": "TABLE_WRITE_DATA",
	})
	if err != nil {
		return err
	}

	_, err = e.Catalog.GrantPermissionCatalog(ctx, catalogName, map[string]interface{}{
		"privilege": "TABLE_READ_DATA",
	})
	if err != nil {
		return err
	}

	return nil
}

func (e *BenchmarkEngine) CreateCatalog(ctx context.Context) (string, error) {
	catalogName := uuid.NewString()

	_, err := e.Catalog.CreateCatalog(ctx, catalogName)
	if err != nil {
		return "", err
	}

	return catalogName, nil
}

func (e *BenchmarkEngine) CreteSchema(ctx context.Context, catalogName string) (string, error) {
	schemaName := uuid.NewString()

	_, err := e.Catalog.CreateSchema(ctx, catalogName, schemaName)
	if err != nil {
		return "", err
	}

	return schemaName, nil
}

func (e *BenchmarkEngine) RunCreateCatalog(ctx context.Context) error {
	return e.runBenchmark(ctx, []WorkerConfig{
		{createCatalogWorker, e.threads, make(map[string]interface{})},
	})
}

func (e *BenchmarkEngine) RunCreatePrincipal(ctx context.Context) error {

	return e.runBenchmark(ctx, []WorkerConfig{
		{createPrincipalWorker, e.threads, make(map[string]interface{})},
	})
}

func (e *BenchmarkEngine) RunCreateSchema(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{createSchemaWorker, e.threads, map[string]interface{}{"catalogName": catalogName}},
	})
}

func (e *BenchmarkEngine) RunCreateTable(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}

	err = e.GrantPermissionCatalog(ctx, catalogName)

	return e.runBenchmark(ctx, []WorkerConfig{
		{createTableWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	})
}

func (e *BenchmarkEngine) RunCreateView(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{createViewWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	})
}

func (e *BenchmarkEngine) RunCreateFunction(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{createFunctionWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	})
}
func (e *BenchmarkEngine) RunCreateModel(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{createModelWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	})
}
func (e *BenchmarkEngine) RunCreateVolume(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{createVolumeWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	})
}

func (e *BenchmarkEngine) RunCreateDeleteCatalog(ctx context.Context) error {
	return e.runBenchmark(ctx, []WorkerConfig{
		{createDeleteCatalogWorker, e.threads, make(map[string]interface{})},
	})
}

func (e *BenchmarkEngine) RunCreateDeleteSchema(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{createDeleteSchemaWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName}},
	})
}

func (e *BenchmarkEngine) RunCreateDeletePrincipal(ctx context.Context) error {
	return e.runBenchmark(ctx, []WorkerConfig{
		{createDeletePrincipalWorker, e.threads, make(map[string]interface{})},
	})
}

func (e *BenchmarkEngine) RunCreateDeleteTable(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}

	err = e.GrantPermissionCatalog(ctx, catalogName)

	return e.runBenchmark(ctx, []WorkerConfig{
		{createDeleteTableWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	})
}

func (e *BenchmarkEngine) RunCreateDeleteView(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{createDeleteViewWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	})
}

func (e *BenchmarkEngine) RunCreateDeleteFunction(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{createFunctionWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	})
}

func (e *BenchmarkEngine) RunCreateDeleteModel(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{createDeleteModelWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	})
}
func (e *BenchmarkEngine) RunCreateDeleteVolume(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{createDeleteVolumeWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	})
}

func (e *BenchmarkEngine) RunUpdateCatalog(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{updateCatalogWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName}},
	})
}

func (e *BenchmarkEngine) RunUpdatePrincipal(ctx context.Context) error {
	principalName := uuid.NewString()

	_, err := e.Catalog.CreatePrincipal(ctx, principalName)
	if err != nil {
		return err
	}
	return e.runBenchmark(ctx, []WorkerConfig{
		{updatePrincipalWorker, e.threads, map[string]interface{}{
			"principalName": principalName}},
	})
}

func (e *BenchmarkEngine) RunUpdateSchema(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{updateSchemaWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	})
}

func (e *BenchmarkEngine) RunUpdateTable(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}

	tableName := uuid.NewString()

	_, err = e.Catalog.CreatePrincipal(ctx, tableName)
	if err != nil {
		return err
	}

	err = e.GrantPermissionCatalog(ctx, tableName)
	return e.runBenchmark(ctx, []WorkerConfig{
		{updateTableWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName, "tableName": tableName}},
	})

}

func (e *BenchmarkEngine) RunUpdateView(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}

	viewName := uuid.NewString()
	_, err = e.Catalog.CreateView(ctx, catalogName, schemaName, viewName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{updateViewWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName, "viewName": viewName}},
	})
}

func (e *BenchmarkEngine) RunUpdateModel(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}
	modelName := uuid.NewString()

	_, err = e.Catalog.CreateModel(ctx, catalogName, schemaName, modelName)
	if err != nil {
		return err
	}
	return e.runBenchmark(ctx, []WorkerConfig{
		{updateModelWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName, "modelName": modelName}},
	})
}
func (e *BenchmarkEngine) RunUpdateVolume(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}
	volumeName := uuid.NewString()
	_, err = e.Catalog.CreateVolume(ctx, catalogName, schemaName, volumeName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{updateVolumeWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName, "volumeName": volumeName}},
	})
}

func (e *BenchmarkEngine) RunCreateDeleteListSchema(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{listSchemasWorker, 1, map[string]interface{}{"catalogName": catalogName}},
		{createDeleteSchemaWorker, e.threads - 1, map[string]interface{}{"catalogName": catalogName}},
	})
}

func (e *BenchmarkEngine) RunCreateDeleteListCatalog(ctx context.Context) error {
	return e.runBenchmark(ctx, []WorkerConfig{
		{listCatalogsWorker, 1, make(map[string]interface{})},
		{createDeleteCatalogWorker, e.threads - 1, make(map[string]interface{})},
	})
}

func (e *BenchmarkEngine) RunCreateDeleteListPrincipal(ctx context.Context) error {
	return e.runBenchmark(ctx, []WorkerConfig{
		{listPrincipalsWorker, 1, make(map[string]interface{})},
		{createDeletePrincipalWorker, e.threads - 1, make(map[string]interface{})},
	})
}

func (e *BenchmarkEngine) RunCreateDeleteListTable(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}

	err = e.GrantPermissionCatalog(ctx, catalogName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{listTablesWorker, 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
		{createDeleteTableWorker, e.threads - 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	})
}

func (e *BenchmarkEngine) RunCreateDeleteListView(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{listViewsWorker, 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
		{createDeleteViewWorker, e.threads - 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	})
}
func (e *BenchmarkEngine) RunCreateDeleteListFunction(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}
	return e.runBenchmark(ctx, []WorkerConfig{
		{listFunctionsWorker, 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
		{createDeleteFunctionWorker, e.threads - 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	})
}
func (e *BenchmarkEngine) RunCreateDeleteListModel(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{listModelsWorker, 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
		{createDeleteModelWorker, e.threads - 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	})
}
func (e *BenchmarkEngine) RunCreateDeleteListVolume(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{listVolumesWorker, 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
		{createDeleteVolumeWorker, e.threads - 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	})
}

func (e *BenchmarkEngine) RunCreateUpdateGetCatalog(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}
	return e.runBenchmark(ctx, []WorkerConfig{
		{createUpdateGetCatalogWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName}},
	})
}

func (e *BenchmarkEngine) RunCreateUpdateGetPrincipal(ctx context.Context) error {
	principalName := uuid.NewString()
	_, err := e.Catalog.CreatePrincipal(ctx, principalName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{createUpdateGetPrincipalWorker, e.threads, map[string]interface{}{
			"principalName": principalName}},
	})

}

func (e *BenchmarkEngine) RunCreateUpdateGetSchema(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{createUpdateGetSchemaWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	})
}

func (e *BenchmarkEngine) RunCreateUpdateGetTable(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}

	tableName := uuid.NewString()
	_, err = e.Catalog.CreateTable(ctx, catalogName, schemaName, tableName)
	if err != nil {
		return err
	}

	err = e.GrantPermissionCatalog(ctx, catalogName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{createUpdateGetTableWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName, "tableName": tableName}},
	})
}

func (e *BenchmarkEngine) RunCreateUpdateGetView(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}
	viewName := uuid.NewString()
	_, err = e.Catalog.CreateView(ctx, catalogName, schemaName, viewName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{createUpdateGetViewWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName, "viewName": viewName}},
	})
}

func (e *BenchmarkEngine) RunCreateUpdateGetModel(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}
	modelName := uuid.NewString()
	_, err = e.Catalog.CreateModel(ctx, catalogName, schemaName, modelName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{createUpdateGetModelWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName, "modelName": modelName}},
	})
}
func (e *BenchmarkEngine) RunCreateUpdateGetVolume(ctx context.Context) error {
	catalogName, err := e.CreateCatalog(ctx)
	if err != nil {
		return err
	}

	schemaName, err := e.CreteSchema(ctx, catalogName)
	if err != nil {
		return err
	}
	volumeName := uuid.NewString()
	_, err = e.Catalog.CreateVolume(ctx, catalogName, schemaName, volumeName)
	if err != nil {
		return err
	}

	return e.runBenchmark(ctx, []WorkerConfig{
		{createUpdateGetVolumeWorker, e.threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName, "volumeName": volumeName}},
	})
}

func (e *BenchmarkEngine) runBenchmark(ctx context.Context, workers []WorkerConfig) error {
	ctx, cancel := context.WithTimeout(ctx, e.duration)
	defer cancel()

	var wg sync.WaitGroup

	threadAllocated := 0

	for _, worker := range workers {
		for t := 0; t < worker.threads; t++ {
			threadID := threadAllocated
			wg.Add(1)

			go func(threadID int, config WorkerConfig) {
				defer wg.Done()
				logger, _ := common.NewRoutineBatchLogger("./output/logs/tmp", e.ExperimentID, threadID, 20)
				defer logger.Close()

				w := &Worker{
					Func:    config.workerFunc,
					Client:  e.client,
					Logger:  logger,
					Catalog: e.Catalog, // or config.catalog if it varies per worker
					Step:    1,
				}

				w.Run(ctx, config.params)

			}(threadID, worker)

			threadAllocated++
		}
	}

	wg.Wait()
	return nil
}
