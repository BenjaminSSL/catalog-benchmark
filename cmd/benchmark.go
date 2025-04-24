package cmd

import (
	"benchmark/internal"
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/catalog/unity"
	"benchmark/internal/common"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

func init() {
	RegisterCommand(newBenchmarkCommand())
}

func newBenchmarkCommand() *Command {
	flags := flag.NewFlagSet("benchmark", flag.ExitOnError)

	// Anonymous flag config struct
	config := struct {
		ExperimentID uuid.UUID
		BenchmarkID  int
		Catalog      string
		Threads      int
		Entity       string
		Duration     string
	}{
		// Default values
		ExperimentID: uuid.New(),
		BenchmarkID:  1,
		Catalog:      "polaris",
		Threads:      1,
		Entity:       "catalog",
		Duration:     "10s",
	}

	flags.IntVar(&config.BenchmarkID, "benchmark-id", config.BenchmarkID, "Benchmark ID")
	flags.StringVar(&config.Catalog, "catalog", config.Catalog, "Catalog")
	flags.IntVar(&config.Threads, "threads", config.Threads, "Threads")
	flags.StringVar(&config.Entity, "entity", config.Entity, "Entity")
	flags.StringVar(&config.Duration, "duration", config.Duration, "Duration")

	return &Command{
		Name:        "benchmark",
		Description: "Run the benchmark driver for the catalogs",
		Flags:       flags,
		Handler: func() error {
			benchmarkType := common.BenchmarkType(config.BenchmarkID)
			entityType := common.EntityType(config.Entity)
			duration, err := time.ParseDuration(config.Duration)
			if err != nil {
				log.Fatal(err)
			}

			experiment := common.Experiment{
				ID:          config.ExperimentID,
				BenchmarkID: benchmarkType,
				Catalog:     config.Catalog,
				Threads:     config.Threads,
				Duration:    duration,
				Entity:      entityType,
			}
			return runBenchmark(experiment)
		},
	}
}

func runBenchmark(experiment common.Experiment) error {
	log.Printf("Starting experiment %s with benchmark scenario %d on entity %s", experiment.ID, experiment.BenchmarkID, experiment.Entity)

	// Setup the catalog
	catalog, err := setupCatalog(experiment.Catalog)
	if err != nil {
		return fmt.Errorf("failed to setup catalog: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan error, 1)

	// Setup the benchmark engine
	engine := internal.NewBenchmarkEngine(experiment.ID.String(), catalog, experiment.Threads, experiment.Duration)

	// Set start time
	startTime := time.Now()
	experiment.StartTimestamp = startTime

	// Clean up logs directory
	defer func() {
		err := common.DeleteLogs("./output/logs/tmp")
		if err != nil {
			log.Printf("Error deleting logs: %s", err)
		}
	}()

	workers, err := setupWorkers(ctx, experiment, catalog)
	if err != nil {
		return err
	}

	go func(workers []internal.WorkerConfig) {
		if err = engine.RunBenchmark(ctx, workers); err != nil {
			log.Printf("Error running benchmark: %s", err)
			done <- err
			return
		}
		done <- nil
		log.Printf("Benchmark completed successfully")

	}(workers)

	go handleShutdownSignal(quit, done, cancel, experiment)

	return processResults(done, startTime, experiment)

}

func saveExperiment(experiment common.Experiment, dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	filePath := filepath.Join(dir, experiment.ID.String()+".json")

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	jsonContent, err := json.MarshalIndent(experiment, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal experiment: %w", err)
	}

	if _, err := file.Write(jsonContent); err != nil {
		return fmt.Errorf("failed to write to file %s: %w", filePath, err)
	}

	return nil
}

func handleShutdownSignal(quit chan os.Signal, done chan error, cancel context.CancelFunc, experiment common.Experiment) {
	sig := <-quit
	log.Printf("Received signal %q, shutting down...", sig)
	log.Printf("Stopping experiment %s with benchmark scenario %d", experiment.ID, experiment.BenchmarkID)
	cancel()
	done <- nil
}

func setupCatalog(catalog string) (internal.Catalog, error) {
	switch catalog {
	case "polaris":
		token, err := common.FetchPolarisToken()
		if err != nil {
			return nil, err
		}
		polaris.SetToken(token)
		return &polaris.Catalog{}, nil
	case "unity":
		return &unity.Catalog{}, nil
	default:
		return nil, fmt.Errorf("unsupported catalog %s", catalog)
	}
}

func setupWorkers(ctx context.Context, experiment common.Experiment, catalog internal.Catalog) ([]internal.WorkerConfig, error) {
	var benchmarkMap map[common.EntityType]func(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error)
	switch experiment.BenchmarkID {
	case common.CreateBenchmark:
		benchmarkMap = createBenchmarkMap()
	case common.CreateDeleteBenchmark:
		benchmarkMap = createDeleteBenchmarkMap()
	case common.UpdateBenchmark:
		benchmarkMap = updateBenchmarkMap()
	case common.CreateDeleteListBenchmark:
		benchmarkMap = createDeleteListBenchmarkMap()
	case common.UpdateGetBenchmark:
		benchmarkMap = updateGetBenchmarkMap()

	default:
		return nil, fmt.Errorf("unsupported benchmark type %d", experiment.BenchmarkID)
	}

	workerFunc, exists := benchmarkMap[experiment.Entity]
	if !exists {
		return nil, fmt.Errorf("unsupported entity type %s for benchmark %d", experiment.Entity, experiment.BenchmarkID)
	}

	workers, err := workerFunc(ctx, catalog, experiment.Threads)
	if err != nil {
		return nil, fmt.Errorf("failed to setup workers: %v", err)
	}

	return workers, nil
}

func processResults(done chan error, startTime time.Time, experiment common.Experiment) error {
	err := <-done
	if err != nil {
		return err
	}

	elapsed := time.Since(startTime)
	experiment.EndTimestamp = time.Now()
	log.Printf("Finished in %.2f seconds experiment %s", elapsed.Seconds(), experiment.ID)

	// Process experiment results in the background
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		log.Printf("Merging logs...")
		if err := common.MergeLogs("./output/logs/tmp", experiment.ID.String()); err != nil {
			log.Printf("Error merging logs: %s", err)
		}
		log.Printf("Logs merged")

		log.Printf("Saving experiment...")
		if err := saveExperiment(experiment, "./output/experiments"); err != nil {
			log.Printf("Error saving experiment: %s", err)
		}
		log.Printf("Experiment saved")
	}()

	// Wait for result processing to complete
	wg.Wait()
	return nil
}

func createBenchmarkMap() map[common.EntityType]func(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	return map[common.EntityType]func(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error){
		common.CatalogEntity: func(_ context.Context, _ internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
			return internal.SetupCreateCatalog(threads)
		},
		common.PrincipalEntity: func(_ context.Context, _ internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
			return internal.SetupCreatePrincipal(threads)
		},
		common.SchemaEntity:   internal.SetupCreateSchema,
		common.TableEntity:    internal.SetupCreateTable,
		common.ViewEntity:     internal.SetupCreateView,
		common.FunctionEntity: internal.SetupCreateFunction,
		common.ModelEntity:    internal.SetupCreateModel,
		common.VolumeEntity:   internal.SetupCreateVolume,
	}
}

func createDeleteBenchmarkMap() map[common.EntityType]func(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	return map[common.EntityType]func(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error){
		common.CatalogEntity:   internal.SetupCreateDeleteCatalog,
		common.PrincipalEntity: internal.SetupCreateDeletePrincipal,
		common.SchemaEntity:    internal.SetupCreateDeleteSchema,
		common.TableEntity:     internal.SetupCreateDeleteTable,
		common.ViewEntity:      internal.SetupCreateDeleteView,
		common.FunctionEntity:  internal.SetupCreateDeleteFunction,
		common.ModelEntity:     internal.SetupCreateDeleteModel,
		common.VolumeEntity:    internal.SetupCreateDeleteVolume,
	}
}

func updateBenchmarkMap() map[common.EntityType]func(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	return map[common.EntityType]func(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error){
		common.CatalogEntity:   internal.SetupUpdateCatalog,
		common.PrincipalEntity: internal.SetupUpdatePrincipal,
		common.SchemaEntity:    internal.SetupUpdateSchema,
		common.TableEntity:     internal.SetupUpdateTable,
		common.ViewEntity:      internal.SetupUpdateView,
		common.ModelEntity:     internal.SetupUpdateModel,
		common.VolumeEntity:    internal.SetupUpdateVolume,
	}
}

func createDeleteListBenchmarkMap() map[common.EntityType]func(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	return map[common.EntityType]func(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error){
		common.CatalogEntity:   internal.SetupCreateDeleteListCatalog,
		common.PrincipalEntity: internal.SetupCreateDeleteListPrincipal,
		common.SchemaEntity:    internal.SetupCreateDeleteListSchema,
		common.TableEntity:     internal.SetupCreateDeleteListTable,
		common.ViewEntity:      internal.SetupCreateDeleteListView,
		common.FunctionEntity:  internal.SetupCreateDeleteListFunction,
		common.ModelEntity:     internal.SetupCreateDeleteListModel,
		common.VolumeEntity:    internal.SetupCreateDeleteListVolume,
	}
}

func updateGetBenchmarkMap() map[common.EntityType]func(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	return map[common.EntityType]func(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error){
		common.CatalogEntity:   internal.SetupUpdateGetCatalog,
		common.PrincipalEntity: internal.SetupUpdateGetPrincipal,
		common.SchemaEntity:    internal.SetupUpdateGetSchema,
		common.TableEntity:     internal.SetupUpdateGetTable,
		common.ViewEntity:      internal.SetupUpdateGetView,
		common.ModelEntity:     internal.SetupUpdateGetModel,
		common.VolumeEntity:    internal.SetupUpdateGetVolume,
	}
}
