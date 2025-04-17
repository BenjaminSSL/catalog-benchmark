package cmd

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/common"
	"benchmark/internal/execution"
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
		ExperimentID string
		BenchmarkID  int
		Catalog      string
		Threads      int
		Entity       string
		Duration     int
	}{
		// Default values
		ExperimentID: uuid.NewString(),
		BenchmarkID:  1,
		Catalog:      "polaris",
		Threads:      1,
		Entity:       "catalog",
		Duration:     20,
	}

	flags.StringVar(&config.ExperimentID, "experiment-id", config.ExperimentID, "Experiment ID")
	flags.IntVar(&config.BenchmarkID, "benchmark-id", config.BenchmarkID, "Benchmark ID")
	flags.StringVar(&config.Catalog, "catalog", config.Catalog, "Catalog")
	flags.IntVar(&config.Threads, "threads", config.Threads, "Threads")
	flags.StringVar(&config.Entity, "entity", config.Entity, "Entity")
	flags.IntVar(&config.Duration, "duration", config.Duration, "Duration (seconds)")

	experimentID, err := uuid.Parse(config.ExperimentID)
	if err != nil {
		log.Fatal(err)
	}
	config.ExperimentID = experimentID.String()

	return &Command{
		Name:        "benchmark",
		Description: "Run the benchmark driver for the catalogs",
		Flags:       flags,
		Handler: func() error {
			benchmarkType := common.BenchmarkType(config.BenchmarkID)
			entityType := common.EntityType(config.Entity)
			experiment := common.Experiment{
				ID:          experimentID,
				BenchmarkID: benchmarkType,
				Catalog:     config.Catalog,
				Threads:     config.Threads,
				Duration:    time.Duration(config.Duration) * time.Second,
				Entity:      entityType,
			}
			return runBenchmark(experiment)
		},
	}
}

func runBenchmark(experiment common.Experiment) error {
	log.Printf("Starting experiment %s with benchmark scenario %d on entity %s", experiment.ID, experiment.BenchmarkID, experiment.Entity)

	if experiment.Catalog == "polaris" {
		token, err := common.FetchPolarisToken()
		if err != nil {
			return err
		}

		polaris.SetToken(token)
	}

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan error)

	engine := execution.NewBenchmarkEngine(experiment.ID.String(), experiment.Catalog, experiment.Threads, experiment.Duration)
	startTime := time.Now()
	experiment.StartTimestamp = startTime

	defer common.DeleteLogs("./output/logs/tmp")

	go func() {
		switch experiment.BenchmarkID {
		case common.CreateBenchmark:
			switch experiment.Entity {
			case common.CatalogEntity:
				done <- engine.RunCreateCatalog(ctx)
			case common.PrincipalEntity:
				done <- engine.RunCreatePrincipal(ctx)
			case common.SchemaEntity:
				done <- engine.RunCreateSchema(ctx)
			case common.TableEntity:
				done <- engine.RunCreateTable(ctx)
			case common.ViewEntity:
				done <- engine.RunCreateView(ctx)
			case common.FunctionEntity:
				done <- engine.RunCreateFunction(ctx)
			case common.ModelEntity:
				done <- engine.RunCreateModel(ctx)
			case common.VolumeEntity:
				done <- engine.RunCreateVolume(ctx)
			default:
				log.Fatalf("unknown entity type: %s", experiment.Entity)
			}
		case common.CreateDeleteBenchmark:
			switch experiment.Entity {
			case common.CatalogEntity:
				done <- engine.RunCreateDeleteCatalog(ctx)
			case common.PrincipalEntity:
				done <- engine.RunCreateDeletePrincipal(ctx)
			case common.SchemaEntity:
				done <- engine.RunCreateDeleteSchema(ctx)
			case common.TableEntity:
				done <- engine.RunCreateDeleteTable(ctx)
			case common.ViewEntity:
				done <- engine.RunCreateDeleteView(ctx)
			case common.FunctionEntity:
				done <- engine.RunCreateDeleteFunction(ctx)
			case common.ModelEntity:
				done <- engine.RunCreateDeleteModel(ctx)
			case common.VolumeEntity:
				done <- engine.RunCreateDeleteVolume(ctx)
			default:
				log.Fatalf("unknown entity type: %s", experiment.Entity)
			}
		case common.CreateUpdateBenchmark:
			switch experiment.Entity {
			case common.CatalogEntity:
				done <- engine.RunCreateUpdateCatalog(ctx)
			case common.PrincipalEntity:
				done <- engine.RunCreateUpdatePrincipal(ctx)
			case common.SchemaEntity:
				done <- engine.RunCreateUpdateSchema(ctx)
			case common.TableEntity:
				done <- engine.RunCreateUpdateTable(ctx)
			case common.ViewEntity:
				done <- engine.RunCreateUpdateView(ctx)
			case common.ModelEntity:
				done <- engine.RunCreateUpdateModel(ctx)
			case common.VolumeEntity:
				done <- engine.RunCreateUpdateVolume(ctx)
			default:
				log.Fatalf("unknown entity type: %s", experiment.Entity)

			}
		case common.CreateDeleteListBenchmark:
			switch experiment.Entity {
			case common.CatalogEntity:
				done <- engine.RunCreateDeleteListCatalog(ctx)
			case common.PrincipalEntity:
				done <- engine.RunCreateDeleteListPrincipal(ctx)
			case common.SchemaEntity:
				done <- engine.RunCreateDeleteListSchema(ctx)
			case common.TableEntity:
				done <- engine.RunCreateDeleteListTable(ctx)
			case common.ViewEntity:
				done <- engine.RunCreateDeleteListView(ctx)
			case common.FunctionEntity:
				done <- engine.RunCreateDeleteListFunction(ctx)
			case common.ModelEntity:
				done <- engine.RunCreateDeleteListModel(ctx)
			case common.VolumeEntity:
				done <- engine.RunCreateDeleteListVolume(ctx)
			default:
				log.Fatalf("unknown entity type: %s", experiment.Entity)

			}
		case common.UpdateGetBenchmark:
			switch experiment.Entity {
			case common.CatalogEntity:
				done <- engine.RunUpdateGetCatalog(ctx)
			case common.PrincipalEntity:
				done <- engine.RunUpdateGetPrincipal(ctx)
			case common.SchemaEntity:
				done <- engine.RunUpdateGetSchema(ctx)
			case common.TableEntity:
				done <- engine.RunUpdateGetTable(ctx)
			case common.ViewEntity:
				done <- engine.RunUpdateGetView(ctx)
			case common.ModelEntity:
				done <- engine.RunUpdateGetModel(ctx)
			case common.VolumeEntity:
				done <- engine.RunUpdateGetVolume(ctx)
			default:
				log.Fatalf("unknown entity type: %s", experiment.Entity)
			}

		default:
			done <- nil
		}

	}()

	go func() {
		sig := <-quit
		log.Printf("Received signal \"%v\", shutting down...", sig)
		log.Printf("Stopping experiment %s with benchmark scenario %d\n", experiment.ID, experiment.BenchmarkID)
		cancel()
		done <- nil // Gracefully stop
	}()

	select {
	case err := <-done:
		if err != nil {
			return err
		}

		elapsed := time.Since(startTime)
		experiment.EndTimestamp = time.Now()
		log.Printf("Finished in %.2f seconds experiment %s\n", elapsed.Seconds(), experiment.ID)

		// Start benchmark evaluation
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()

			log.Printf("Merging logs...\n")
			// Merge logs after evaluation
			if err := common.MergeLogs("./output/logs/tmp", experiment.ID.String()); err != nil {
				log.Printf("Error merging logs: %s\n", err)
			}
			log.Printf("Logs merged\n")

			log.Printf("Saving experiment...\n")
			if err := saveExperiment(experiment, "./output/experiments"); err != nil {
				log.Printf("Error saving experiment: %s\n", err)
			}

			log.Printf("Experiment saved\n")
		}()

		// Wait for benchmark and log merge to finish
		wg.Wait()
		return nil
	}

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
