package cmd

import (
	"benchmark/internal/common"
	"benchmark/internal/evaluate"
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
		Repeat       int
	}{
		// Default values
		ExperimentID: uuid.NewString(),
		BenchmarkID:  1,
		Catalog:      "polaris",
		Threads:      1,
		Repeat:       1,
	}

	flags.StringVar(&config.ExperimentID, "experiment-id", config.ExperimentID, "Experiment ID")
	flags.IntVar(&config.BenchmarkID, "benchmark-id", config.BenchmarkID, "Benchmark ID")
	flags.StringVar(&config.Catalog, "catalog", config.Catalog, "Catalog")
	flags.IntVar(&config.Threads, "threads", config.Threads, "Threads")
	flags.IntVar(&config.Repeat, "repeat", config.Repeat, "Repeats")

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
			// TODO: validate the flags
			benchmarkType := common.BenchmarkType(config.BenchmarkID)
			experiment := common.Experiment{
				ID:          experimentID,
				BenchmarkID: benchmarkType,
				Catalog:     config.Catalog,
				Threads:     config.Threads,
				Repeat:      config.Repeat,
			}
			return runBenchmark(experiment)
		},
	}
}

func runBenchmark(experiment common.Experiment) error {
	log.Printf("Starting experiment %s with benchmark scenario %d", experiment.ID, experiment.BenchmarkID)

	config, err := common.GetRequestConfigFromEnv(experiment.Catalog)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, "config", config)

	defer cancel()

	executionPlans, err := GenerateExecutionPlan(ctx, experiment)
	if err != nil {
		log.Printf("Error getting execution scenario: %s\n", err)
		return err
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan error)

	engine := execution.NewExecutionEngine(experiment.ID, executionPlans)
	startTime := time.Now()
	experiment.StartTimestamp = startTime

	defer common.DeleteLogs("./output/logs/tmp")

	if err != nil {
		return err
	}

	go func() {
		done <- engine.Run(ctx)
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

			log.Printf("Evaluating benchmark...\n")
			evaluation, err := evaluate.Benchmark(ctx, experiment)
			if err != nil {
				log.Printf("Error evaluating benchmark: %s\n", err)
				return
			}
			log.Printf("Benchmark evaluation finished\n")

			log.Printf("Saving evaluation...\n")
			if err := saveEvaluation(evaluation, "./output/evaluations"); err != nil {
				log.Printf("Error saving evaluation: %s\n", err)
			}
			log.Printf("Evaluation saved\n")

		}()

		// Wait for benchmark and log merge to finish
		wg.Wait()
		return nil
	}

}

func GenerateExecutionPlan(ctx context.Context, experiment common.Experiment) (*execution.Plan, error) {
	generator := execution.NewExecutionPlanGenerator(experiment.Catalog, experiment.Threads, experiment.Repeat)

	switch experiment.BenchmarkID {
	case common.CreateCatalogBenchmark:
		return generator.CreateCatalog(ctx)
	case common.CreateDeleteCatalogBenchmark:
		return generator.CreateDeleteCatalog(ctx)
	case common.CreateUpdateCatalogBenchmark:
		return generator.CreateUpdateCatalog(ctx)
	case common.CreateDeleteListCatalogBenchmark:
		return generator.CreateDeleteListCatalog(ctx)
	case common.UpdatePropertiesCatalogBenchmark:
		return generator.UpdatePropertiesCatalog(ctx)
	case common.UpdateGetCatalogBenchmark:
		return generator.UpdateGetCatalog(ctx)
	default:
		return nil, fmt.Errorf("unknown benchmark %v for catalog: %s", experiment.BenchmarkID, experiment.Catalog)
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
func saveEvaluation(evaluation *evaluate.Evaluation, dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	filePath := filepath.Join(dir, evaluation.ExperimentID.String()+".json")

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	jsonContent, err := json.MarshalIndent(evaluation, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal experiment: %w", err)
	}

	if _, err := file.Write(jsonContent); err != nil {
		return fmt.Errorf("failed to write to file %s: %w", filePath, err)
	}

	return nil

}
