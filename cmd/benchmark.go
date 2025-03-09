package cmd

import (
	"benchmark/internal/common"
	"benchmark/internal/evaluate"
	"benchmark/internal/execution"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/google/uuid"
	"log"
	"os"
	"os/signal"
	"path/filepath"
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

	return &Command{
		Name:        "benchmark",
		Description: "Run the benchmark driver for the catalogs",
		Flags:       flags,
		Handler: func() error {
			// TODO: validate the flags
			benchmarkType := common.BenchmarkType(config.BenchmarkID)
			experiment := common.Experiment{
				ID:          config.ExperimentID,
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

	saveExperiment(experiment, "./output/experiments")

	log.Printf("Starting experiment %s with benchmark scenario %d", experiment.ID, experiment.BenchmarkID)

	context, err := common.GetRequestContextFromEnv(experiment.Catalog)
	if err != nil {
		return err
	}

	executionPlans, err := GenerateExecutionPlan(context, experiment)
	if err != nil {
		log.Printf("Error getting execution scenario: %s\n", err)
		return err
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan error)

	engine := execution.NewExecutionEngine(experiment.ID, executionPlans)
	startTime := time.Now()

	go func() {
		done <- engine.Run()
	}()

	select {
	case err = <-done:
		if err != nil {
			return err
		}

		elapsed := time.Since(startTime)

		log.Printf("\nFinished in %f seconds experiment %s\n", elapsed.Seconds(), experiment.ID)

		err = evaluate.BenchmarkExecution(context, experiment)
		if err != nil {
			log.Printf("Error evaluating benchmark: %s\n", err)
		}

		return common.MergeLogs("./output/logs/tmp", experiment.ID)

	case sig := <-quit:
		log.Printf("Received signal \"%v\", shutting down...", sig)
		log.Printf("Stopping experiment %s with benchmark scenario %d\n", experiment.ID, experiment.BenchmarkID)
		return nil
	}

}

func GenerateExecutionPlan(context common.RequestContext, experiment common.Experiment) (*execution.Plan, error) {
	generator := execution.NewExecutionPlanGenerator(context, experiment.Catalog, experiment.Threads, experiment.Repeat)

	switch experiment.BenchmarkID {
	case common.CreateCatalogBenchmark:
		return generator.CreateCatalog()
	case common.CreateDeleteCatalogBenchmark:
		return generator.CreateDeleteCatalog()
	case common.UpdateCatalogBenchmark:
		log.Printf("Updating catalog benchmark")
		return generator.UpdateCatalog()
	default:
		return nil, fmt.Errorf("unknown benchmark %v for catalog: %s", experiment.BenchmarkID, experiment.Catalog)
	}

}

func saveExperiment(experiment common.Experiment, dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	filePath := filepath.Join(dir, experiment.ID+".json")

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
