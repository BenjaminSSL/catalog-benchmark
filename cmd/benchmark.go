package cmd

import (
	"benchmark/internal/common"
	"benchmark/internal/evaluate"
	"benchmark/internal/execution"
	"benchmark/internal/plan"
	"os"
	"os/signal"
	"syscall"

	"flag"
	"github.com/google/uuid"
	"log"
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
			return runBenchmark(config.ExperimentID, benchmarkType, config.Catalog, config.Threads, config.Repeat)
		},
	}
}

func runBenchmark(experimentID string, benchmarkID common.BenchmarkType, catalogName string, threads int, repeat int) error {
	experiment := common.Experiment{
		ID:          experimentID,
		BenchmarkID: benchmarkID,
		Catalog:     catalogName,
		Threads:     threads,
		Repeat:      repeat,
	}
	log.Printf("Starting experiment %s with benchmark scenario %d", experimentID, benchmarkID)

	context, err := common.GetRequestContextFromEnv(catalogName)
	if err != nil {
		return err
	}

	executionPlans, err := plan.GenerateExecutionPlan(context, experiment)
	if err != nil {
		log.Printf("Error getting execution scenario: %s\n", err)
		return err
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan error)

	engine := execution.NewExecutionEngine(experimentID, executionPlans)
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

		log.Printf("Finished in %f seconds experiment %s\n", elapsed.Seconds(), experimentID)

		err = evaluate.BenchmarkExecution(context, experiment)
		if err != nil {
			log.Printf("Error evaluating benchmark: %s\n", err)
		}

		return common.MergeLogs("./logs/tmp", experimentID)

	case sig := <-quit:
		log.Printf("Received signal \"%v\", shutting down...", sig)
		log.Printf("Stopping experiment %s with benchmark scenario %d\n", experimentID, benchmarkID)
		return nil
	}

}
