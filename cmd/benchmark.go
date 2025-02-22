package cmd

import (
	"benchmark/internal/common"
	"benchmark/internal/execution"

	"benchmark/internal/scenario"
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

	flags.StringVar(&config.ExperimentID, "experimentID", config.ExperimentID, "Experiment ID")
	flags.IntVar(&config.BenchmarkID, "benchmarkID", config.BenchmarkID, "Benchmark ID")
	flags.StringVar(&config.Catalog, "catalog-refactor", config.Catalog, "Catalog")
	flags.IntVar(&config.Threads, "threads", config.Threads, "Threads")
	flags.IntVar(&config.Repeat, "repeat", config.Repeat, "Repeats")

	return &Command{
		Name:        "benchmark",
		Description: "Run the benchmark driver for the catalogs",
		Flags:       flags,
		Handler: func() error {
			// TODO: validate the flags
			benchmarkType := scenario.BenchmarkType(config.BenchmarkID)
			return runBenchmark(config.ExperimentID, benchmarkType, config.Catalog, config.Threads, config.Repeat)
		},
	}
}

func runBenchmark(experimentID string, benchmarkID scenario.BenchmarkType, catalogName string, threads int, repeat int) error {
	log.Printf("Starting experiment %s with benchmark scenario %d", experimentID, benchmarkID)

	context, err := common.GetRequestContextFromEnv(catalogName)
	if err != nil {
		return err
	}

	executionPlans, err := scenario.GetExecutionPlanFromBenchmarkID(catalogName, benchmarkID, context, threads, repeat)
	if err != nil {
		log.Printf("Error getting execution scenario: %s\n", err)
		return err
	}

	engine := execution.NewExecutionEngine(experimentID, executionPlans)
	startTime := time.Now()
	engine.Run()

	elapsedTime := time.Since(startTime)

	log.Printf("Finished in %f seconds experiment %s\n", elapsedTime.Seconds(), experimentID)

	return nil
}
