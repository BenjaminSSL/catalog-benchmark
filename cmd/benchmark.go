package cmd

import (
	"benchmark/internal/common"
	"benchmark/internal/execution"
	"benchmark/internal/factories"
	"benchmark/internal/plan"
	"flag"
	"github.com/google/uuid"
	"log"
	"os"
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
	}{
		// Default values
		ExperimentID: uuid.NewString(),
		BenchmarkID:  1,
		Catalog:      "polaris",
		Threads:      1,
	}

	flags.StringVar(&config.ExperimentID, "experimentID", config.ExperimentID, "Experiment ID")
	flags.IntVar(&config.BenchmarkID, "benchmarkID", config.BenchmarkID, "Benchmark ID")
	flags.StringVar(&config.Catalog, "catalog", config.Catalog, "Catalog")
	flags.IntVar(&config.Threads, "threads", config.Threads, "Threads")

	benchmarkType := plan.BenchmarkType(config.BenchmarkID)

	return &Command{
		Name:        "benchmark",
		Description: "Run the benchmark driver for the catalogs",
		Flags:       flags,
		Handler: func() error {
			// TODO: validate the flags
			return runBenchmark(config.ExperimentID, benchmarkType, config.Catalog, config.Threads)
		},
	}
}

func runBenchmark(experimentID string, benchmarkID plan.BenchmarkType, catalog string, threads int) error {
	log.Printf("Starting experiment %s\n", experimentID)

	var host string
	var factory factories.CatalogOperationFactory

	// Set up the catalog and their factory
	if catalog == "polaris" {
		host = os.Getenv("POLARIS_HOST")

		token, err := common.FetchPolarisToken(host)
		if err != nil {
			return err
		}

		factory = factories.NewPolarisFactory(host, token)
	} else if catalog == "unity" {
		host = os.Getenv("UNITY_HOST")
		factory = factories.NewUnityFactory(host)
	}

	builder := plan.NewBuilder(factory, threads)

	executionPlans, err := plan.GetExecutionPlanFromBenchmarkID(benchmarkID, builder)
	if err != nil {
		log.Printf("Error getting execution plan: %s\n", err)
		return err
	}

	engine := execution.NewExecutionEngine(experimentID, executionPlans)
	startTime := time.Now()
	engine.Run()

	elapsedTime := time.Since(startTime)

	log.Printf("Finished in %f seconds experiment %s\n", elapsedTime.Seconds(), experimentID)

	return nil
}
