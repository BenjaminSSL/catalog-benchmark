package cmd

import (
	"benchmark/internal/catalog"
	"benchmark/internal/common"
	"benchmark/internal/execution"
	"benchmark/internal/requests"
	"benchmark/internal/scenario"
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

	return &Command{
		Name:        "benchmark",
		Description: "Run the benchmark driver for the catalogs",
		Flags:       flags,
		Handler: func() error {
			// TODO: validate the flags
			benchmarkType := scenario.BenchmarkType(config.BenchmarkID)
			return runBenchmark(config.ExperimentID, benchmarkType, config.Catalog, config.Threads)
		},
	}
}

func runBenchmark(experimentID string, benchmarkID scenario.BenchmarkType, catalogName string, threads int) error {
	log.Printf("Starting experiment %s with benchmark scenario %d", experimentID, benchmarkID)

	var host string
	var requestFactory requests.CatalogRequestFactory

	// Set up the catalogName and their factory
	if catalogName == "polaris" {
		host = os.Getenv("POLARIS_HOST")

		token, err := common.FetchPolarisToken(host)
		if err != nil {
			return err
		}

		log.Printf("Fetched the token from Polaris")

		requestFactory = requests.NewPolarisFactory(host, token)
	} else if catalogName == "unity" {
		host = os.Getenv("UNITY_HOST")
		requestFactory = requests.NewUnityFactory(host)
	}

	planFactory := scenario.NewExecutionPlanFactory(requestFactory, catalog.Catalog, threads, 100)
	executionPlans, err := scenario.GetExecutionPlanFromBenchmarkID(benchmarkID, planFactory)
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
