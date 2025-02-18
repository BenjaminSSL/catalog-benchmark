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
			return runBenchmark(config.ExperimentID, config.BenchmarkID, config.Catalog, config.Threads)
		},
	}
}

func runBenchmark(experimentID string, benchmarkID int, catalog string, threads int) error {
	log.Printf("Starting experiment %s\n", experimentID)

	var host string
	var factory factories.CatalogOperationFactory

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

	executionPlan := plan.NewBuilder(factory, threads).CreateDelete(100).BuildExecutionPlan()
	engine := execution.NewExecutionEngine(executionPlan)
	engine.Run()

	return nil
}
