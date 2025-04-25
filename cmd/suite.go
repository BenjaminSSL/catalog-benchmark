package cmd

import (
	"benchmark/internal/common"
	"flag"
	"github.com/google/uuid"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func init() {
	RegisterCommand(newSuiteCommand())
}

func newSuiteCommand() *Command {
	var catalog string

	flags := flag.NewFlagSet("suite", flag.ExitOnError)

	flags.StringVar(&catalog, "catalog", "polaris", "Catalog to use for the suite")

	return &Command{
		Name:        "suite",
		Description: "Run a suite of predefined benchmarks",
		Flags:       flags,
		Handler: func() error {

			return runSuite(catalog)
		},
	}
}

func runSuite(catalog string) error {
	entities := []common.EntityType{common.CatalogEntity, common.SchemaEntity, common.TableEntity}

	catalogEntities := map[string][]common.EntityType{
		"polaris": {common.ViewEntity, common.PrincipalEntity},
		"unity":   {common.FunctionEntity, common.ModelEntity, common.VolumeEntity},
	}

	if extraEntities, exists := catalogEntities[catalog]; exists {
		entities = append(entities, extraEntities...)
	}

	threads := []int{100}
	durations := []time.Duration{time.Millisecond * 100}
	benchmarks := []common.BenchmarkType{
		common.CreateBenchmark,
		common.CreateDeleteBenchmark,
		common.UpdateBenchmark,
		common.CreateDeleteListBenchmark,
		common.UpdateGetBenchmark,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	done := make(chan struct{})

	go func() {
		for _, entity := range entities {
			for _, thread := range threads {
				for _, duration := range durations {
					for _, benchmark := range benchmarks {
						experiment := common.Experiment{
							ID:          uuid.New(),
							Catalog:     catalog,
							BenchmarkID: benchmark,
							Threads:     thread,
							Entity:      entity,
							Duration:    duration,
						}

						log.Printf("Running benchmark: %d, Entity: %s, Threads: %d, Duration: %d seconds\n", benchmark, entity, thread, duration)
						if err := runBenchmark(experiment); err != nil {
							log.Printf("Error benchmark: %s\n", err)
						}
					}
				}
			}
		}
		close(done)
	}()

	select {
	case <-done:
		log.Println("All benchmarks completed successfully.")
	case <-quit:
		log.Println("Received interrupt signal, shutting down...")
	}
	return nil
}
