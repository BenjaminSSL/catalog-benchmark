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
	if catalog == "polaris" {
		entities = append(entities, common.ViewEntity, common.PrincipalEntity)
	} else if catalog == "unity" {
		entities = append(entities, common.FunctionEntity, common.ModelEntity, common.VolumeEntity)
	}

	threads := []int{2, 5, 25, 50, 100}
	duration := []int{1, 2, 5}
	benchmarks := []common.BenchmarkType{
		common.CreateBenchmark,
		common.CreateDeleteBenchmark,
		common.CreateUpdateBenchmark,
		common.CreateDeleteListBenchmark,
		common.UpdateGetBenchmark,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	done := make(chan struct{})
	go func() {
		for _, entity := range entities {
			for _, thread := range threads {
				for _, duration := range duration {
					for _, benchmark := range benchmarks {
						experiment := common.Experiment{
							ID:          uuid.New(),
							Catalog:     catalog,
							BenchmarkID: benchmark,
							Threads:     thread,
							Entity:      entity,
							Duration:    time.Duration(duration) * time.Second,
						}
						log.Printf("Running benchmark: %s, Entity: %s, Threads: %d, Duration: %d seconds\n", benchmark, entity, thread, duration)
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
