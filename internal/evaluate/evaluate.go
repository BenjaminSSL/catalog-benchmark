package evaluate

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/catalog/unity"
	"benchmark/internal/common"
	"context"
	"fmt"
	"log"

	"slices"

	"github.com/google/uuid"
)

type Evaluation struct {
	ExperimentID uuid.UUID `json:"experiment_id"`
	IsSuccess    bool      `json:"is_success"`
	Message      string    `json:"message"`
	Error        string    `json:"error"`
}

func Benchmark(ctx context.Context, experiment common.Experiment) (*Evaluation, error) {

	switch experiment.BenchmarkID {
	case common.CreateCatalogBenchmark:
		return createCatalogBenchmark(ctx, experiment)
	case common.UpdateGetCatalogBenchmark:
		return updateGetCatalogBenchmark(ctx, experiment)

	default:
		return nil, fmt.Errorf("unknown benchmark for catalog: %s", experiment.Catalog)
	}

}

func createCatalogBenchmark(ctx context.Context, experiment common.Experiment) (*Evaluation, error) {
	// Get the expected amount of catalogs
	expectedCatalogs := experiment.Threads * experiment.Repeat

	var ids []string
	// List all the catalogs to be abel to count them
	switch experiment.Catalog {
	case "polaris":
		catalogs, err := polaris.ListCatalogs(ctx)
		if err != nil {
			return nil, err
		}
		for _, catalog := range catalogs {
			ids = append(ids, catalog.Name)
		}
	case "unity":
		catalogs, err := unity.ListCatalogs(ctx, 1000)
		if err != nil {
			return nil, err
		}
		for _, catalog := range catalogs {
			ids = append(ids, catalog.Name)
		}
	}

	return &Evaluation{
		experiment.ID,
		len(ids) == expectedCatalogs,
		fmt.Sprintf("Expected %d catalogs, got %d", expectedCatalogs, len(ids)),
		"",
	}, nil

}

func updateGetCatalogBenchmark(ctx context.Context, experiment common.Experiment) (*Evaluation, error) {
	var isSuccess bool = true

	// Load the loglines
	logs, err := common.LoadLogs(experiment.ID.String())
	if err != nil {
		return nil, err
	}

	// for every thread remove first step
	for i := 0; i < len(logs); i++ {
		if logs[i].StepID == 0 {
			logs = slices.Delete(logs, i, i+1)
			continue
		}
	}
	entriesByThread := map[int][]common.LogEntry{}
	for _, log := range logs {
		entriesByThread[log.ThreadID] = append(entriesByThread[log.ThreadID], log)
	}

	for thread, entries := range entriesByThread {
		for i := 0; i < len(entries); i += 2 {
			if entries[i].Body != entries[i+1].Body {
				isSuccess = false
				log.Printf("Body mismatch: %s != %s %d %d", entries[i].Body, entries[i+1].Body, thread, i)
			}
		}

	}

	return &Evaluation{
		experiment.ID,
		isSuccess,
		"",
		"",
	}, nil
}
