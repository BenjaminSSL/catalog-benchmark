package evaluate

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/catalog/unity"
	"benchmark/internal/common"
	"context"
	"fmt"
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
