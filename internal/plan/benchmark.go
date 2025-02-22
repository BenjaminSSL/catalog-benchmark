package plan

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/common"
	"fmt"
	"net/http"
)

type BenchmarkType int

const (
	CreateCatalogBenchmark BenchmarkType = iota + 1
	CreateDeleteCatalogBenchmark
	UpdateCatalogBenchmark
)

func GetExecutionPlanFromBenchmarkID(catalog string, benchmarkID BenchmarkType, context common.RequestContext, threads int, repeat int) ([][]*http.Request, error) {
	switch catalog {
	case "polaris":
		factory := polaris.NewExecutionPlanFactory(context, threads, repeat)

		switch benchmarkID {
		case CreateCatalogBenchmark:
			return factory.CreateCatalog()
		case CreateDeleteCatalogBenchmark:
			return factory.CreateDeleteCatalog()
		case UpdateCatalogBenchmark:
			return factory.UpdateCatalog()
		default:
			return nil, fmt.Errorf("unknown BenchmarkType: %v", benchmarkID)
		}
	case "unity":
		switch benchmarkID {
		}
	}

	return nil, fmt.Errorf("unknown BenchmarkType: %v", benchmarkID)

}
