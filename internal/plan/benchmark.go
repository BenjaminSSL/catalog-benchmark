package plan

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/catalog/unity"
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
		factory := polaris.NewExecutionPlanGenerator(context, threads, repeat)

		switch benchmarkID {
		case CreateCatalogBenchmark:
			return factory.CreateCatalog()
		case CreateDeleteCatalogBenchmark:
			return factory.CreateDeleteCatalog()
		case UpdateCatalogBenchmark:
			return factory.UpdateCatalog()
		default:
			return nil, fmt.Errorf("unknown benchmark for catalog: %s", catalog)
		}
	case "unity":
		factory := unity.NewExecutionPlanGenerator(context, threads, repeat)
		switch benchmarkID {
		case CreateCatalogBenchmark:
			return factory.CreateCatalog()
		default:
			return nil, fmt.Errorf("unknown benchmark for catalog: %s", catalog)

		}
	}

	return nil, fmt.Errorf("unknown benchmark for catalog: %s", catalog)

}
