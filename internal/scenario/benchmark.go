package scenario

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/common"
	"benchmark/internal/execution"
	"fmt"
	"net/http"
)

type ExecutionPlanFactoryOptions struct {
	Threads int
	Repeat  int
}

type BenchmarkType int

const (
	CreateCatalogBenchmark BenchmarkType = iota + 1
	CreateDeleteCatalogBenchmark
	UpdateCatalogBenchmark
)

func GetExecutionPlanFromBenchmarkID(catalog string, benchmarkID BenchmarkType, context common.RequestContext, executionPlanFactoryOptions ExecutionPlanFactoryOptions) ([]execution.Plan, error) {
	switch catalog {
	case "polaris":
		factory := polaris.NewExecutionPlanFactory(context, executionPlanFactoryOptions)

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

}

func BuildPlans(operations [][]*http.Request) []execution.Plan {
	var plans = make([]execution.Plan, 0)
	for _, operation := range operations {

		plans = append(plans, execution.Plan{Steps: operation})
	}
	return plans
}
