package plan

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/catalog/unity"
	"benchmark/internal/common"
	"fmt"
	"net/http"
)

func GenerateExecutionPlan(context common.RequestContext, experiment common.Experiment) ([][]*http.Request, error) {
	switch experiment.Catalog {
	case "polaris":
		generator := polaris.NewExecutionPlanGenerator(context, experiment.Threads, experiment.Repeat)

		switch experiment.BenchmarkID {
		case common.CreateCatalogBenchmark:
			return generator.CreateCatalog()
		case common.CreateDeleteCatalogBenchmark:
			return generator.CreateDeleteCatalog()
		case common.UpdateCatalogBenchmark:
			return generator.UpdateCatalog()
		default:
			return nil, fmt.Errorf("unknown benchmark %v for catalog: %s", experiment.BenchmarkID, experiment.Catalog)
		}
	case "unity":
		generator := unity.NewExecutionPlanGenerator(context, experiment.Threads, experiment.Repeat)
		switch experiment.BenchmarkID {
		case common.CreateCatalogBenchmark:
			return generator.CreateCatalog()
		default:
			return nil, fmt.Errorf("unknown benchmark %v for catalog: %s", experiment.BenchmarkID, experiment.Catalog)

		}
	}

	return nil, fmt.Errorf("unknown benchmark %v for catalog: %s", experiment.BenchmarkID, experiment.Catalog)

}
