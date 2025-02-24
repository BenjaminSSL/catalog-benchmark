package evaluate

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/common"
	"fmt"
)

func BenchmarkExecution(context common.RequestContext, experiment common.Experiment) error {
	switch experiment.Catalog {
	case "polaris":

		switch experiment.BenchmarkID {
		case common.CreateCatalogBenchmark:
			return polaris.EvaluateCreateCatalogBenchmark(context, experiment)

		default:
			return fmt.Errorf("unknown benchmark for catalog: %s", experiment.Catalog)
		}
	case "unity":

		switch experiment.BenchmarkID {

		default:
			return fmt.Errorf("unknown benchmark for catalog: %s", experiment.Catalog)

		}
	}

	return fmt.Errorf("unknown benchmark for catalog: %s", experiment.Catalog)

}
