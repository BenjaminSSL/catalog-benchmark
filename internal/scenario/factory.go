package scenario

import (
	"benchmark/internal/catalog"
	"benchmark/internal/execution"
	"benchmark/internal/requests"
	"fmt"
	"github.com/google/uuid"
	"net/http"
)

type BenchmarkType int

const (
	CreateBenchmark BenchmarkType = iota + 1
	CreateDeleteBenchmark
	UpdateBenchmark
)

func GetExecutionPlanFromBenchmarkID(benchmarkID BenchmarkType, factory *ExecutionPlanFactory) ([]execution.Plan, error) {
	switch benchmarkID {
	case CreateBenchmark:
		return factory.Create()
	case CreateDeleteBenchmark:
		return factory.CreateDelete()
	default:
		return nil, fmt.Errorf("unknown BenchmarkType: %v", benchmarkID)
	}

}

type ExecutionPlanFactory struct {
	factory    requests.CatalogRequestFactory
	entityType catalog.EntityType
	repeat     int
	threads    int
}

type Builder struct {
	factory    requests.CatalogRequestFactory
	operations [][]*http.Request
	threads    int
}

func NewExecutionPlanFactory(factory requests.CatalogRequestFactory, entityType catalog.EntityType, threads int, repeat int) *ExecutionPlanFactory {
	return &ExecutionPlanFactory{
		factory:    factory,
		entityType: entityType,
		threads:    threads,
		repeat:     repeat,
	}

}

func (f *ExecutionPlanFactory) Create() ([]execution.Plan, error) {
	operations := make([][]*http.Request, f.threads)
	for thread := 0; thread < f.threads; thread++ {
		for i := 0; i < f.repeat; i++ {
			name := uuid.New().String()
			var req *http.Request
			var err error
			switch f.entityType {
			case catalog.Catalog:
				req, err = f.factory.CreateCatalogRequest(requests.CreateCatalogParams{Name: name})

			}

			if err != nil {
				return nil, err
			}
			operations[thread] = append(operations[thread], req)
		}
	}
	return buildPlans(operations), nil
}

func (f *ExecutionPlanFactory) CreateDelete() ([]execution.Plan, error) {
	operations := make([][]*http.Request, f.threads)
	for thread := 0; thread < f.threads; thread++ {
		for i := 0; i < f.repeat; i++ {
			name := uuid.New().String()

			createRequest, err := f.factory.CreateCatalogRequest(requests.CreateCatalogParams{Name: name})
			if err != nil {
				return nil, err
			}
			deleteRequest, err := f.factory.DeleteCatalogRequest(requests.DeleteCatalogParams{Name: name})
			if err != nil {
				return nil, err
			}
			operations[thread] = append(operations[thread], createRequest)
			operations[thread] = append(operations[thread], deleteRequest)

		}
	}

	return buildPlans(operations), nil
}

func buildPlans(operations [][]*http.Request) []execution.Plan {
	var plans = make([]execution.Plan, 0)
	for _, operation := range operations {

		plans = append(plans, execution.Plan{Steps: operation})
	}
	return plans
}
