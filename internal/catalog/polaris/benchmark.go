package polaris

import (
	"benchmark/internal/common"
	"benchmark/internal/execution"

	"benchmark/internal/scenario"
	"github.com/google/uuid"
	"net/http"
)

type ExecutionPlanFactory struct {
	factory *requests.PolarisFactory
	threads int
	repeat  int
}

func NewExecutionPlanFactory(context common.RequestContext, options scenario.ExecutionPlanFactoryOptions) *ExecutionPlanFactory {

	return &ExecutionPlanFactory{
		factory: requests.NewPolarisFactory(context.Host, context.Token),
		threads: options.Threads,
		repeat:  options.Repeat,
	}
}

func (f *ExecutionPlanFactory) CreateCatalog() ([]execution.Plan, error) {
	operations := make([][]*http.Request, f.threads)
	for thread := 0; thread < f.threads; thread++ {
		for i := 0; i < f.repeat; i++ {
			name := uuid.New().String()
			req, err := f.factory.CreateCatalogRequest(CreateCatalogParams{Name: name})

			if err != nil {
				return nil, err
			}
			operations[thread] = append(operations[thread], req)
		}
	}
	return scenario.BuildPlans(operations), nil
}

func (f *ExecutionPlanFactory) CreateDeleteCatalog() ([]execution.Plan, error) {
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

	return scenario.BuildPlans(operations), nil
}

func (f *ExecutionPlanFactory) UpdateCatalog() ([]execution.Plan, error) {
	operations := make([][]*http.Request, f.threads)
	for thread := 0; thread < f.threads; thread++ {
		name := uuid.New().String()
		createRequest, _ := f.factory.CreateCatalogRequest(requests.CreateCatalogParams{Name: name})
		operations[thread] = append(operations[thread], createRequest)
		entityVersion := 1
		for i := 0; i < f.repeat; i++ {
			updateRequest, _ := f.factory.UpdateCatalogRequest(requests.UpdateCatalogParams{
				Name:    name,
				Version: entityVersion,
			})
			operations[thread] = append(operations[thread], updateRequest)

			entityVersion++
		}
	}

	return scenario.BuildPlans(operations), nil
}
