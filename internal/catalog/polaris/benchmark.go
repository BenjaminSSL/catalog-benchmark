package polaris

import (
	"benchmark/internal/common"
	"benchmark/internal/execution"
	"github.com/google/uuid"
	"net/http"
)

type ExecutionPlanFactory struct {
	factory *CatalogRequestFactory
	threads int
	repeat  int
}

func NewExecutionPlanFactory(context common.RequestContext, threads int, repeat int) *ExecutionPlanFactory {

	return &ExecutionPlanFactory{
		factory: NewCatalogRequestFactory(context.Host, context.Token),
		threads: threads,
		repeat:  repeat,
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
	return BuildPlans(operations), nil
}

func (f *ExecutionPlanFactory) CreateDeleteCatalog() ([]execution.Plan, error) {
	operations := make([][]*http.Request, f.threads)
	for thread := 0; thread < f.threads; thread++ {
		for i := 0; i < f.repeat; i++ {
			name := uuid.New().String()

			createRequest, err := f.factory.CreateCatalogRequest(CreateCatalogParams{Name: name})
			if err != nil {
				return nil, err
			}
			deleteRequest, err := f.factory.DeleteCatalogRequest(DeleteCatalogParams{Name: name})
			if err != nil {
				return nil, err
			}
			operations[thread] = append(operations[thread], createRequest)
			operations[thread] = append(operations[thread], deleteRequest)

		}
	}

	return BuildPlans(operations), nil
}

func (f *ExecutionPlanFactory) UpdateCatalog() ([]execution.Plan, error) {
	operations := make([][]*http.Request, f.threads)
	for thread := 0; thread < f.threads; thread++ {
		name := uuid.New().String()
		createRequest, _ := f.factory.CreateCatalogRequest(CreateCatalogParams{Name: name})
		operations[thread] = append(operations[thread], createRequest)
		entityVersion := 1
		for i := 0; i < f.repeat; i++ {
			updateRequest, _ := f.factory.UpdateCatalogRequest(UpdateCatalogParams{
				Name:    name,
				Version: entityVersion,
			})
			operations[thread] = append(operations[thread], updateRequest)

			entityVersion++
		}
	}

	return BuildPlans(operations), nil
}
func BuildPlans(operations [][]*http.Request) []execution.Plan {
	var plans = make([]execution.Plan, 0)
	for _, operation := range operations {

		plans = append(plans, execution.Plan{Steps: operation})
	}
	return plans
}
