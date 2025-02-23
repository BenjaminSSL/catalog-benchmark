package polaris

import (
	"benchmark/internal/common"
	"benchmark/internal/execution"
	"github.com/google/uuid"
	"net/http"
)

type ExecutionPlanGenerator struct {
	context common.RequestContext
	threads int
	repeat  int
}

func NewExecutionPlanGenerator(context common.RequestContext, threads int, repeat int) *ExecutionPlanGenerator {

	return &ExecutionPlanGenerator{
		context: context,
		threads: threads,
		repeat:  repeat,
	}
}

func (f *ExecutionPlanGenerator) CreateCatalog() (execution.Plans, error) {
	operations := make([][]*http.Request, f.threads)
	for thread := 0; thread < f.threads; thread++ {
		for i := 0; i < f.repeat; i++ {
			name := uuid.New().String()
			req, err := NewCreateCatalogRequest(f.context, CreateCatalogParams{Name: name})

			if err != nil {
				return nil, err
			}
			operations[thread] = append(operations[thread], req)
		}
	}
	return operations, nil
}

func (f *ExecutionPlanGenerator) CreateDeleteCatalog() (execution.Plans, error) {
	operations := make([][]*http.Request, f.threads)
	for thread := 0; thread < f.threads; thread++ {
		for i := 0; i < f.repeat; i++ {
			name := uuid.New().String()

			createRequest, err := NewCreateCatalogRequest(f.context, CreateCatalogParams{Name: name})
			if err != nil {
				return nil, err
			}
			deleteRequest, err := NewDeleteCatalogRequest(f.context, DeleteCatalogParams{Name: name})
			if err != nil {
				return nil, err
			}
			operations[thread] = append(operations[thread], createRequest)
			operations[thread] = append(operations[thread], deleteRequest)

		}
	}

	return operations, nil
}

func (f *ExecutionPlanGenerator) UpdateCatalog() (execution.Plans, error) {
	operations := make([][]*http.Request, f.threads)
	for thread := 0; thread < f.threads; thread++ {
		name := uuid.New().String()
		createRequest, _ := NewCreateCatalogRequest(f.context, CreateCatalogParams{Name: name})
		operations[thread] = append(operations[thread], createRequest)
		entityVersion := 1
		for i := 0; i < f.repeat; i++ {
			updateRequest, _ := NewUpdateCatalogRequest(f.context, UpdateCatalogParams{
				Name:          name,
				EntityVersion: entityVersion,
			})
			operations[thread] = append(operations[thread], updateRequest)

			entityVersion++
		}
	}

	return operations, nil
}
