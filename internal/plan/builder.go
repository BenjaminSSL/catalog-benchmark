package plan

import (
	"benchmark/internal/execution"
	"benchmark/internal/factories"
	"github.com/google/uuid"
)

type Builder struct {
	factory    factories.CatalogOperationFactory
	operations [][]execution.Request
	threads    int
}

func NewBuilder(factory factories.CatalogOperationFactory, threads int) *Builder {
	return &Builder{
		factory:    factory,
		operations: make([][]execution.Request, threads),
		threads:    threads}
}

func (b *Builder) Create(times int) *Builder {
	for thread := 0; thread < b.threads; thread++ {
		for i := 0; i < times; i++ {
			name := uuid.New().String()
			request := b.factory.CreateCatalogRequest(name)
			b.operations[thread] = append(b.operations[thread], request)
		}
	}
	return b
}

func (b *Builder) CreateDelete(times int) *Builder {
	for thread := 0; thread < b.threads; thread++ {
		for i := 0; i < times; i++ {
			name := uuid.New().String()
			createRequest := b.factory.CreateCatalogRequest(name)
			deleteRequest := b.factory.DeleteCatalogRequest(name)
			b.operations[thread] = append(b.operations[thread], createRequest)
			b.operations[thread] = append(b.operations[thread], deleteRequest)

		}
	}

	return b
}

func (b *Builder) BuildExecutionPlan() []execution.Plan {
	var plans = make([]execution.Plan, 0)

	for _, operations := range b.operations {

		plans = append(plans, execution.Plan{Steps: operations})
	}

	return plans
}
