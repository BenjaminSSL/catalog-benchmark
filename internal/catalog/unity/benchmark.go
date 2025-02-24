package unity

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

func (f *ExecutionPlanGenerator) CreateCatalog() (*execution.Plan, error) {
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
	return &execution.Plan{
		Execution: operations,
	}, nil
}
