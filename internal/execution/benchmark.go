package execution

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/catalog/unity"
	"benchmark/internal/common"
	"github.com/google/uuid"
	"net/http"
)

type ExecutionPlanGenerator struct {
	context common.RequestContext
	threads int
	repeat  int
	catalog string
}

func NewExecutionPlanGenerator(context common.RequestContext, catalog string, threads int, repeat int) *ExecutionPlanGenerator {

	return &ExecutionPlanGenerator{
		context: context,
		catalog: catalog,
		threads: threads,
		repeat:  repeat,
	}
}

func (f *ExecutionPlanGenerator) CreateCatalog() (*Plan, error) {
	var req *http.Request
	var err error

	operations := make([][]*http.Request, f.threads)
	for thread := 0; thread < f.threads; thread++ {
		for i := 0; i < f.repeat; i++ {
			name := uuid.New().String()

			switch f.catalog {
			case "polaris":
				req, err = polaris.NewCreateCatalogRequest(f.context, name)
			case "unity":
				req, err = unity.NewCreateCatalogRequest(f.context, name)
			}
			if err != nil {
				return nil, err
			}
			operations[thread] = append(operations[thread], req)
		}
	}
	return &Plan{
		Execution: operations,
	}, nil
}

func (f *ExecutionPlanGenerator) CreateDeleteCatalog() (*Plan, error) {
	var createRequest *http.Request
	var deleteRequest *http.Request
	var err error

	operations := make([][]*http.Request, f.threads)
	for thread := 0; thread < f.threads; thread++ {
		for i := 0; i < f.repeat; i++ {

			name := uuid.New().String()

			switch f.catalog {
			case "polaris":
				createRequest, err = polaris.NewCreateCatalogRequest(f.context, name)
				deleteRequest, err = polaris.NewDeleteCatalogRequest(f.context, name)
			case "unity":
				createRequest, err = unity.NewCreateCatalogRequest(f.context, name)
				deleteRequest, err = unity.NewDeleteCatalogRequest(f.context, name)
			}
			if err != nil {
				return nil, err
			}
			operations[thread] = append(operations[thread], createRequest)
			operations[thread] = append(operations[thread], deleteRequest)

		}
	}

	return &Plan{Execution: operations}, nil
}

func (f *ExecutionPlanGenerator) UpdateCatalog() (*Plan, error) {
	var createRequest *http.Request
	var updateRequest *http.Request
	var err error

	setup := make([]*http.Request, 0)
	operations := make([][]*http.Request, f.threads)
	name := uuid.New().String()

	switch f.catalog {
	case "polaris":
		createRequest, err = polaris.NewCreateCatalogRequest(f.context, name)
	case "unity":
		createRequest, err = unity.NewCreateCatalogRequest(f.context, name)
	}
	if err != nil {
	}
	setup = append(setup, createRequest)
	for thread := 0; thread < f.threads; thread++ {
		entityVersion := 1
		for i := 0; i < f.repeat; i++ {
			switch f.catalog {
			case "polaris":
				updateRequest, err = polaris.NewUpdateCatalogRequest(f.context, name, entityVersion)
				entityVersion++
			case "unity":
				updateRequest, err = unity.NewUpdateCatalogRequest(f.context, name)
			}
			operations[thread] = append(operations[thread], updateRequest)

		}
	}

	return &Plan{
		Setup:     setup,
		Execution: operations,
	}, nil
}

func (f *ExecutionPlanGenerator) CreateAndListCatalog() (*Plan, error) {
	var listCatalogRequest *http.Request
	var createCatalogRequest *http.Request
	var err error

	operations := make([][]*http.Request, f.threads)
	checkpoints := f.repeat / 10
	for thread := 0; thread < f.threads; thread++ {
		for i := 0; i < f.repeat; i++ {
			if i%checkpoints == 0 {
				switch f.catalog {
				case "polaris":
					listCatalogRequest, err = polaris.NewListCatalogsRequest(f.context)
				case "unity":
					listCatalogRequest, err = unity.NewListCatalogsRequest(f.context, "", 100)
				}
				if err != nil {
					return nil, err
				}
				operations[thread] = append(operations[thread], listCatalogRequest)
			} else {
				switch f.catalog {
				case "polaris":
					createCatalogRequest, err = polaris.NewCreateCatalogRequest(f.context, uuid.New().String())
				case "unity":
					createCatalogRequest, err = unity.NewCreateCatalogRequest(f.context, uuid.New().String())
				}
				if err != nil {
					return nil, err
				}

				operations[thread] = append(operations[thread], createCatalogRequest)
			}
		}
	}

	return &Plan{
		Execution: operations,
	}, nil
}
