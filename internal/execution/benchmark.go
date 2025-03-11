package execution

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/catalog/unity"
	"context"
	"github.com/google/uuid"
	"net/http"
)

type PlanGenerator struct {
	threads int
	repeat  int
	catalog string
}

func NewExecutionPlanGenerator(catalog string, threads int, repeat int) *PlanGenerator {

	return &PlanGenerator{
		catalog: catalog,
		threads: threads,
		repeat:  repeat,
	}
}

func (f *PlanGenerator) CreateCatalog(ctx context.Context) (*Plan, error) {
	var req *http.Request
	var err error

	operations := make([][]*http.Request, f.threads)
	for thread := 0; thread < f.threads; thread++ {
		for i := 0; i < f.repeat; i++ {
			name := uuid.New().String()

			switch f.catalog {
			case "polaris":
				req, err = polaris.NewCreateCatalogRequest(ctx, name)
			case "unity":
				req, err = unity.NewCreateCatalogRequest(ctx, name)
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

func (f *PlanGenerator) CreateDeleteCatalog(ctx context.Context) (*Plan, error) {
	var createRequest *http.Request
	var deleteRequest *http.Request
	var err error

	operations := make([][]*http.Request, f.threads)
	for thread := 0; thread < f.threads; thread++ {
		for i := 0; i < f.repeat; i++ {

			name := uuid.New().String()

			switch f.catalog {
			case "polaris":
				createRequest, err = polaris.NewCreateCatalogRequest(ctx, name)
				deleteRequest, err = polaris.NewDeleteCatalogRequest(ctx, name)
			case "unity":
				createRequest, err = unity.NewCreateCatalogRequest(ctx, name)
				deleteRequest, err = unity.NewDeleteCatalogRequest(ctx, name)
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

func (f *PlanGenerator) CreateUpdateCatalog(ctx context.Context) (*Plan, error) {
	var createRequest *http.Request
	var updateRequest *http.Request
	var err error

	setup := make([]*http.Request, 0)
	operations := make([][]*http.Request, f.threads)
	name := uuid.New().String()

	switch f.catalog {
	case "polaris":
		createRequest, err = polaris.NewCreateCatalogRequest(ctx, name)
	case "unity":
		createRequest, err = unity.NewCreateCatalogRequest(ctx, name)
	}
	if err != nil {
	}
	setup = append(setup, createRequest)
	for thread := 0; thread < f.threads; thread++ {
		entityVersion := 1
		for i := 0; i < f.repeat; i++ {
			switch f.catalog {
			case "polaris":
				updateRequest, err = polaris.NewUpdateCatalogRequest(ctx, name, entityVersion)
				entityVersion++
			case "unity":
				updateRequest, err = unity.NewUpdateCatalogRequest(ctx, name)
			}
			operations[thread] = append(operations[thread], updateRequest)

		}
	}

	return &Plan{
		Setup:     setup,
		Execution: operations,
	}, nil
}

func (f *PlanGenerator) CreateListCatalog(ctx context.Context) (*Plan, error) {
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
					listCatalogRequest, err = polaris.NewListCatalogsRequest(ctx)
				case "unity":
					listCatalogRequest, err = unity.NewListCatalogsRequest(ctx, "", 100)
				}
				if err != nil {
					return nil, err
				}
				operations[thread] = append(operations[thread], listCatalogRequest)
			} else {
				switch f.catalog {
				case "polaris":
					createCatalogRequest, err = polaris.NewCreateCatalogRequest(ctx, uuid.New().String())
				case "unity":
					createCatalogRequest, err = unity.NewCreateCatalogRequest(ctx, uuid.New().String())
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
