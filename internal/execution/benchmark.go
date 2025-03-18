package execution

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/catalog/unity"
	"context"
	"fmt"
	"github.com/google/uuid"
	"net/http"
	"strconv"
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
				updateRequest, err = polaris.NewUpdateCatalogRequest(ctx, name, entityVersion, nil)
				entityVersion++
			case "unity":
				updateRequest, err = unity.NewUpdateCatalogRequest(ctx, name, nil)
			}
			operations[thread] = append(operations[thread], updateRequest)

		}
	}

	return &Plan{
		Setup:     setup,
		Execution: operations,
	}, nil
}

func (f *PlanGenerator) CreateDeleteListCatalog(ctx context.Context) (*Plan, error) {
	var listCatalogRequest *http.Request
	var createCatalogRequest *http.Request
	var deleteCatalogRequest *http.Request
	var err error

	if f.threads < 2 {
		return nil, fmt.Errorf("threads must be greater than 1")
	}

	operations := make([][]*http.Request, f.threads)
	for thread := 0; thread < f.threads; thread++ {
		for i := 0; i < f.repeat; i++ {
			if thread == 0 {
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
				name := uuid.New().String()
				switch f.catalog {
				case "polaris":
					createCatalogRequest, err = polaris.NewCreateCatalogRequest(ctx, name)
					deleteCatalogRequest, err = polaris.NewDeleteCatalogRequest(ctx, name)
				case "unity":
					createCatalogRequest, err = unity.NewCreateCatalogRequest(ctx, name)
					deleteCatalogRequest, err = unity.NewDeleteCatalogRequest(ctx, name)
				}
				if err != nil {
					return nil, err
				}
				operations[thread] = append(operations[thread], createCatalogRequest)
				operations[thread] = append(operations[thread], deleteCatalogRequest)

			}
		}
	}

	return &Plan{
		Execution: operations,
	}, nil
}

func (f *PlanGenerator) UpdatePropertiesCatalog(ctx context.Context) (*Plan, error) {
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
		value := 0
		for i := 0; i < f.repeat; i++ {
			switch f.catalog {
			case "polaris":
				// The entity version will block the requests
				//propertyName := uuid.New().String()
				//updateRequest, err = polaris.NewUpdateCatalogRequest(ctx, name, f.threads, nil)
				break
			case "unity":
				properties := map[string]string{
					fmt.Sprintf("Thread %d", thread): strconv.Itoa(value),
				}
				updateRequest, err = unity.NewUpdateCatalogRequest(ctx, name, properties)

			}

			value++

			operations[thread] = append(operations[thread], updateRequest)
		}
	}

	return &Plan{
		Setup:     setup,
		Execution: operations,
	}, nil
}

func (f *PlanGenerator) UpdateGetCatalog(ctx context.Context) (*Plan, error) {
	var createRequest *http.Request
	var updateRequest *http.Request
	var getRequest *http.Request

	operations := make([][]*http.Request, f.threads)

	for thread := 0; thread < f.threads; thread++ {
		name := uuid.New().String()
		switch f.catalog {
		case "polaris":
			createRequest, _ = polaris.NewCreateCatalogRequest(ctx, name)
		case "unity":
			createRequest, _ = unity.NewCreateCatalogRequest(ctx, name)
		}

		entityVersion := 1
		properties := map[string]string{
			fmt.Sprintf("Thread %d", thread): strconv.Itoa(entityVersion),
		}

		operations[thread] = append(operations[thread], createRequest)
		for i := 0; i < f.repeat; i++ {
			switch f.catalog {
			case "polaris":
				updateRequest, _ = polaris.NewUpdateCatalogRequest(ctx, name, entityVersion, nil)
				getRequest, _ = polaris.NewGetCatalogRequest(ctx, name)
			case "unity":
				updateRequest, _ = unity.NewUpdateCatalogRequest(ctx, name, properties)
				getRequest, _ = unity.NewGetCatalogRequest(ctx, name)
			}

			operations[thread] = append(operations[thread], updateRequest)
			operations[thread] = append(operations[thread], getRequest)

			entityVersion++
			properties[fmt.Sprintf("Thread %d", thread)] = strconv.Itoa(entityVersion)
		}
	}

	return &Plan{
		Execution: operations,
	}, nil

}
