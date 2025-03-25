package execution

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/catalog/unity"
	"benchmark/internal/common"
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type BenchmarkEngine struct {
	ExperimentID string
	threads      int
	duration     time.Duration
	wg           sync.WaitGroup
	catalog      string
}

func NewBenchmarkEngine(experimentID string, catalog string, threads int, duration time.Duration) *BenchmarkEngine {
	return &BenchmarkEngine{
		ExperimentID: experimentID,
		catalog:      catalog,
		threads:      threads,
		duration:     duration,
	}
}

func getHttpClient() *http.Client {
	return &http.Client{
		Timeout: time.Second * 30,
		Transport: &http.Transport{
			MaxIdleConns:        10000,
			MaxIdleConnsPerHost: 1000,
			MaxConnsPerHost:     1000,
			DisableKeepAlives:   false,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}
}

func getLogger(experimentID string, thread int) *common.RoutineBatchLogger {
	logger, _ := common.NewRoutineBatchLogger("./output/logs/tmp", experimentID, thread, 20)

	return logger

}

func (e *BenchmarkEngine) RunWorkers(ctx context.Context, configs []WorkerConfig) error {
	ctx, cancel := context.WithTimeout(ctx, e.duration)
	defer cancel()

	client := getHttpClient()

	for _, config := range configs {
		for thread := 0; thread < config.Threads; thread++ {
			e.wg.Add(1)
			go func(thread int, workerFunc func(ctx context.Context, client *http.Client, logger *common.RoutineBatchLogger)) {
				logger, _ := common.NewRoutineBatchLogger("./output/logs/tmp", e.ExperimentID, thread, 20)
				defer logger.Close()
				defer e.wg.Done()
				workerFunc(ctx, client, logger)
			}(thread, config.Func)
		}
	}

	e.wg.Wait()

	return nil
}
func (e *BenchmarkEngine) CreateCatalogWorker(ctx context.Context, client *http.Client, logger *common.RoutineBatchLogger) {
	for step := 1; ; step++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		name := uuid.New().String()
		resp, err := createCatalogRequest(e.catalog, client, name)
		if err != nil {
			handleRequestError(err, logger, step)
			continue
		}

		handleResponse(resp, logger, step)
	}
}

func (e *BenchmarkEngine) createCatalogRequest(catalog string, client *http.Client, name string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewCreateCatalogRequest(ctx, name))
	case "unity":
		return client.Do(unity.NewCreateCatalogRequest(ctx, name))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}
func handleRequestError(err error, logger *common.RoutineBatchLogger, step int) {
	switch {
	case errors.Is(err, context.Canceled):
		logger.Log("ERROR", step, 0, err.Error(), errors.New("Request timed out").Error())
	case err.(*url.Error).Timeout():
		logger.Log("ERROR", step, 0, err.Error(), errors.New("Connection timeout").Error())
	default:
		logger.Log("ERROR", step, 0, err.Error(), errors.New("Request failed").Error())
	}
}

func handleResponse(resp *http.Response, logger *common.RoutineBatchLogger, step int) {
	statusCode := resp.StatusCode

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()

	if err != nil {
		logger.Log("ERROR", step, statusCode, "", errors.New("Failed to read response body").Error())
		return
	}

	if len(body) > 1000 {
		body = body[:1000]
	}

	if statusCode >= 200 && statusCode <= 299 {
		logger.Log("INFO", step, statusCode, string(body), "")
	} else {
		logger.Log("ERROR", step, statusCode, string(body), errors.New(fmt.Sprintf("Step %d has failed", step)))
	}
}

func (e *BenchmarkEngine) CreateDeleteCatalogWorker(ctx context.Context, client *http.Client, logger *common.RoutineBatchLogger) {
	for step := 1; ; step++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		name := uuid.New().String()

		resp, err := createCatalogRequest(e.catalog, client, name)
		if err != nil {
			handleRequestError(err, logger, step)
		} else {
			handleResponse(resp, logger, step)
		}

		resp, err = deleteCatalogRequest(e.catalog, client, name)
		if err != nil {
			handleRequestError(err, logger, step)
		} else {
			handleResponse(resp, logger, step)

		}

	}
}

func (e *BenchmarkEngine) deleteCatalogRequest(catalog string, client *http.Client, name string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewDeleteCatalogRequest(ctx, name))
	case "unity":
		return client.Do(unity.NewDeleteCatalogRequest(ctx, name))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

//
//func (f *PlanGenerator) CreateDeleteCatalog(ctx context.Context) (*Plan, error) {
//	var createRequest *http.Request
//	var deleteRequest *http.Request
//	var err error
//
//	operations := make([][]*http.Request, f.threads)
//	for thread := 0; thread < f.threads; thread++ {
//		for i := 0; i < f.repeat; i++ {
//
//			name := uuid.New().String()
//
//			switch f.catalog {
//			case "polaris":
//				createRequest, err = polaris.NewCreateCatalogRequest(ctx, name)
//				deleteRequest, err = polaris.NewDeleteCatalogRequest(ctx, name)
//			case "unity":
//				createRequest, err = unity.NewCreateCatalogRequest(ctx, name)
//				deleteRequest, err = unity.NewDeleteCatalogRequest(ctx, name)
//			}
//			if err != nil {
//				return nil, err
//			}
//			operations[thread] = append(operations[thread], createRequest)
//			operations[thread] = append(operations[thread], deleteRequest)
//
//		}
//	}
//
//	return &Plan{Execution: operations}, nil
//}
//
//func (f *PlanGenerator) CreateUpdateCatalog(ctx context.Context) (*Plan, error) {
//	var createRequest *http.Request
//	var updateRequest *http.Request
//	var err error
//
//	setup := make([]*http.Request, 0)
//	operations := make([][]*http.Request, f.threads)
//	name := uuid.New().String()
//
//	switch f.catalog {
//	case "polaris":
//		createRequest, err = polaris.NewCreateCatalogRequest(ctx, name)
//	case "unity":
//		createRequest, err = unity.NewCreateCatalogRequest(ctx, name)
//	}
//	if err != nil {
//	}
//	setup = append(setup, createRequest)
//	for thread := 0; thread < f.threads; thread++ {
//		entityVersion := 1
//		for i := 0; i < f.repeat; i++ {
//			switch f.catalog {
//			case "polaris":
//				updateRequest, err = polaris.NewUpdateCatalogRequest(ctx, name, entityVersion, nil)
//				entityVersion++
//			case "unity":
//				updateRequest, err = unity.NewUpdateCatalogRequest(ctx, name, nil)
//			}
//			operations[thread] = append(operations[thread], updateRequest)
//
//		}
//	}
//
//	return &Plan{
//		Setup:     setup,
//		Execution: operations,
//	}, nil
//}
//
//func (f *PlanGenerator) CreateDeleteListCatalog(ctx context.Context) (*Plan, error) {
//	var listCatalogRequest *http.Request
//	var createCatalogRequest *http.Request
//	var deleteCatalogRequest *http.Request
//	var err error
//
//	if f.threads < 2 {
//		return nil, fmt.Errorf("threads must be greater than 1")
//	}
//
//	operations := make([][]*http.Request, f.threads)
//	for thread := 0; thread < f.threads; thread++ {
//		for i := 0; i < f.repeat; i++ {
//			if thread == 0 {
//				switch f.catalog {
//				case "polaris":
//					listCatalogRequest, err = polaris.NewListCatalogsRequest(ctx)
//				case "unity":
//					listCatalogRequest, err = unity.NewListCatalogsRequest(ctx, "", 100)
//				}
//				if err != nil {
//					return nil, err
//				}
//				operations[thread] = append(operations[thread], listCatalogRequest)
//			} else {
//				name := uuid.New().String()
//				switch f.catalog {
//				case "polaris":
//					createCatalogRequest, err = polaris.NewCreateCatalogRequest(ctx, name)
//					deleteCatalogRequest, err = polaris.NewDeleteCatalogRequest(ctx, name)
//				case "unity":
//					createCatalogRequest, err = unity.NewCreateCatalogRequest(ctx, name)
//					deleteCatalogRequest, err = unity.NewDeleteCatalogRequest(ctx, name)
//				}
//				if err != nil {
//					return nil, err
//				}
//				operations[thread] = append(operations[thread], createCatalogRequest)
//				operations[thread] = append(operations[thread], deleteCatalogRequest)
//
//			}
//		}
//	}
//
//	return &Plan{
//		Execution: operations,
//	}, nil
//}
//
//func (f *PlanGenerator) UpdatePropertiesCatalog(ctx context.Context) (*Plan, error) {
//	var createRequest *http.Request
//	var updateRequest *http.Request
//	var err error
//
//	setup := make([]*http.Request, 0)
//	operations := make([][]*http.Request, f.threads)
//	name := uuid.New().String()
//
//	switch f.catalog {
//	case "polaris":
//		createRequest, err = polaris.NewCreateCatalogRequest(ctx, name)
//	case "unity":
//		createRequest, err = unity.NewCreateCatalogRequest(ctx, name)
//	}
//	if err != nil {
//	}
//	setup = append(setup, createRequest)
//	for thread := 0; thread < f.threads; thread++ {
//		value := 0
//		for i := 0; i < f.repeat; i++ {
//			switch f.catalog {
//			case "polaris":
//				// The entity version will block the requests
//				//propertyName := uuid.New().String()
//				//updateRequest, err = polaris.NewUpdateCatalogRequest(ctx, name, f.threads, nil)
//				break
//			case "unity":
//				properties := map[string]string{
//					fmt.Sprintf("Thread %d", thread): strconv.Itoa(value),
//				}
//				updateRequest, err = unity.NewUpdateCatalogRequest(ctx, name, properties)
//
//			}
//
//			value++
//
//			operations[thread] = append(operations[thread], updateRequest)
//		}
//	}
//
//	return &Plan{
//		Setup:     setup,
//		Execution: operations,
//	}, nil
//}
//
//func (f *PlanGenerator) UpdateGetCatalog(ctx context.Context) (*Plan, error) {
//	var createRequest *http.Request
//	var updateRequest *http.Request
//	var getRequest *http.Request
//
//	operations := make([][]*http.Request, f.threads)
//
//	for thread := 0; thread < f.threads; thread++ {
//		name := uuid.New().String()
//		switch f.catalog {
//		case "polaris":
//			createRequest, _ = polaris.NewCreateCatalogRequest(ctx, name)
//		case "unity":
//			createRequest, _ = unity.NewCreateCatalogRequest(ctx, name)
//		}
//
//		entityVersion := 1
//		properties := map[string]string{
//			fmt.Sprintf("Thread %d", thread): strconv.Itoa(entityVersion),
//		}
//
//		operations[thread] = append(operations[thread], createRequest)
//		for i := 0; i < f.repeat; i++ {
//			switch f.catalog {
//			case "polaris":
//				updateRequest, _ = polaris.NewUpdateCatalogRequest(ctx, name, entityVersion, nil)
//				getRequest, _ = polaris.NewGetCatalogRequest(ctx, name)
//			case "unity":
//				updateRequest, _ = unity.NewUpdateCatalogRequest(ctx, name, properties)
//				getRequest, _ = unity.NewGetCatalogRequest(ctx, name)
//			}
//
//			operations[thread] = append(operations[thread], updateRequest)
//			operations[thread] = append(operations[thread], getRequest)
//
//			entityVersion++
//			properties[fmt.Sprintf("Thread %d", thread)] = strconv.Itoa(entityVersion)
//		}
//	}
//
//	return &Plan{
//		Execution: operations,
//	}, nil
//
//}
