package execution

import (
	"benchmark/internal/common"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
)

type Plan struct {
	Setup     []*http.Request
	Execution [][]*http.Request
}
type Engine struct {
	ExperimentID string
	Plan         *Plan
	wg           sync.WaitGroup
}

func NewExecutionEngine(ExperimentID string, plan *Plan) *Engine {
	return &Engine{
		ExperimentID: ExperimentID,
		Plan:         plan,
	}
}

func (engine *Engine) Run() error {
	// Initialize the environment by executing the setup requests
	if err := setup(engine.Plan.Setup); err != nil {
		return err
	}

	var httpClient = &http.Client{
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	totalTasks := 0
	for _, executionPlan := range engine.Plan.Execution {
		totalTasks += len(executionPlan)
	}
	progressBar := common.NewProgressBar(totalTasks)

	for i := range engine.Plan.Execution {
		engine.wg.Add(1)
		go func(id int, executionPlan []*http.Request) {
			logger, _ := common.NewRoutineBatchLogger("./logs/tmp", engine.ExperimentID, i, 100)

			defer engine.wg.Done()
			defer logger.Close()
			for taskID, task := range executionPlan {

				req := task.WithContext(ctx)
				resp, err := httpClient.Do(req)
				if err != nil {
					switch {
					case errors.Is(err, context.Canceled):
						logger.Log("ERROR", taskID, "Request timed out", err)
					case err.(*url.Error).Timeout():
						logger.Log("ERROR", taskID, "Connection timeout", err)
					default:
						logger.Log("ERROR", taskID, "Request failed", err)
					}
					continue
				}

				body, err := io.ReadAll(resp.Body)

				resp.Body.Close()
				if err != nil {
					logger.Log("ERROR", taskID, "Failed to read response body", err)
					continue
				}

				statusCode := resp.StatusCode

				logData := map[string]interface{}{
					"task_id":       taskID,
					"status_code":   statusCode,
					"response_body": string(body),
				}

				if statusCode >= 200 && statusCode <= 299 {
					logger.Log("INFO", taskID, fmt.Sprintf("Finished step %d", taskID), logData)
				} else {
					logger.Log("ERROR", taskID, fmt.Sprintf("Step %d has failed", taskID), logData)
				}

				progressBar.Add(1)

				continue
			}

		}(i, engine.Plan.Execution[i])
	}

	engine.wg.Wait()
	return nil
}

func setup(requests []*http.Request) error {

	for _, request := range requests {
		resp, err := http.DefaultClient.Do(request)
		if err != nil {
			return err
		}
		resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
	}

	return nil
}
