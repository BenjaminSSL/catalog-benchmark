package execution

import (
	"benchmark/internal/common"
	"fmt"
	"io"
	"log"
	"net/http"
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
	for _, setup := range engine.Plan.Setup {
		resp, err := http.DefaultClient.Do(setup)
		if err != nil {
			return err
		}

		resp.Body.Close()
	}
	for i := range engine.Plan.Execution {
		engine.wg.Add(1)
		go func(id int, executionPlan []*http.Request) {
			logger, _ := common.NewRoutineBatchLogger("./logs/tmp", engine.ExperimentID, i, 100)
			client := &http.Client{Timeout: time.Second * 30}

			defer engine.wg.Done()
			defer logger.Close()

			for taskID, task := range executionPlan {
				logger.Log("INFO", fmt.Sprintf("Starting step %d", taskID), nil)

				resp, err := client.Do(task)
				if err != nil {
					logger.Log("ERROR", fmt.Sprintf("failed to execute step"), err)
					continue
				}

				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					log.Printf("failed to read response body %d: %v", taskID, err)
					continue
				}
				statusCode := resp.StatusCode

				logData := map[string]interface{}{
					"task_id":       taskID,
					"status_code":   statusCode,
					"response_body": string(body),
				}

				if statusCode >= 200 && statusCode <= 299 {
					logger.Log("INFO", fmt.Sprintf("Finished step %d", taskID), logData)
				} else {
					logger.Log("ERROR", fmt.Sprintf("Step %d has failed", taskID), logData)
				}

			}

		}(i, engine.Plan.Execution[i])
	}

	engine.wg.Wait()
	return nil
}
