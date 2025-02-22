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

type Plans [][]*http.Request

type Engine struct {
	ExperimentID string
	Plans        Plans
	wg           sync.WaitGroup
}

func NewExecutionEngine(ExperimentID string, plans Plans) *Engine {
	return &Engine{
		ExperimentID: ExperimentID,
		Plans:        plans,
	}
}

func (engine *Engine) Run() {
	for i := range engine.Plans {
		engine.wg.Add(1)
		go func(id int, executionPlan []*http.Request) {
			logger, _ := common.NewRoutineBatchLogger("./logs", engine.ExperimentID, i, 100)
			client := &http.Client{Timeout: time.Second * 30}

			defer engine.wg.Done()
			defer logger.Close()

			for taskID, task := range executionPlan {
				logger.Log("INFO", fmt.Sprintf("Starting step %d", taskID), nil)

				resp, err := client.Do(task)
				if err != nil {
					log.Printf("failed to execute step %d: %v", taskID, err)
					continue
				}

				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil {
					log.Printf("failed to read response body %d: %v", taskID, err)
					continue
				}

				logData := map[string]interface{}{
					"task_id":       taskID,
					"status_code":   resp.StatusCode,
					"response_body": string(body),
				}

				statusCode := resp.StatusCode
				if statusCode != 0 {
					logger.Log("INFO", fmt.Sprintf("Finished step %d", taskID), logData)
				} else {
					logger.Log("ERROR", fmt.Sprintf("Step %d has failed", taskID), logData)
				}

			}

		}(i, engine.Plans[i])
	}

	engine.wg.Wait()

}
