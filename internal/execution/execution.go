package execution

import (
	"benchmark/internal/common"
	"fmt"
	"sync"
)

type Plan struct {
	Steps []Request
}

type Engine struct {
	ExperimentID string
	Plans        []Plan
	wg           sync.WaitGroup
}

func NewExecutionEngine(ExperimentID string, plans []Plan) *Engine {
	return &Engine{
		ExperimentID: ExperimentID,
		Plans:        plans,
	}
}

func (engine *Engine) Run() {
	for i := range engine.Plans {
		engine.wg.Add(1)
		go func(id int, executionPlan *Plan) {
			logger, _ := common.NewRoutineBatchLogger("./logs", engine.ExperimentID, i, 100)

			defer engine.wg.Done()
			defer logger.Close()

			for taskID, task := range executionPlan.Steps {
				logger.Log("INFO", fmt.Sprintf("Starting step %d", taskID), nil)

				statusCode := task.Execute()
				if statusCode != 0 {
					logger.Log("INFO", fmt.Sprintf("Finished step %d", taskID), nil)
				} else {
					logger.Log("ERROR", fmt.Sprintf("Step %d has failed", taskID), nil)
				}

			}

		}(i, &engine.Plans[i])
	}

	engine.wg.Wait()

}
