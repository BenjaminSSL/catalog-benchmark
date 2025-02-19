package execution

import (
	"benchmark/internal/common"
	"fmt"
	"log"
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
			logger, err := common.NewRoutineBatchLogger("./logs", engine.ExperimentID, i, 100)
			if err != nil {
				log.Fatal(err)
			}
			defer engine.wg.Done()
			defer logger.Close()

			log.Printf("Starting execution plan %d\n", id)
			for taskID, task := range executionPlan.Steps {
				logger.Log("INFO", fmt.Sprintf("Starting step %d", taskID), nil)
				log.Printf("Starting task %d", taskID)
				statusCode := task.Execute()
				if statusCode != 0 {
					logger.Log("INFO", fmt.Sprintf("Finished step %d", taskID), nil)
				} else {
					logger.Log("ERROR", fmt.Sprintf("Step %d has failed", taskID), nil)
				}
				log.Printf("Finished step %d", taskID)

			}

			log.Printf("Execution plan %d has completed\n", id)
		}(i, &engine.Plans[i])
	}

	engine.wg.Wait()
	log.Println("All execution plans have completed")
}
