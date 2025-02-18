package execution

import (
	"log"
	"sync"
)

type Plan struct {
	Steps []Request
	wg    sync.WaitGroup
}

type Engine struct {
	Plans []Plan
	wg    sync.WaitGroup
}

func NewExecutionEngine(plans []Plan) *Engine {
	return &Engine{
		Plans: plans,
	}
}

func (engine *Engine) Run() {
	for i := range engine.Plans {
		engine.wg.Add(1)
		go func(id int, executionPlan *Plan) {
			log.Printf("Starting execution plan %d\n", id)
			defer engine.wg.Done()

			for i, task := range executionPlan.Steps {
				stausCode := task.Execute()
				log.Printf("Completed step %d in execution plan %d with status: %d", i, id, stausCode)

			}

			executionPlan.wg.Wait()
			log.Printf("Execution plan %d has completed\n", id)
		}(i, &engine.Plans[i])
	}

	engine.wg.Wait()
	log.Println("All execution plans have completed")
}
