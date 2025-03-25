package execution

//
//import (
//	"benchmark/internal/common"
//	"context"
//	"errors"
//	"fmt"
//	"github.com/google/uuid"
//	"io"
//	"net/http"
//	"net/url"
//	"sync"
//	"time"
//)
//
//type Plan struct {
//	Setup     []*http.Request
//	Execution [][]*http.Request
//}
//type Engine struct {
//	ExperimentID uuid.UUID
//	Plan         *Plan
//	wg           sync.WaitGroup
//}
//
//func NewExecutionEngine(ExperimentID uuid.UUID, plan *Plan) *Engine {
//	return &Engine{
//		ExperimentID: ExperimentID,
//		Plan:         plan,
//	}
//}
//
//func (engine *Engine) Run(ctx context.Context) error {
//	// Initialize the environment by executing the setup requests
//	if err := setup(ctx, engine.Plan.Setup); err != nil {
//		return err
//	}
//
//	var httpClient = &http.Client{
//		Timeout: time.Second * 30,
//		Transport: &http.Transport{
//			MaxIdleConns:        10000,
//			MaxIdleConnsPerHost: 1000,
//			MaxConnsPerHost:     1000,
//			DisableKeepAlives:   false,
//			IdleConnTimeout:     90 * time.Second,
//			TLSHandshakeTimeout: 10 * time.Second,
//		},
//	}
//
//	totalTasks := 0
//	for _, executionPlan := range engine.Plan.Execution {
//		totalTasks += len(executionPlan)
//	}
//	progressBar := common.NewProgressBar(totalTasks)
//
//	for i := range engine.Plan.Execution {
//		engine.wg.Add(1)
//		go func(id int, executionPlan []*http.Request) {
//			logger, _ := common.NewRoutineBatchLogger("./output/logs/tmp", engine.ExperimentID, i, 100)
//
//			defer engine.wg.Done()
//			defer logger.Close()
//			for taskID, task := range executionPlan {
//				// Check if the context is cancelled
//				select {
//				case <-ctx.Done():
//					return
//				default:
//				}
//
//				progressBar.Add(1)
//				req := task.WithContext(ctx)
//				resp, err := httpClient.Do(req)
//				if err != nil {
//					switch {
//					case errors.Is(err, context.Canceled):
//						logger.Log("ERROR", taskID, 0, err.Error(), errors.New("Request timed out").Error())
//					case err.(*url.Error).Timeout():
//						logger.Log("ERROR", taskID, 0, err.Error(), errors.New("Connection timeout").Error())
//					default:
//						logger.Log("ERROR", taskID, 0, err.Error(), errors.New("Request failed").Error())
//					}
//					continue
//				}
//
//				statusCode := resp.StatusCode
//
//				body, err := io.ReadAll(resp.Body)
//
//				resp.Body.Close()
//				if err != nil {
//					logger.Log("ERROR", taskID, statusCode, "", errors.New("Failed to read response body").Error())
//					continue
//				}
//
//				if len(body) > 1000 {
//					body = body[:1000]
//				}
//
//				if statusCode >= 200 && statusCode <= 299 {
//					logger.Log("INFO", taskID, statusCode, string(body), req.Method)
//				} else {
//					logger.Log("ERROR", taskID, statusCode, string(body), errors.New(fmt.Sprintf("Step %d has failed", taskID)))
//				}
//
//				continue
//			}
//
//		}(i, engine.Plan.Execution[i])
//	}
//
//	engine.wg.Wait()
//	progressBar.Flush()
//	return ctx.Err()
//}
//
//func setup(ctx context.Context, requests []*http.Request) error {
//
//	for _, request := range requests {
//		select {
//		case <-ctx.Done():
//			return ctx.Err()
//		default:
//			resp, err := http.DefaultClient.Do(request)
//			if err != nil {
//				return err
//			}
//			resp.Body.Close()
//
//			if resp.StatusCode < 200 || resp.StatusCode > 299 {
//				return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
//			}
//		}
//	}
//
//	return nil
//}
