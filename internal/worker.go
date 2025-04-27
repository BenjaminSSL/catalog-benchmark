package internal

import (
	"benchmark/internal/common"
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"net/http"
	"net/url"
)

type WorkerFunc func(w *Worker)
type Worker struct {
	Ctx     context.Context
	Func    WorkerFunc
	Catalog Catalog
	Client  *http.Client
	Logger  *common.RoutineBatchLogger
	Step    int
	Params  map[string]interface{}
}

func NewWorker(client *http.Client, catalog Catalog, logger *common.RoutineBatchLogger, params map[string]interface{}, workerFunc WorkerFunc) *Worker {
	// Ensures the Params map is not modified outside of the worker
	paramsCopy := make(map[string]interface{})
	for k, v := range params {
		paramsCopy[k] = v
	}
	return &Worker{
		Client:  client,
		Catalog: catalog,
		Logger:  logger,
		Step:    0,
		Params:  paramsCopy,
		Ctx:     context.Background(),
		Func:    workerFunc,
	}
}

func (w *Worker) Log(resp *http.Response, err error) {
	method := "NONE"
	if err != nil {
		switch {
		case errors.Is(err, context.Canceled):
			w.Logger.Log("ERROR", method, w.Step, 0, err.Error())
		case err.(*url.Error).Timeout():
			w.Logger.Log("ERROR", method, w.Step, 0, err.Error())
		default:
			w.Logger.Log("ERROR", method, w.Step, 0, err.Error())
		}

		return
	}
	statusCode := resp.StatusCode
	method = resp.Request.Method

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)

	if err != nil {
		w.Logger.Log("ERROR", method, w.Step, statusCode, err.Error())
		return
	}

	level := "ERROR"
	if statusCode >= 200 && statusCode <= 299 {
		level = "INFO"
	}

	w.Logger.Log(level, method, w.Step, statusCode, string(body))
}

func (w *Worker) IncrementStep() {
	w.Step++
}

func (w *Worker) Run(ctx context.Context) {
	w.Ctx = ctx
	// EntityVersion counter for update operations
	entityVersion := 1
	w.Params["entityVersion"] = entityVersion
	for ctx.Err() == nil {
		w.Func(w)
		w.Step++
		entityVersion++
		w.Params["entityVersion"] = entityVersion
	}
}

func CreateCatalogWorker(w *Worker) {
	catalogName := uuid.NewString()
	resp, err := w.Catalog.CreateCatalog(w.Ctx, catalogName)
	w.Log(resp, err)
}

func CreatePrincipalWorker(w *Worker) {
	catalogName := uuid.NewString()
	resp, err := w.Catalog.CreatePrincipal(w.Ctx, catalogName)
	w.Log(resp, err)
}

func CreateSchemaWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := uuid.NewString()
	resp, err := w.Catalog.CreateSchema(w.Ctx, catalogName, schemaName)
	w.Log(resp, err)
}

func CreateTableWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)

	tableName := uuid.NewString()
	resp, err := w.Catalog.CreateTable(w.Ctx, catalogName, schemaName, tableName)
	w.Log(resp, err)
}

func CreateViewWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)

	viewName := uuid.NewString()
	resp, err := w.Catalog.CreateView(w.Ctx, catalogName, schemaName, viewName)
	w.Log(resp, err)
}

func CreateFunctionWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)

	functionName := uuid.NewString()
	resp, err := w.Catalog.CreateFunction(w.Ctx, catalogName, schemaName, functionName)
	w.Log(resp, err)
}

func CreateModelWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)

	modelName := uuid.NewString()
	resp, err := w.Catalog.CreateModel(w.Ctx, catalogName, schemaName, modelName)
	w.Log(resp, err)
}

func CreateVolumeWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)

	volumeName := uuid.NewString()
	resp, err := w.Catalog.CreateVolume(w.Ctx, catalogName, schemaName, volumeName)
	w.Log(resp, err)

}

func CreateDeleteCatalogWorker(w *Worker) {

	catalogName := uuid.NewString()

	resp, err := w.Catalog.CreateCatalog(w.Ctx, catalogName)
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.DeleteCatalog(w.Ctx, catalogName)
	w.Log(resp, err)
}

func CreateDeletePrincipalWorker(w *Worker) {
	principalName := uuid.NewString()
	resp, err := w.Catalog.CreatePrincipal(w.Ctx, principalName)
	w.Log(resp, err)

	resp, err = w.Catalog.DeletePrincipal(w.Ctx, principalName)
	w.Log(resp, err)
}

func CreateDeleteSchemaWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)

	schemaName := uuid.NewString()

	resp, err := w.Catalog.CreateSchema(w.Ctx, catalogName, schemaName)
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.DeleteSchema(w.Ctx, catalogName, schemaName)
	w.Log(resp, err)
}

func CreateDeleteTableWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)

	tableName := uuid.NewString()
	resp, err := w.Catalog.CreateTable(w.Ctx, catalogName, schemaName, tableName)
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.DeleteTable(w.Ctx, catalogName, schemaName, tableName)
	w.Log(resp, err)
}

func CreateDeleteViewWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)
	viewName := uuid.NewString()

	resp, err := w.Catalog.CreateView(w.Ctx, catalogName, schemaName, viewName)
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.DeleteView(w.Ctx, catalogName, schemaName, viewName)
	w.Log(resp, err)
}

func CreateDeleteFunctionWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)
	functionName := uuid.NewString()

	resp, err := w.Catalog.CreateFunction(w.Ctx, catalogName, schemaName, functionName)
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.DeleteFunction(w.Ctx, catalogName, schemaName, functionName)
	w.Log(resp, err)
}

func CreateDeleteModelWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)
	modelName := uuid.NewString()

	resp, err := w.Catalog.CreateModel(w.Ctx, catalogName, schemaName, modelName)
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.DeleteModel(w.Ctx, catalogName, schemaName, modelName)
	w.Log(resp, err)
}

func CreateDeleteVolumeWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)
	volumeName := uuid.NewString()

	resp, err := w.Catalog.CreateVolume(w.Ctx, catalogName, schemaName, volumeName)
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.DeleteVolume(w.Ctx, catalogName, schemaName, volumeName)
	w.Log(resp, err)
}

func UpdateCatalogWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	entityVersion := w.Params["entityVersion"].(int)

	resp, err := w.Catalog.UpdateCatalog(w.Ctx, catalogName, map[string]interface{}{
		"entityVersion": entityVersion,
	})
	w.Log(resp, err)

}

func UpdatePrincipalWorker(w *Worker) {
	principalName := w.Params["principalName"].(string)
	entityVersion := w.Params["entityVersion"].(int)

	resp, err := w.Catalog.UpdatePrincipal(w.Ctx, principalName, map[string]interface{}{
		"entityVersion": entityVersion,
	})
	w.Log(resp, err)
}

func UpdateSchemaWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)
	entityVersion := w.Params["entityVersion"].(int)

	fmt.Printf("schemaname : %s\n", schemaName)

	resp, err := w.Catalog.UpdateSchema(w.Ctx, catalogName, schemaName, map[string]interface{}{
		"entityVersion": entityVersion},
	)
	w.Log(resp, err)
}
func UpdateTableWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)
	tableName := w.Params["tableName"].(string)
	entityVersion := w.Params["entityVersion"].(int)

	resp, err := w.Catalog.UpdateTable(w.Ctx, catalogName, schemaName, tableName, map[string]interface{}{
		"entityVersion": entityVersion},
	)
	w.Log(resp, err)
}

func UpdateViewWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)
	viewName := w.Params["viewName"].(string)
	entityVersion := w.Params["entityVersion"].(int)

	resp, err := w.Catalog.UpdateView(w.Ctx, catalogName, schemaName, viewName, map[string]interface{}{
		"entityVersion": entityVersion},
	)
	w.Log(resp, err)
}

func UpdateModelWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)
	modelName := w.Params["modelName"].(string)
	entityVersion := w.Params["entityVersion"].(int)

	resp, err := w.Catalog.UpdateModel(w.Ctx, catalogName, schemaName, modelName, map[string]interface{}{
		"entityVersion": entityVersion},
	)
	w.Log(resp, err)
}

func UpdateVolumeWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)
	volumeName := w.Params["volumeName"].(string)
	entityVersion := w.Params["entityVersion"].(int)

	resp, err := w.Catalog.UpdateVolume(w.Ctx, catalogName, schemaName, volumeName, map[string]interface{}{
		"entityVersion": entityVersion},
	)
	w.Log(resp, err)
}

func UpdateGetCatalogWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	entityVersion := w.Params["entityVersion"].(int)

	resp, err := w.Catalog.UpdateCatalog(w.Ctx, catalogName, map[string]interface{}{
		"entityVersion": entityVersion,
	})
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.GetCatalog(w.Ctx, catalogName)
	w.Log(resp, err)
}

func UpdateGetPrincipalWorker(w *Worker) {
	principalName := w.Params["principalName"].(string)
	entityVersion := w.Params["entityVersion"].(int)

	resp, err := w.Catalog.UpdatePrincipal(w.Ctx, principalName, map[string]interface{}{
		"entityVersion": entityVersion,
	})
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.GetPrincipal(w.Ctx, principalName)
	w.Log(resp, err)
}

func UpdateGetSchemaWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)
	entityVersion := w.Params["entityVersion"].(int)

	resp, err := w.Catalog.UpdateSchema(w.Ctx, catalogName, schemaName, map[string]interface{}{
		"entityVersion": entityVersion,
	})
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.GetSchema(w.Ctx, catalogName, schemaName)
	w.Log(resp, err)
}

func UpdateGetTableWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)
	tableName := w.Params["tableName"].(string)
	entityVersion := w.Params["entityVersion"].(int)
	resp, err := w.Catalog.UpdateTable(w.Ctx, catalogName, schemaName, tableName, map[string]interface{}{
		"entityVersion": entityVersion,
	})
	w.Log(resp, err)

	entityVersion++

	resp, err = w.Catalog.GetTable(w.Ctx, catalogName, schemaName, tableName)
	w.Log(resp, err)
}

func UpdateGetViewWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)
	viewName := w.Params["viewName"].(string)
	entityVersion := w.Params["entityVersion"].(int)
	resp, err := w.Catalog.UpdateView(w.Ctx, catalogName, schemaName, viewName, map[string]interface{}{
		"entityVersion": entityVersion,
	})
	w.Log(resp, err)

	entityVersion++

	resp, err = w.Catalog.GetView(w.Ctx, catalogName, schemaName, viewName)
	w.Log(resp, err)
}

func UpdateGetModelWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)
	modelName := w.Params["modelName"].(string)
	entityVersion := w.Params["entityVersion"].(int)
	resp, err := w.Catalog.UpdateModel(w.Ctx, catalogName, schemaName, modelName, map[string]interface{}{
		"entityVersion": entityVersion,
	})
	w.Log(resp, err)

	entityVersion++

	resp, err = w.Catalog.GetModel(w.Ctx, catalogName, schemaName, modelName)
	w.Log(resp, err)
}

func UpdateGetVolumeWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)
	volumeName := w.Params["volumeName"].(string)
	entityVersion := w.Params["entityVersion"].(int)

	resp, err := w.Catalog.UpdateVolume(w.Ctx, catalogName, schemaName, volumeName, map[string]interface{}{
		"entityVersion": entityVersion,
	})
	w.Log(resp, err)

	entityVersion++

	resp, err = w.Catalog.GetVolume(w.Ctx, catalogName, schemaName, volumeName)
	w.Log(resp, err)
}

func ListCatalogsWorker(w *Worker) {
	responses, err := w.Catalog.ListCatalogs(w.Ctx, w.Params)
	if len(responses) == 0 || err != nil {
		w.Log(nil, err)
		return
	}
	resp := responses[len(responses)-1]
	w.Log(resp, err)
}

func ListPrincipalsWorker(w *Worker) {
	responses, err := w.Catalog.ListPrincipals(w.Ctx, w.Params)
	if len(responses) == 0 || err != nil {
		w.Log(nil, err)
		return
	}
	resp := responses[len(responses)-1]
	w.Log(resp, err)
}

func ListSchemasWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	responses, err := w.Catalog.ListSchemas(w.Ctx, catalogName, w.Params)
	if len(responses) == 0 || err != nil {
		w.Log(nil, err)
		return
	}
	resp := responses[len(responses)-1]
	w.Log(resp, err)
}

func ListTablesWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)
	responses, err := w.Catalog.ListTables(w.Ctx, catalogName, schemaName, w.Params)
	if len(responses) == 0 || err != nil {
		w.Log(nil, err)
		return
	}
	resp := responses[len(responses)-1]
	w.Log(resp, err)
}

func ListViewsWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)

	responses, err := w.Catalog.ListViews(w.Ctx, catalogName, schemaName, w.Params)
	if len(responses) == 0 || err != nil {
		w.Log(nil, err)
		return
	}
	resp := responses[len(responses)-1]
	w.Log(resp, err)
}

func ListFunctionsWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)

	responses, err := w.Catalog.ListFunctions(w.Ctx, catalogName, schemaName, w.Params)
	if len(responses) == 0 || err != nil {
		w.Log(nil, err)
		return
	}
	resp := responses[len(responses)-1]
	w.Log(resp, err)
}
func ListModelsWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)

	responses, err := w.Catalog.ListModels(w.Ctx, catalogName, schemaName, w.Params)
	if len(responses) == 0 || err != nil {
		w.Log(nil, err)
		return
	}
	resp := responses[len(responses)-1]
	w.Log(resp, err)
}

func ListVolumesWorker(w *Worker) {
	catalogName := w.Params["catalogName"].(string)
	schemaName := w.Params["schemaName"].(string)

	responses, err := w.Catalog.ListVolumes(w.Ctx, catalogName, schemaName, w.Params)
	if len(responses) == 0 || err != nil {
		w.Log(nil, err)
		return
	}
	resp := responses[len(responses)-1]
	w.Log(resp, err)
}
