package internal

import (
	"benchmark/internal/common"
	"context"
	"errors"
	"github.com/google/uuid"
	"io"
	"net/http"
	"net/url"
)

type WorkerFunc func(ctx context.Context, w *Worker, params map[string]interface{})
type Worker struct {
	Func    WorkerFunc
	Client  *http.Client
	Logger  *common.RoutineBatchLogger
	Catalog Catalog
	Step    int
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

func (w *Worker) Run(ctx context.Context, params map[string]interface{}) {
	// EntityVersion counter for update operations
	entityVersion := 1
	params["entityVersion"] = entityVersion
	for ctx.Err() == nil {
		w.Func(ctx, w, params)
		w.Step++
		entityVersion++
		params["entityVersion"] = entityVersion
	}
}

func createCatalogWorker(ctx context.Context, w *Worker, _ map[string]interface{}) {
	catalogName := uuid.NewString()
	resp, err := w.Catalog.CreateCatalog(ctx, catalogName)
	w.Log(resp, err)
}

func createPrincipalWorker(ctx context.Context, w *Worker, _ map[string]interface{}) {
	catalogName := uuid.NewString()
	resp, err := w.Catalog.CreatePrincipal(ctx, catalogName)
	w.Log(resp, err)
}

func createSchemaWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := uuid.NewString()
	resp, err := w.Catalog.CreateSchema(ctx, catalogName, schemaName)
	w.Log(resp, err)
}

func createTableWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)

	tableName := uuid.NewString()
	resp, err := w.Catalog.CreateTable(ctx, catalogName, schemaName, tableName)
	w.Log(resp, err)
}

func createViewWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)

	viewName := uuid.NewString()
	resp, err := w.Catalog.CreateView(ctx, catalogName, schemaName, viewName)
	w.Log(resp, err)
}

func createFunctionWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)

	functionName := uuid.NewString()
	resp, err := w.Catalog.CreateFunction(ctx, catalogName, schemaName, functionName)
	w.Log(resp, err)
}

func createModelWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)

	modelName := uuid.NewString()
	resp, err := w.Catalog.CreateModel(ctx, catalogName, schemaName, modelName)
	w.Log(resp, err)
}

func createVolumeWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)

	volumeName := uuid.NewString()
	resp, err := w.Catalog.CreateVolume(ctx, catalogName, schemaName, volumeName)
	w.Log(resp, err)

}

func createDeleteCatalogWorker(ctx context.Context, w *Worker, _ map[string]interface{}) {

	catalogName := uuid.NewString()

	resp, err := w.Catalog.CreateCatalog(ctx, catalogName)
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.DeleteCatalog(ctx, catalogName)
	w.Log(resp, err)
}

func createDeletePrincipalWorker(ctx context.Context, w *Worker, _ map[string]interface{}) {
	principalName := uuid.NewString()
	resp, err := w.Catalog.CreatePrincipal(ctx, principalName)
	w.Log(resp, err)

	resp, err = w.Catalog.DeletePrincipal(ctx, principalName)
	w.Log(resp, err)
}

func createDeleteSchemaWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)

	schemaName := uuid.NewString()

	resp, err := w.Catalog.CreateSchema(ctx, catalogName, schemaName)
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.DeleteSchema(ctx, catalogName, schemaName)
	w.Log(resp, err)
}

func createDeleteTableWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)

	tableName := uuid.NewString()
	resp, err := w.Catalog.CreateTable(ctx, catalogName, schemaName, tableName)
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.DeleteTable(ctx, catalogName, schemaName, tableName)
	w.Log(resp, err)
}

func createDeleteViewWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)
	viewName := uuid.NewString()

	resp, err := w.Catalog.CreateView(ctx, catalogName, schemaName, viewName)
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.DeleteView(ctx, catalogName, schemaName, viewName)
	w.Log(resp, err)
}

func createDeleteFunctionWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)
	functionName := uuid.NewString()

	resp, err := w.Catalog.CreateFunction(ctx, catalogName, schemaName, functionName)
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.DeleteFunction(ctx, catalogName, schemaName, functionName)
	w.Log(resp, err)
}

func createDeleteModelWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)
	modelName := uuid.NewString()

	resp, err := w.Catalog.CreateModel(ctx, catalogName, schemaName, modelName)
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.DeleteModel(ctx, catalogName, schemaName, modelName)
	w.Log(resp, err)
}

func createDeleteVolumeWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)
	volumeName := uuid.NewString()

	resp, err := w.Catalog.CreateVolume(ctx, catalogName, schemaName, volumeName)
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.DeleteVolume(ctx, catalogName, schemaName, volumeName)
	w.Log(resp, err)
}

func updateCatalogWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	entityVersion := params["entityVersion"].(int)

	resp, err := w.Catalog.UpdateCatalog(ctx, catalogName, map[string]interface{}{
		"entityVersion": entityVersion,
	})
	w.Log(resp, err)

}

func updatePrincipalWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	principalName := params["principalName"].(string)
	entityVersion := params["entityVersion"].(int)

	resp, err := w.Catalog.UpdatePrincipal(ctx, principalName, map[string]interface{}{
		"entityVersion": entityVersion,
	})
	w.Log(resp, err)
}

func updateSchemaWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)
	entityVersion := params["entityVersion"].(int)

	resp, err := w.Catalog.UpdateSchema(ctx, catalogName, schemaName, map[string]interface{}{
		"entityVersion": entityVersion},
	)
	w.Log(resp, err)
}
func updateTableWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)
	tableName := params["tableName"].(string)
	entityVersion := params["entityVersion"].(int)

	resp, err := w.Catalog.UpdateTable(ctx, catalogName, schemaName, tableName, map[string]interface{}{
		"entityVersion": entityVersion},
	)
	w.Log(resp, err)
}

func updateViewWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)
	viewName := params["viewName"].(string)
	entityVersion := params["entityVersion"].(int)

	resp, err := w.Catalog.UpdateView(ctx, catalogName, schemaName, viewName, map[string]interface{}{
		"entityVersion": entityVersion},
	)
	w.Log(resp, err)
}

func updateModelWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)
	modelName := params["modelName"].(string)
	entityVersion := params["entityVersion"].(int)

	resp, err := w.Catalog.UpdateModel(ctx, catalogName, schemaName, modelName, map[string]interface{}{
		"entityVersion": entityVersion},
	)
	w.Log(resp, err)
}

func updateVolumeWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)
	volumeName := params["volumeName"].(string)
	entityVersion := params["entityVersion"].(int)

	resp, err := w.Catalog.UpdateVolume(ctx, catalogName, schemaName, volumeName, map[string]interface{}{
		"entityVersion": entityVersion},
	)
	w.Log(resp, err)
}

func updateGetCatalogWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	entityVersion := params["entityVersion"].(int)

	resp, err := w.Catalog.UpdateCatalog(ctx, catalogName, map[string]interface{}{
		"entityVersion": entityVersion,
	})
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.GetCatalog(ctx, catalogName)
	w.Log(resp, err)
}

func updateGetPrincipalWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	principalName := params["principalName"].(string)
	entityVersion := params["entityVersion"].(int)

	resp, err := w.Catalog.UpdatePrincipal(ctx, principalName, map[string]interface{}{
		"entityVersion": entityVersion,
	})
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.GetPrincipal(ctx, principalName)
	w.Log(resp, err)
}

func updateGetSchemaWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)
	entityVersion := params["entityVersion"].(int)

	resp, err := w.Catalog.UpdateSchema(ctx, catalogName, schemaName, map[string]interface{}{
		"entityVersion": entityVersion,
	})
	w.Log(resp, err)

	w.IncrementStep()

	resp, err = w.Catalog.GetSchema(ctx, catalogName, schemaName)
	w.Log(resp, err)
}

func updateGetTableWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)
	tableName := params["tableName"].(string)
	entityVersion := params["entityVersion"].(int)
	resp, err := w.Catalog.UpdateTable(ctx, catalogName, schemaName, tableName, map[string]interface{}{
		"entityVersion": entityVersion,
	})
	w.Log(resp, err)

	entityVersion++

	resp, err = w.Catalog.GetTable(ctx, catalogName, schemaName, tableName)
	w.Log(resp, err)
}

func updateGetViewWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)
	viewName := params["viewName"].(string)
	entityVersion := params["entityVersion"].(int)
	resp, err := w.Catalog.UpdateView(ctx, catalogName, schemaName, viewName, map[string]interface{}{
		"entityVersion": entityVersion,
	})
	w.Log(resp, err)

	entityVersion++

	resp, err = w.Catalog.GetView(ctx, catalogName, schemaName, viewName)
	w.Log(resp, err)
}

func updateGetModelWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)
	modelName := params["modelName"].(string)
	entityVersion := params["entityVersion"].(int)
	resp, err := w.Catalog.UpdateModel(ctx, catalogName, schemaName, modelName, map[string]interface{}{
		"entityVersion": entityVersion,
	})
	w.Log(resp, err)

	entityVersion++

	resp, err = w.Catalog.GetModel(ctx, catalogName, schemaName, modelName)
	w.Log(resp, err)
}

func updateGetVolumeWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)
	volumeName := params["volumeName"].(string)
	entityVersion := params["entityVersion"].(int)

	resp, err := w.Catalog.UpdateVolume(ctx, catalogName, schemaName, volumeName, map[string]interface{}{
		"entityVersion": entityVersion,
	})
	w.Log(resp, err)

	entityVersion++

	resp, err = w.Catalog.GetVolume(ctx, catalogName, schemaName, volumeName)
	w.Log(resp, err)
}

func listCatalogsWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	responses, err := w.Catalog.ListCatalogs(ctx, params)
	if len(responses) == 0 || err != nil {
		w.Log(nil, err)
		return
	}
	resp := responses[len(responses)-1]
	w.Log(resp, err)
}

func listPrincipalsWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	responses, err := w.Catalog.ListPrincipals(ctx, params)
	if len(responses) == 0 || err != nil {
		w.Log(nil, err)
		return
	}
	resp := responses[len(responses)-1]
	w.Log(resp, err)
}

func listSchemasWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	responses, err := w.Catalog.ListSchemas(ctx, catalogName, params)
	if len(responses) == 0 || err != nil {
		w.Log(nil, err)
		return
	}
	resp := responses[len(responses)-1]
	w.Log(resp, err)
}

func listTablesWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)
	responses, err := w.Catalog.ListTables(ctx, catalogName, schemaName, params)
	if len(responses) == 0 || err != nil {
		w.Log(nil, err)
		return
	}
	resp := responses[len(responses)-1]
	w.Log(resp, err)
}

func listViewsWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)

	responses, err := w.Catalog.ListViews(ctx, catalogName, schemaName, params)
	if len(responses) == 0 || err != nil {
		w.Log(nil, err)
		return
	}
	resp := responses[len(responses)-1]
	w.Log(resp, err)
}

func listFunctionsWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)

	responses, err := w.Catalog.ListFunctions(ctx, catalogName, schemaName, params)
	if len(responses) == 0 || err != nil {
		w.Log(nil, err)
		return
	}
	resp := responses[len(responses)-1]
	w.Log(resp, err)
}
func listModelsWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)

	responses, err := w.Catalog.ListModels(ctx, catalogName, schemaName, params)
	if len(responses) == 0 || err != nil {
		w.Log(nil, err)
		return
	}
	resp := responses[len(responses)-1]
	w.Log(resp, err)
}

func listVolumesWorker(ctx context.Context, w *Worker, params map[string]interface{}) {
	catalogName := params["catalogName"].(string)
	schemaName := params["schemaName"].(string)

	responses, err := w.Catalog.ListVolumes(ctx, catalogName, schemaName, params)
	if len(responses) == 0 || err != nil {
		w.Log(nil, err)
		return
	}
	resp := responses[len(responses)-1]
	w.Log(resp, err)
}
