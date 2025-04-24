package internal

import (
	"context"
	"github.com/google/uuid"
)

func grantPermissionCatalog(ctx context.Context, catalog Catalog, catalogName string) error {
	_, err := catalog.GrantPermissionCatalog(ctx, catalogName, map[string]interface{}{
		"privilege": "TABLE_WRITE_DATA",
	})
	if err != nil {
		return err
	}

	_, err = catalog.GrantPermissionCatalog(ctx, catalogName, map[string]interface{}{
		"privilege": "TABLE_READ_DATA",
	})
	if err != nil {
		return err
	}

	return nil
}

func createCatalog(ctx context.Context, catalog Catalog) (string, error) {
	catalogName := uuid.NewString()

	_, err := catalog.CreateCatalog(ctx, catalogName)
	if err != nil {
		return "", err
	}

	return catalogName, nil
}

func createSchema(ctx context.Context, catalog Catalog, catalogName string) (string, error) {
	schemaName := uuid.NewString()

	_, err := catalog.CreateSchema(ctx, catalogName, schemaName)
	if err != nil {
		return "", err
	}

	return schemaName, nil
}

func SetupCreateCatalog(threads int) ([]WorkerConfig, error) {
	return []WorkerConfig{
		{createCatalogWorker, threads, make(map[string]interface{})},
	}, nil
}

func SetupCreatePrincipal(threads int) ([]WorkerConfig, error) {

	return []WorkerConfig{
		{createPrincipalWorker, threads, make(map[string]interface{})},
	}, nil
}

func SetupCreateSchema(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	return []WorkerConfig{
		{createSchemaWorker, threads, map[string]interface{}{"catalogName": catalogName}},
	}, nil
}

func SetupCreateTable(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	err = grantPermissionCatalog(ctx, catalog, catalogName)

	return []WorkerConfig{
		{createTableWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func SetupCreateView(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []WorkerConfig{
		{createViewWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func SetupCreateFunction(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []WorkerConfig{
		{createFunctionWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}
func SetupCreateModel(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []WorkerConfig{
		{createModelWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}
func SetupCreateVolume(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []WorkerConfig{
		{createVolumeWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func SetupCreateDeleteCatalog(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	return []WorkerConfig{
		{createDeleteCatalogWorker, threads, make(map[string]interface{})},
	}, nil
}

func SetupCreateDeleteSchema(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	return []WorkerConfig{
		{createDeleteSchemaWorker, threads, map[string]interface{}{
			"catalogName": catalogName}},
	}, nil
}

func SetupCreateDeletePrincipal(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	return []WorkerConfig{
		{createDeletePrincipalWorker, threads, make(map[string]interface{})},
	}, nil
}

func SetupCreateDeleteTable(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	err = grantPermissionCatalog(ctx, catalog, catalogName)

	return []WorkerConfig{
		{createDeleteTableWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func SetupCreateDeleteView(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []WorkerConfig{
		{createDeleteViewWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func SetupCreateDeleteFunction(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []WorkerConfig{
		{createFunctionWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func SetupCreateDeleteModel(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []WorkerConfig{
		{createDeleteModelWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}
func SetupCreateDeleteVolume(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []WorkerConfig{
		{createDeleteVolumeWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func SetupUpdateCatalog(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	return []WorkerConfig{
		{updateCatalogWorker, threads, map[string]interface{}{
			"catalogName": catalogName}},
	}, nil
}

func SetupUpdatePrincipal(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	principalName := uuid.NewString()

	_, err := catalog.CreatePrincipal(ctx, principalName)
	if err != nil {
		return nil, err
	}
	return []WorkerConfig{
		{updatePrincipalWorker, threads, map[string]interface{}{
			"principalName": principalName}},
	}, nil
}

func SetupUpdateSchema(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []WorkerConfig{
		{updateSchemaWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func SetupUpdateTable(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	tableName := uuid.NewString()

	_, err = catalog.CreatePrincipal(ctx, tableName)
	if err != nil {
		return nil, err
	}

	err = grantPermissionCatalog(ctx, catalog, catalogName)
	return []WorkerConfig{
		{updateTableWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName, "tableName": tableName}},
	}, nil

}

func SetupUpdateView(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	viewName := uuid.NewString()
	_, err = catalog.CreateView(ctx, catalogName, schemaName, viewName)
	if err != nil {
		return nil, err
	}

	return []WorkerConfig{
		{updateViewWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName, "viewName": viewName}},
	}, nil
}

func SetupUpdateModel(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}
	modelName := uuid.NewString()

	_, err = catalog.CreateModel(ctx, catalogName, schemaName, modelName)
	if err != nil {
		return nil, err
	}
	return []WorkerConfig{
		{updateModelWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName, "modelName": modelName}},
	}, nil
}
func SetupUpdateVolume(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}
	volumeName := uuid.NewString()
	_, err = catalog.CreateVolume(ctx, catalogName, schemaName, volumeName)
	if err != nil {
		return nil, err
	}

	return []WorkerConfig{
		{updateVolumeWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName, "volumeName": volumeName}},
	}, nil
}

func SetupCreateDeleteListSchema(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	return []WorkerConfig{
		{listSchemasWorker, 1, map[string]interface{}{"catalogName": catalogName}},
		{createDeleteSchemaWorker, threads - 1, map[string]interface{}{"catalogName": catalogName}},
	}, nil
}

func SetupCreateDeleteListCatalog(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	return []WorkerConfig{
		{listCatalogsWorker, 1, make(map[string]interface{})},
		{createDeleteCatalogWorker, threads - 1, make(map[string]interface{})},
	}, nil
}

func SetupCreateDeleteListPrincipal(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	return []WorkerConfig{
		{listPrincipalsWorker, 1, make(map[string]interface{})},
		{createDeletePrincipalWorker, threads - 1, make(map[string]interface{})},
	}, nil
}

func SetupCreateDeleteListTable(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	err = grantPermissionCatalog(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []WorkerConfig{
		{listTablesWorker, 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
		{createDeleteTableWorker, threads - 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func SetupCreateDeleteListView(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []WorkerConfig{
		{listViewsWorker, 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
		{createDeleteViewWorker, threads - 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}
func SetupCreateDeleteListFunction(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}
	return []WorkerConfig{
		{listFunctionsWorker, 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
		{createDeleteFunctionWorker, threads - 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}
func SetupCreateDeleteListModel(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []WorkerConfig{
		{listModelsWorker, 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
		{createDeleteModelWorker, threads - 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}
func SetupCreateDeleteListVolume(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []WorkerConfig{
		{listVolumesWorker, 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
		{createDeleteVolumeWorker, threads - 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func SetupUpdateGetCatalog(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	workers := make([]WorkerConfig, threads)
	for thread := range threads {
		catalogName, err := createCatalog(ctx, catalog)
		if err != nil {
			return nil, err
		}

		workers[thread] = WorkerConfig{
			workerFunc: updateGetCatalogWorker,
			threads:    1,
			params: map[string]interface{}{
				"catalogName": catalogName,
			},
		}
	}
	return workers, nil
}

func SetupUpdateGetPrincipal(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	workers := make([]WorkerConfig, threads)

	for thread := range threads {
		principalName := uuid.NewString()
		_, err := catalog.CreatePrincipal(ctx, principalName)
		if err != nil {
			return nil, err
		}
		workers[thread] = WorkerConfig{
			workerFunc: updateGetPrincipalWorker,
			threads:    1,
			params: map[string]interface{}{
				"principalName": principalName,
			},
		}
	}
	return workers, nil

}

func SetupUpdateGetSchema(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	workers := make([]WorkerConfig, threads)

	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}
	for thread := range threads {
		schemaName, err := createSchema(ctx, catalog, catalogName)
		if err != nil {
			return nil, err
		}

		workers[thread] = WorkerConfig{
			workerFunc: updateGetSchemaWorker,
			threads:    1,
			params: map[string]interface{}{
				"catalogName": catalogName,
				"schemaName":  schemaName,
			},
		}

	}

	return workers, nil
}

func SetupUpdateGetTable(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	workers := make([]WorkerConfig, threads)

	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	for thread := range threads {
		tableName := uuid.NewString()
		_, err := catalog.CreateTable(ctx, catalogName, schemaName, tableName)
		if err != nil {
			return nil, err
		}

		workers[thread] = WorkerConfig{
			workerFunc: updateGetTableWorker,
			threads:    1,
			params: map[string]interface{}{
				"catalogName": catalogName,
				"schemaName":  schemaName,
				"tableName":   tableName,
			},
		}
	}

	err = grantPermissionCatalog(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return workers, nil
}

func SetupUpdateGetView(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	workers := make([]WorkerConfig, threads)
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	for thread := range threads {

		viewName := uuid.NewString()
		_, err = catalog.CreateView(ctx, catalogName, schemaName, viewName)
		if err != nil {
			return nil, err
		}

		workers[thread] = WorkerConfig{
			updateGetViewWorker, 1, map[string]interface{}{
				"catalogName": catalogName,
				"schemaName":  schemaName,
				"viewName":    viewName,
			},
		}
	}

	return workers, nil
}

func SetupUpdateGetModel(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	workers := make([]WorkerConfig, threads)
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}
	for thread := range threads {
		modelName := uuid.NewString()
		_, err = catalog.CreateModel(ctx, catalogName, schemaName, modelName)
		if err != nil {
			return nil, err
		}

		workers[thread] = WorkerConfig{
			updateGetModelWorker, 1, map[string]interface{}{
				"catalogName": catalogName,
				"schemaName":  schemaName,
				"modelName":   modelName,
			},
		}

	}
	return workers, nil
}
func SetupUpdateGetVolume(ctx context.Context, catalog Catalog, threads int) ([]WorkerConfig, error) {
	workers := make([]WorkerConfig, threads)
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	for thread := range threads {
		volumeName := uuid.NewString()
		_, err = catalog.CreateVolume(ctx, catalogName, schemaName, volumeName)
		if err != nil {
			return nil, err
		}

		workers[thread] = WorkerConfig{
			updateGetVolumeWorker, 1, map[string]interface{}{
				"catalogName": catalogName,
				"schemaName":  schemaName,
				"volumeName":  volumeName,
			},
		}

	}

	return workers, nil
}
