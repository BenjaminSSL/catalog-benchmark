package setup

import (
	"benchmark/internal"
	"context"
	"github.com/google/uuid"
)

func grantPermissionCatalog(ctx context.Context, catalog internal.Catalog, catalogName string) error {
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

func createCatalog(ctx context.Context, catalog internal.Catalog) (string, error) {
	catalogName := uuid.NewString()

	_, err := catalog.CreateCatalog(ctx, catalogName)
	if err != nil {
		return "", err
	}

	return catalogName, nil
}

func createSchema(ctx context.Context, catalog internal.Catalog, catalogName string) (string, error) {
	schemaName := uuid.NewString()

	_, err := catalog.CreateSchema(ctx, catalogName, schemaName)
	if err != nil {
		return "", err
	}

	return schemaName, nil
}

func CreateCatalog(threads int) ([]internal.WorkerConfig, error) {
	return []internal.WorkerConfig{
		{internal.CreateCatalogWorker, threads, make(map[string]interface{})},
	}, nil
}

func CreatePrincipal(threads int) ([]internal.WorkerConfig, error) {

	return []internal.WorkerConfig{
		{internal.CreatePrincipalWorker, threads, make(map[string]interface{})},
	}, nil
}

func CreateSchema(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	return []internal.WorkerConfig{
		{internal.CreateSchemaWorker, threads, map[string]interface{}{"catalogName": catalogName}},
	}, nil
}

func CreateTable(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	err = grantPermissionCatalog(ctx, catalog, catalogName)

	return []internal.WorkerConfig{
		{internal.CreateTableWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func CreateView(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []internal.WorkerConfig{
		{internal.CreateViewWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func CreateFunction(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []internal.WorkerConfig{
		{internal.CreateFunctionWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}
func CreateModel(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []internal.WorkerConfig{
		{internal.CreateModelWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}
func CreateVolume(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []internal.WorkerConfig{
		{internal.CreateVolumeWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func CreateDeleteCatalog(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	return []internal.WorkerConfig{
		{internal.CreateDeleteCatalogWorker, threads, make(map[string]interface{})},
	}, nil
}

func CreateDeleteSchema(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	return []internal.WorkerConfig{
		{internal.CreateDeleteSchemaWorker, threads, map[string]interface{}{
			"catalogName": catalogName}},
	}, nil
}

func CreateDeletePrincipal(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	return []internal.WorkerConfig{
		{internal.CreateDeletePrincipalWorker, threads, make(map[string]interface{})},
	}, nil
}

func CreateDeleteTable(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	err = grantPermissionCatalog(ctx, catalog, catalogName)

	return []internal.WorkerConfig{
		{internal.CreateDeleteTableWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func CreateDeleteView(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []internal.WorkerConfig{
		{internal.CreateDeleteViewWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func CreateDeleteFunction(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []internal.WorkerConfig{
		{internal.CreateFunctionWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func CreateDeleteModel(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []internal.WorkerConfig{
		{internal.CreateDeleteModelWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}
func CreateDeleteVolume(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []internal.WorkerConfig{
		{internal.CreateDeleteVolumeWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func UpdateCatalog(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	return []internal.WorkerConfig{
		{internal.UpdateCatalogWorker, threads, map[string]interface{}{
			"catalogName": catalogName}},
	}, nil
}

func UpdatePrincipal(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	principalName := uuid.NewString()

	_, err := catalog.CreatePrincipal(ctx, principalName)
	if err != nil {
		return nil, err
	}
	return []internal.WorkerConfig{
		{internal.UpdatePrincipalWorker, threads, map[string]interface{}{
			"principalName": principalName}},
	}, nil
}

func UpdateSchema(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []internal.WorkerConfig{
		{internal.UpdateSchemaWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func UpdateTable(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
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
	return []internal.WorkerConfig{
		{internal.UpdateTableWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName, "tableName": tableName}},
	}, nil

}

func UpdateView(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
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

	return []internal.WorkerConfig{
		{internal.UpdateViewWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName, "viewName": viewName}},
	}, nil
}

func UpdateModel(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
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
	return []internal.WorkerConfig{
		{internal.UpdateModelWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName, "modelName": modelName}},
	}, nil
}
func UpdateVolume(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
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

	return []internal.WorkerConfig{
		{internal.UpdateVolumeWorker, threads, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName, "volumeName": volumeName}},
	}, nil
}

func CreateDeleteListSchema(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	return []internal.WorkerConfig{
		{internal.ListSchemasWorker, 1, map[string]interface{}{"catalogName": catalogName}},
		{internal.CreateDeleteSchemaWorker, threads - 1, map[string]interface{}{"catalogName": catalogName}},
	}, nil
}

func CreateDeleteListCatalog(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	return []internal.WorkerConfig{
		{internal.ListCatalogsWorker, 1, make(map[string]interface{})},
		{internal.CreateDeleteCatalogWorker, threads - 1, make(map[string]interface{})},
	}, nil
}

func CreateDeleteListPrincipal(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	return []internal.WorkerConfig{
		{internal.ListPrincipalsWorker, 1, make(map[string]interface{})},
		{internal.CreateDeletePrincipalWorker, threads - 1, make(map[string]interface{})},
	}, nil
}

func CreateDeleteListTable(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
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

	return []internal.WorkerConfig{
		{internal.ListTablesWorker, 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
		{internal.CreateDeleteTableWorker, threads - 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func CreateDeleteListView(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []internal.WorkerConfig{
		{internal.ListViewsWorker, 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
		{internal.CreateDeleteViewWorker, threads - 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}
func CreateDeleteListFunction(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}
	return []internal.WorkerConfig{
		{internal.ListFunctionsWorker, 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
		{internal.CreateDeleteFunctionWorker, threads - 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}
func CreateDeleteListModel(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []internal.WorkerConfig{
		{internal.ListModelsWorker, 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
		{internal.CreateDeleteModelWorker, threads - 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}
func CreateDeleteListVolume(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}

	schemaName, err := createSchema(ctx, catalog, catalogName)
	if err != nil {
		return nil, err
	}

	return []internal.WorkerConfig{
		{internal.ListVolumesWorker, 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
		{internal.CreateDeleteVolumeWorker, threads - 1, map[string]interface{}{
			"catalogName": catalogName, "schemaName": schemaName}},
	}, nil
}

func UpdateGetCatalog(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	workers := make([]internal.WorkerConfig, threads)
	for thread := range threads {
		catalogName, err := createCatalog(ctx, catalog)
		if err != nil {
			return nil, err
		}

		workers[thread] = internal.WorkerConfig{
			internal.UpdateGetCatalogWorker,
			1,
			map[string]interface{}{
				"catalogName": catalogName,
			},
		}
	}
	return workers, nil
}

func UpdateGetPrincipal(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	workers := make([]internal.WorkerConfig, threads)

	for thread := range threads {
		principalName := uuid.NewString()
		_, err := catalog.CreatePrincipal(ctx, principalName)
		if err != nil {
			return nil, err
		}
		workers[thread] = internal.WorkerConfig{
			internal.UpdateGetPrincipalWorker,
			1,
			map[string]interface{}{
				"principalName": principalName,
			},
		}
	}
	return workers, nil

}

func UpdateGetSchema(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	workers := make([]internal.WorkerConfig, threads)

	catalogName, err := createCatalog(ctx, catalog)
	if err != nil {
		return nil, err
	}
	for thread := range threads {
		schemaName, err := createSchema(ctx, catalog, catalogName)
		if err != nil {
			return nil, err
		}

		workers[thread] = internal.WorkerConfig{
			internal.UpdateGetSchemaWorker,
			1,
			map[string]interface{}{
				"catalogName": catalogName,
				"schemaName":  schemaName,
			},
		}

	}

	return workers, nil
}

func UpdateGetTable(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	workers := make([]internal.WorkerConfig, threads)

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

		workers[thread] = internal.WorkerConfig{
			internal.UpdateGetTableWorker,
			1,
			map[string]interface{}{
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

func UpdateGetView(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	workers := make([]internal.WorkerConfig, threads)
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

		workers[thread] = internal.WorkerConfig{
			internal.UpdateGetViewWorker, 1, map[string]interface{}{
				"catalogName": catalogName,
				"schemaName":  schemaName,
				"viewName":    viewName,
			},
		}
	}

	return workers, nil
}

func UpdateGetModel(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	workers := make([]internal.WorkerConfig, threads)
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

		workers[thread] = internal.WorkerConfig{
			internal.UpdateGetModelWorker, 1, map[string]interface{}{
				"catalogName": catalogName,
				"schemaName":  schemaName,
				"modelName":   modelName,
			},
		}

	}
	return workers, nil
}
func UpdateGetVolume(ctx context.Context, catalog internal.Catalog, threads int) ([]internal.WorkerConfig, error) {
	workers := make([]internal.WorkerConfig, threads)
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

		workers[thread] = internal.WorkerConfig{
			internal.UpdateGetVolumeWorker, 1, map[string]interface{}{
				"catalogName": catalogName,
				"schemaName":  schemaName,
				"volumeName":  volumeName,
			},
		}

	}

	return workers, nil
}
