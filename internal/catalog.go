package internal

import (
	"context"
	"net/http"
)

type Catalog interface {
	// Catalog
	CreateCatalog(ctx context.Context, name string) (*http.Response, error)
	GetCatalog(ctx context.Context, name string) (*http.Response, error)
	UpdateCatalog(ctx context.Context, name string, params map[string]interface{}) (*http.Response, error)
	DeleteCatalog(ctx context.Context, name string) (*http.Response, error)
	ListCatalogs(ctx context.Context, params map[string]interface{}) ([]*http.Response, error)

	// Principal
	CreatePrincipal(ctx context.Context, name string) (*http.Response, error)
	GetPrincipal(ctx context.Context, name string) (*http.Response, error)
	UpdatePrincipal(ctx context.Context, name string, params map[string]interface{}) (*http.Response, error)
	DeletePrincipal(ctx context.Context, name string) (*http.Response, error)
	ListPrincipals(ctx context.Context, params map[string]interface{}) ([]*http.Response, error)

	// Schema
	CreateSchema(ctx context.Context, catalogName string, schemaName string) (*http.Response, error)
	GetSchema(ctx context.Context, catalogName string, schemaName string) (*http.Response, error)
	UpdateSchema(ctx context.Context, catalogName string, schemaName string, params map[string]interface{}) (*http.Response, error)
	DeleteSchema(ctx context.Context, catalogName string, schemaName string) (*http.Response, error)
	ListSchemas(ctx context.Context, catalogName string, params map[string]interface{}) ([]*http.Response, error)

	// Table
	CreateTable(ctx context.Context, catalogName string, schemaName string, tableName string) (*http.Response, error)
	GetTable(ctx context.Context, catalogName string, schemaName string, tableName string) (*http.Response, error)
	UpdateTable(ctx context.Context, catalogName string, schemaName string, tableName string, params map[string]interface{}) (*http.Response, error)
	DeleteTable(ctx context.Context, catalogName string, schemaName string, tableName string) (*http.Response, error)
	ListTables(ctx context.Context, catalogName string, schemaName string, params map[string]interface{}) ([]*http.Response, error)

	// View
	CreateView(ctx context.Context, catalogName string, schemaName string, viewName string) (*http.Response, error)
	GetView(ctx context.Context, catalogName string, schemaName string, viewName string) (*http.Response, error)
	UpdateView(ctx context.Context, catalogName string, schemaName string, viewName string, params map[string]interface{}) (*http.Response, error)
	DeleteView(ctx context.Context, catalogName string, schemaName string, viewName string) (*http.Response, error)
	ListViews(ctx context.Context, catalogName string, schemaName string, params map[string]interface{}) ([]*http.Response, error)

	// Function
	CreateFunction(ctx context.Context, catalogName string, schemaName string, functionName string) (*http.Response, error)
	GetFunction(ctx context.Context, catalogName string, schemaName string, functionName string) (*http.Response, error)
	DeleteFunction(ctx context.Context, catalogName string, schemaName string, functionName string) (*http.Response, error)
	ListFunctions(ctx context.Context, catalogName string, schemaName string, params map[string]interface{}) ([]*http.Response, error)

	// Model
	CreateModel(ctx context.Context, catalogName string, schemaName string, modelName string) (*http.Response, error)
	GetModel(ctx context.Context, catalogName string, schemaName string, modelName string) (*http.Response, error)
	UpdateModel(ctx context.Context, catalogName string, schemaName string, modelName string, params map[string]interface{}) (*http.Response, error)
	DeleteModel(ctx context.Context, catalogName string, schemaName string, modelName string) (*http.Response, error)
	ListModels(ctx context.Context, catalogName string, schemaName string, params map[string]interface{}) ([]*http.Response, error)

	// Volume
	CreateVolume(ctx context.Context, catalogName string, schemaName string, volumeName string) (*http.Response, error)
	GetVolume(ctx context.Context, catalogName string, schemaName string, volumeName string) (*http.Response, error)
	UpdateVolume(ctx context.Context, catalogName string, schemaName string, volumeName string, params map[string]interface{}) (*http.Response, error)
	DeleteVolume(ctx context.Context, catalogName string, schemaName string, volumeName string) (*http.Response, error)
	ListVolumes(ctx context.Context, catalogName string, schemaName string, params map[string]interface{}) ([]*http.Response, error)

	GrantPermissionCatalog(ctx context.Context, catalogName string, params map[string]interface{}) (*http.Response, error)
}
