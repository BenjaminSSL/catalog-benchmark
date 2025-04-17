package polaris

import (
	"benchmark/internal/common"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
)

var (
	Host           = common.GetEnv("POLARIS_HOST", "localhost:8181")
	PathManagement = common.GetEnv("POLARIS_PATH_MANAGEMENT", "/api/management/v1")
	PathCatalog    = common.GetEnv("POLARIS_PATH_CATALOG", "/api/catalog/v1")
)

var Token string

func SetToken(token string) {
	Token = token
}

func NewCreateCatalogRequest(ctx context.Context, name string) *http.Request {
	body := CreateCatalogBody{
		Catalog: Catalog{
			EntityType: "INTERNAL",
			Name:       name,
			Properties: CatalogProperties{
				DefaultBaseLocation: fmt.Sprintf("file:///tmp/%s/", name),
			},
			StorageConfigInfo: CatalogStorageConfigInfo{
				StorageType: "FILE",
			},
		},
	}

	jsonBody, _ := json.Marshal(body)

	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/catalogs").SetJSONBody(jsonBody).Build(ctx, Host, PathManagement, Token)
}

func NewDeleteCatalogRequest(ctx context.Context, name string) *http.Request {
	return common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("/catalogs/%s", name)).Build(ctx, Host, PathManagement, Token)
}

func NewListCatalogsRequest(ctx context.Context) *http.Request {
	return common.NewRequestBuilder().SetEndpoint("/catalogs").Build(ctx, Host, PathManagement, Token)
}

func NewGetCatalogRequest(ctx context.Context, name string) *http.Request {
	return common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("/catalogs/%s", name)).Build(ctx, Host, PathManagement, Token)
}
func NewGetPrincipalRequest(ctx context.Context, name string) *http.Request {
	return common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("/principals/%s", name)).Build(ctx, Host, PathManagement, Token)
}

func NewGetNamespaceRequest(ctx context.Context, catalogName string, namespaceName string) *http.Request {
	return common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("%s/namespaces/%s", catalogName, namespaceName)).Build(ctx, Host, PathCatalog, Token)
}

func NewGetTableRequest(ctx context.Context, catalogName string, namespaceName string, name string) *http.Request {
	return common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/tables/%s", catalogName, namespaceName, name)).Build(ctx, Host, PathCatalog, Token)
}

func NewUpdateCatalogRequest(ctx context.Context, name string, entityVersion int, properties map[string]string) *http.Request {
	var catalogProperties = CatalogProperties{}
	if properties != nil {
		catalogProperties = CatalogProperties{
			AdditionalProps: properties,
		}
	}

	body := UpdateCatalogBody{
		CurrentEntityVersion: entityVersion,
		Properties:           catalogProperties,
		StorageConfigInfo: CatalogStorageConfigInfo{
			StorageType: "FILE",
		},
	}

	jsonBody, _ := json.Marshal(body)

	return common.NewRequestBuilder().SetMethod("PUT").SetEndpoint(fmt.Sprintf("/catalogs/%s", name)).SetJSONBody(jsonBody).Build(ctx, Host, PathManagement, Token)
}

// Principal

func NewUpdatePrincipalRequest(ctx context.Context, name string, entityVersion int, properties map[string]string) *http.Request {

	body := UpdatePrincipalBody{
		CurrentEntityVersion: entityVersion,
		Properties:           properties,
	}

	jsonBody, _ := json.Marshal(body)

	return common.NewRequestBuilder().SetMethod("PUT").SetEndpoint(fmt.Sprintf("/principals/%s", name)).SetJSONBody(jsonBody).Build(ctx, Host, PathManagement, Token)
}

func NewUpdateNamespaceRequest(ctx context.Context, catalogName string, namespaceName string, properties map[string]string) *http.Request {
	body := UpdateNamespaceBody{
		Updates: properties,
	}

	jsonBody, _ := json.Marshal(body)
	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/properties", catalogName, namespaceName)).SetJSONBody(jsonBody).Build(ctx, Host, PathCatalog, Token)
}

func NewUpdateTableRequest(ctx context.Context, catalogName string, namespaceName string, tableName string, properties map[string]string) *http.Request {
	body := UpdateTableBody{
		Updates: []map[string]interface{}{
			{
				"action":  "set-properties",
				"updates": properties,
			},
		},
	}

	jsonBody, _ := json.Marshal(body)

	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/tables/%s", catalogName, namespaceName, tableName)).SetJSONBody(jsonBody).Build(ctx, Host, PathCatalog, Token)
}

func NewListPrincipalsRequest(ctx context.Context) *http.Request {
	return common.NewRequestBuilder().SetEndpoint(fmt.Sprintf("/principals")).Build(ctx, Host, PathManagement, Token)
}

func NewCreatePrincipalRequest(ctx context.Context, name string) *http.Request {
	body := CreatePrincipalBody{
		Principal: Principal{
			Name: name,
		},
		CredentialRotationRequired: false,
	}

	jsonBody, _ := json.Marshal(body)

	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/principals").SetJSONBody(jsonBody).Build(ctx, Host, PathManagement, Token)
}

func NewDeletePrincipalRequest(ctx context.Context, name string) *http.Request {
	return common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("/principals/%s", name)).Build(ctx, Host, PathManagement, Token)

}

func NewDeleteNamespaceRequest(ctx context.Context, catalogName string, namespaceName string) *http.Request {
	return common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("%s/namespaces/%s", catalogName, namespaceName)).Build(ctx, Host, PathCatalog, Token)

}

func NewListNamespacesRequest(ctx context.Context, catalogName string, pageToken string, maxResults int) *http.Request {

	builder := common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("%s/namespaces", catalogName))

	if pageToken != "" {
		builder.AddQueryParam("pageToken", pageToken)
	}
	if maxResults != 0 {
		builder.AddQueryParam("pageSize", strconv.Itoa(maxResults))
	}

	return builder.Build(ctx, Host, PathCatalog, Token)
}

func NewCreateNamespaceRequest(ctx context.Context, catalogName string, namespaceName string) *http.Request {
	body := CreateNamespaceBody{
		Namespace:  []string{namespaceName},
		Properties: map[string]string{},
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}

	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint(fmt.Sprintf("%s/namespaces", catalogName)).SetJSONBody(jsonBody).Build(ctx, Host, PathCatalog, Token)
}

func NewCreateTableRequest(ctx context.Context, catalogName string, namespaceName string, tableName string) *http.Request {
	body := CreateTableBody{
		Name: tableName,
		Schema: TableSchema{
			Type:   "struct",
			Fields: make([]interface{}, 0),
		},
		StageCreate: false,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}

	req := common.NewRequestBuilder().SetMethod("POST").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/tables", catalogName, namespaceName)).SetJSONBody(jsonBody).Build(ctx, Host, PathCatalog, Token)

	return req

}

func NewDeleteTableRequest(ctx context.Context, catalogName string, namespaceName string, name string) *http.Request {
	return common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/tables/%s", catalogName, namespaceName, name)).Build(ctx, Host, PathCatalog, Token)
}

func NewGrantPermissionCatalogRequest(ctx context.Context, catalogName string, privilege string) *http.Request {
	body := GrantCatalogPermissionBody{
		Grants: GrantPrivilege{
			Privilege: privilege,
			Type:      "catalog",
		},
	}

	jsonBody, _ := json.Marshal(body)

	return common.NewRequestBuilder().SetMethod("PUT").SetEndpoint(fmt.Sprintf("catalogs/%s/catalog-roles/catalog_admin/grants", catalogName)).SetJSONBody(jsonBody).Build(ctx, Host, PathManagement, Token)
}

func NewListTablesRequest(ctx context.Context, catalogName string, namespaceName string, pageToken string, maxResults int) *http.Request {
	builder := common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/tables", catalogName, namespaceName))

	if pageToken != "" {
		builder.AddQueryParam("pageToken", pageToken)
	}
	if maxResults != 0 {
		builder.AddQueryParam("pageSize", strconv.Itoa(maxResults))
	}

	req := builder.Build(ctx, Host, PathCatalog, Token)
	return req
}

func NewCreateViewRequest(ctx context.Context, catalogName string, namespaceName string, viewName string) *http.Request {
	body := CreateViewBody{
		Name:     viewName,
		Location: fmt.Sprintf("file:///tmp/%s/%s/", catalogName, namespaceName),
		Schema: ViewBodySchema{
			Type:   "struct",
			Fields: []interface{}{},
		},
		ViewVersion: ViewBodyViewVersion{
			VersionId:   0,
			TimestampMs: 0,
			SchemaId:    0,
			Summary:     map[string]string{},
			Representations: []ViewBodyViewVersionRepresentation{{
				Type:    "sql",
				Sql:     "SELECT 1 AS test_column",
				Dialect: "ansi",
			}},
			DefaultCatalog: catalogName,
			DefaultNamespace: []string{
				namespaceName,
			},
		},
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}

	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/views", catalogName, namespaceName)).SetJSONBody(jsonBody).Build(ctx, Host, PathCatalog, Token)
}

func NewDeleteViewRequest(ctx context.Context, catalogName string, namespaceName string, viewName string) *http.Request {
	return common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/views/%s", catalogName, namespaceName, viewName)).Build(ctx, Host, PathCatalog, Token)
}

func NewGetViewRequest(ctx context.Context, catalogName string, namespaceName string, viewName string) *http.Request {
	return common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/views/%s", catalogName, namespaceName, viewName)).Build(ctx, Host, PathCatalog, Token)
}

func NewListViewsRequest(ctx context.Context, catalogName string, namespaceName string, pageToken string, maxResults int) *http.Request {
	builder := common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/views", catalogName, namespaceName))

	if pageToken != "" {
		builder.AddQueryParam("pageToken", pageToken)
	}
	if maxResults != 0 {
		builder.AddQueryParam("pageSize", strconv.Itoa(maxResults))
	}

	req := builder.Build(ctx, Host, PathCatalog, Token)
	return req
}

func NewUpdateViewRequest(ctx context.Context, catalogName string, namespaceName string, viewName string, properties map[string]string) *http.Request {
	body := UpdateViewBody{
		Updates: []map[string]interface{}{
			{
				"action":  "set-properties",
				"updates": properties,
			},
		},
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}

	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/views/%s", catalogName, namespaceName, viewName)).SetJSONBody(jsonBody).Build(ctx, Host, PathCatalog, Token)
}
