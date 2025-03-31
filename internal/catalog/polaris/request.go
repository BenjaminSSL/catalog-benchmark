package polaris

import (
	"benchmark/internal/common"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
				DefaultBaseLocation: fmt.Sprintf("/%s/", name),
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

func NewUpdatePrincipalRequest(ctx context.Context, name string, entityVersion int, properties map[string]string) *http.Request {

	body := UpdatePrincipalBody{
		CurrentEntityVersion: entityVersion,
		Properties:           properties,
	}

	jsonBody, _ := json.Marshal(body)

	return common.NewRequestBuilder().SetMethod("PUT").SetEndpoint(fmt.Sprintf("/principals/%s", name)).SetJSONBody(jsonBody).Build(ctx, Host, PathManagement, Token)

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

func NewDeleteNamespaceRequest(ctx context.Context, name string, catalogName string) *http.Request {
	return common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("%s/namespaces/%s", catalogName, name)).Build(ctx, Host, PathCatalog, Token)

}

func NewListNamespacesRequest(ctx context.Context, catalogName string) *http.Request {
	return common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("%s/namespaces", catalogName)).Build(ctx, Host, PathCatalog, Token)

}

func NewCreateNamespaceRequest(ctx context.Context, name string, catalogName string) *http.Request {
	body := CreateNamespaceBody{
		Namespace:  []string{name},
		Properties: map[string]string{},
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}
	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint(fmt.Sprintf("%s/namespaces", catalogName)).SetJSONBody(jsonBody).Build(ctx, Host, PathCatalog, Token)
}
