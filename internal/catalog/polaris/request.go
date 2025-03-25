package polaris

import (
	"benchmark/internal/common"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

var (
	Host = common.GetEnv("POLARIS_HOST", "localhost:8181")
	Path = common.GetEnv("POLARIS_PATH", "/api/management/v1")
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

	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/catalogs").SetJSONBody(jsonBody).Build(ctx, Host, Path, Token)
}

func NewDeleteCatalogRequest(ctx context.Context, name string) *http.Request {
	return common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("/catalogs/%s", name)).Build(ctx, Host, Path, Token)

}

func NewListCatalogsRequest(ctx context.Context) *http.Request {
	return common.NewRequestBuilder().SetEndpoint("/catalogs").Build(ctx, Host, Path, Token)
}

func NewGetCatalogRequest(ctx context.Context, name string) *http.Request {
	return common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("/catalogs/%s", name)).Build(ctx, Host, Path, Token)
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

	return common.NewRequestBuilder().SetMethod("PUT").SetEndpoint(fmt.Sprintf("/catalogs/%s", name)).SetJSONBody(jsonBody).Build(ctx, Host, Path, Token)
}

func NewListPrincipalsRequest(ctx context.Context) *http.Request {
	return common.NewRequestBuilder().SetEndpoint("/principals").Build(ctx, Host, Path, Token)
}

func CreatePrincipalRequest(ctx context.Context, name string) *http.Request {
	return nil
}

func NewCreateSchemaRequest(ctx context.Context, name string, prefix string) *http.Request {
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

	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/catalogs").SetJSONBody(jsonBody).Build(ctx, Host, Path, Token)
}
