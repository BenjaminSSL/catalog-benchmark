package polaris

import (
	"benchmark/internal/common"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

func NewCreateCatalogRequest(ctx context.Context, name string) (*http.Request, error) {
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

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/catalogs").SetJSONBody(jsonBody).Build(ctx)
}

func NewDeleteCatalogRequest(ctx context.Context, name string) (*http.Request, error) {
	return common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("/catalogs/%s", name)).Build(ctx)

}

func NewListCatalogsRequest(ctx context.Context) (*http.Request, error) {
	return common.NewRequestBuilder().SetEndpoint("/catalogs").Build(ctx)
}

func NewUpdateCatalogRequest(ctx context.Context, name string, entityVersion int) (*http.Request, error) {
	body := UpdateCatalogBody{
		CurrentEntityVersion: entityVersion,
		Properties:           CatalogProperties{},
		StorageConfigInfo: CatalogStorageConfigInfo{
			StorageType: "FILE",
		},
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	return common.NewRequestBuilder().SetMethod("PUT").SetEndpoint(fmt.Sprintf("/catalogs/%s", name)).SetJSONBody(jsonBody).Build(ctx)
}

func NewListPrincipalsRequest(ctx context.Context) (*http.Request, error) {
	return common.NewRequestBuilder().SetEndpoint("/principals").Build(ctx)
}

func CreatePrincipalRequest(ctx context.Context, name string) (*http.Request, error) {
	return nil, nil
}

func NewCreateSchemaRequest(ctx context.Context, name string, prefix string) (*http.Request, error) {
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

	jsonBody, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}

	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/catalogs").SetJSONBody(jsonBody).Build(ctx)
}
