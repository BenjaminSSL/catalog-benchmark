package polaris

import (
	"benchmark/internal/common"
	"encoding/json"
	"fmt"
	"net/http"
)

func NewCreateCatalogRequest(context common.RequestContext, params CreateCatalogParams) (*http.Request, error) {
	body := CreateCatalogBody{
		Catalog: Catalog{
			EntityType: "INTERNAL",
			Name:       params.Name,
			Properties: CatalogProperties{
				DefaultBaseLocation: fmt.Sprintf("/%s/", params.Name),
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

	return common.NewRequestBuilder(context).SetMethod("POST").SetEndpoint("/api/management/v1/catalogs").SetJSONBody(jsonBody).Build()
}

func NewDeleteCatalogRequest(context common.RequestContext, params DeleteCatalogParams) (*http.Request, error) {
	endpoint := fmt.Sprintf("/api/management/v1/catalogs/%s", params.Name)
	return common.NewRequestBuilder(context).SetMethod("DELETE").SetEndpoint(endpoint).Build()

}

func NewListCatalogsRequest(context common.RequestContext) (*http.Request, error) {
	return common.NewRequestBuilder(context).SetEndpoint("/api/management/v1/catalogs").Build()
}

func NewUpdateCatalogRequest(context common.RequestContext, params UpdateCatalogParams) (*http.Request, error) {
	endpoint := fmt.Sprintf("/api/management/v1/catalogs/%s", params.Name)
	body := UpdateCatalogBody{
		CurrentEntityVersion: params.EntityVersion,
		Properties:           CatalogProperties{},
		StorageConfigInfo: CatalogStorageConfigInfo{
			StorageType: "FILE",
		},
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	return common.NewRequestBuilder(context).SetMethod("PUT").SetEndpoint(endpoint).SetJSONBody(jsonBody).Build()

}

func NewCreateSchemaRequest(context common.RequestContext, params CreateSchemaParams) (*http.Request, error) {
	body := CreateCatalogBody{
		Catalog: Catalog{
			EntityType: "INTERNAL",
			Name:       params.Name,
			Properties: CatalogProperties{
				DefaultBaseLocation: fmt.Sprintf("/%s/", params.Name),
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

	return common.NewRequestBuilder(context).SetMethod("POST").SetEndpoint("/api/management/v1/catalogs").SetJSONBody(jsonBody).Build()
}
