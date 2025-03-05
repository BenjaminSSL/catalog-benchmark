package polaris

import (
	"benchmark/internal/common"
	"encoding/json"
	"fmt"
	"net/http"
)

func NewCreateCatalogRequest(context common.RequestContext, name string) (*http.Request, error) {
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

	return common.NewRequestBuilder(context).SetMethod("POST").SetEndpoint("/api/management/v1/catalogs").SetJSONBody(jsonBody).Build()
}

func NewDeleteCatalogRequest(context common.RequestContext, name string) (*http.Request, error) {
	endpoint := fmt.Sprintf("/api/management/v1/catalogs/%s", name)
	return common.NewRequestBuilder(context).SetMethod("DELETE").SetEndpoint(endpoint).Build()

}

func NewListCatalogsRequest(context common.RequestContext) (*http.Request, error) {
	return common.NewRequestBuilder(context).SetEndpoint("/api/management/v1/catalogs").Build()
}

func NewUpdateCatalogRequest(context common.RequestContext, name string, entityVersion int) (*http.Request, error) {
	endpoint := fmt.Sprintf("/api/management/v1/catalogs/%s", name)
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

	return common.NewRequestBuilder(context).SetMethod("PUT").SetEndpoint(endpoint).SetJSONBody(jsonBody).Build()

}

func NewCreateSchemaRequest(context common.RequestContext, name string, prefix string) (*http.Request, error) {
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

	return common.NewRequestBuilder(context).SetMethod("POST").SetEndpoint("/api/management/v1/catalogs").SetJSONBody(jsonBody).Build()
}
