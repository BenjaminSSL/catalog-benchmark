package polaris

import (
	"benchmark/internal/common"
	"net/http"
)

type CatalogRequestFactory struct {
	RequestContext common.RequestContext
}

func NewCatalogRequestFactory(endpoint string, token string) *CatalogRequestFactory {
	return &CatalogRequestFactory{
		RequestContext: common.RequestContext{
			Host:  endpoint,
			Token: token,
		},
	}
}

func (f *CatalogRequestFactory) CreateCatalogRequest(params CreateCatalogParams) (*http.Request, error) {
	operation := CreateCatalog{
		Name: params.Name,
	}

	return operation.Build(f.RequestContext)
}

func (f *CatalogRequestFactory) DeleteCatalogRequest(params DeleteCatalogParams) (*http.Request, error) {
	operation := DeleteCatalog{
		Name: params.Name,
	}
	return operation.Build(f.RequestContext)
}

func (f *CatalogRequestFactory) UpdateCatalogRequest(params UpdateCatalogParams) (*http.Request, error) {
	operation := UpdateCatalog{
		Name:          params.Name,
		EntityVersion: params.Version,
	}

	return operation.Build(f.RequestContext)
}

func (f *CatalogRequestFactory) ListCatalogsRequest() (*http.Request, error) {
	operation := ListCatalogs{}

	return operation.Build(f.RequestContext)
}

func (f *CatalogRequestFactory) CreateSchemaRequest(params CreateSchemaParams) (*http.Request, error) {
	operation := CreateSchema{}

	return operation.Build(f.RequestContext)
}
