package requests

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/common"
	"net/http"
)

type PolarisFactory struct {
	RequestContext common.RequestContext
}

func NewPolarisFactory(endpoint string, token string) *PolarisFactory {
	return &PolarisFactory{
		RequestContext: common.RequestContext{
			Host:  endpoint,
			Token: token,
		},
	}
}

func (f *PolarisFactory) CreateCatalogRequest(params CreateCatalogParams) (*http.Request, error) {
	operation := polaris.CreateCatalog{
		Name: params.Name,
	}

	return operation.Build(f.RequestContext)
}

func (f *PolarisFactory) DeleteCatalogRequest(params DeleteCatalogParams) (*http.Request, error) {
	operation := polaris.DeleteCatalog{
		Name: params.Name,
	}
	return operation.Build(f.RequestContext)
}

func (f *PolarisFactory) UpdateCatalogRequest(params UpdateCatalogParams) (*http.Request, error) {
	return nil, nil
}

func (f *PolarisFactory) ListCatalogsRequest() (*http.Request, error) {
	operation := polaris.ListCatalogs{}

	return operation.Build(f.RequestContext)
}

func (f *PolarisFactory) CreateSchemaRequest(params CreateSchemaParams) (*http.Request, error) {
	operation := polaris.CreateSchema{}

	return operation.Build(f.RequestContext)
}
