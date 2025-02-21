package factories

import (
	"benchmark/internal/common"
	"benchmark/internal/execution"
	"benchmark/internal/polaris"
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

func (f *PolarisFactory) CreateCatalogRequest(name string) (*http.Request, error) {
	operation := polaris.CreateCatalog{
		Name: name,
	}

	return operation.Build(f.RequestContext)
}

func (f *PolarisFactory) DeleteCatalogRequest(name string) (*http.Request, error) {
	operation := polaris.DeleteCatalog{
		Name: name,
	}
	return operation.Build(f.RequestContext)
}

func (f *PolarisFactory) UpdateCatalogRequest(name string) (*http.Request, error) {
	return nil, nil
}

func (f *PolarisFactory) ListCatalogsRequest() (*http.Request, error) {
	operation := polaris.ListCatalogs{}

	return operation.Build(f.RequestContext)
}
