package requests

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/common"
	"fmt"
	"net/http"
)

type PolarisFactory struct {
	RequestContext common.RequestContext
}

func NewPolarisFactory(host string, token string) *PolarisFactory {
	return &PolarisFactory{
		RequestContext: common.RequestContext{
			Host:  host,
			Token: token,
		},
	}
}

func (f *PolarisFactory) CreateCatalogRequest(params RequestParams) (*http.Request, error) {
	p, ok := params.(CreateCatalogParams)
	if !ok {
		return nil, fmt.Errorf("invalid params")
	}
	operation := polaris.CreateCatalog{
		Name: p.Name,
	}

	return operation.Build(f.RequestContext)
}

func (f *PolarisFactory) DeleteCatalogRequest(params RequestParams) (*http.Request, error) {
	p, ok := params.(DeleteCatalogParams)
	if !ok {
		return nil, fmt.Errorf("invalid params")
	}
	operation := polaris.DeleteCatalog{
		Name: p.Name,
	}
	return operation.Build(f.RequestContext)
}

func (f *PolarisFactory) UpdateCatalogRequest(params RequestParams) (*http.Request, error) {
	p, ok := params.(UpdateCatalogParams)
	if !ok {
		return nil, fmt.Errorf("invalid params")
	}
	operation := polaris.UpdateCatalog{
		Name:          p.Name,
		EntityVersion: p.Version,
	}

	return operation.Build(f.RequestContext)
}

func (f *PolarisFactory) ListCatalogsRequest() (*http.Request, error) {
	operation := polaris.ListCatalogs{}

	return operation.Build(f.RequestContext)
}

func (f *PolarisFactory) CreateSchemaRequest(params RequestParams) (*http.Request, error) {
	operation := polaris.CreateSchema{}

	return operation.Build(f.RequestContext)
}
