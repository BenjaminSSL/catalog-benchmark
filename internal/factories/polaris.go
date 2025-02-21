package factories

import (
	"benchmark/internal/common"
	"benchmark/internal/execution"
	"benchmark/internal/polaris"
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

func (f *PolarisFactory) CreateCatalogRequest(name string) execution.Request {
	operation := polaris.CreateCatalog{
		Name: name,
	}

	return operation.Build(f.RequestContext)
}

func (f *PolarisFactory) DeleteCatalogRequest(name string) execution.Request {
	operation := polaris.DeleteCatalog{
		Name: name,
	}
	return operation.Build(f.RequestContext)
}

func (f *PolarisFactory) ListCatalogsRequest() execution.Request {
	operation := polaris.ListCatalogs{}

	return operation
}
