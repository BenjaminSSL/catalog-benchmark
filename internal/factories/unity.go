package factories

import (
	"benchmark/internal/common"
	"benchmark/internal/unity"
	"net/http"
)

type UnityFactory struct {
	RequestContext common.RequestContext
}

func NewUnityFactory(endpoint string) *UnityFactory {
	return &UnityFactory{
		RequestContext: common.RequestContext{
			Host: endpoint,
		},
	}
}

func (f *UnityFactory) CreateCatalogRequest(name string) (*http.Request, error) {
	operation := unity.CreateCatalog{
		Name: name,
	}

	return operation.Build(f.RequestContext)
}

func (f *UnityFactory) DeleteCatalogRequest(name string) (*http.Request, error) {
	panic("This operation is not supported on unity catalog")
	return nil, nil
}

func (f *UnityFactory) UpdateCatalogRequest(name string) (*http.Request, error) {
	panic("This operation is not supported on unity catalog")
	return nil, nil
}

func (f *UnityFactory) ListCatalogsRequest() (*http.Request, error) {
	panic("This operation is not supported on unity catalog")
	return nil, nil
}
