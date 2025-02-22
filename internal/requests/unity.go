package requests

import (
	"benchmark/internal/catalog/unity"
	"benchmark/internal/common"
	"fmt"
	"net/http"
)

type UnityFactory struct {
	RequestContext common.RequestContext
}

func NewUnityFactory(host string) *UnityFactory {
	return &UnityFactory{
		RequestContext: common.RequestContext{
			Host: host,
		},
	}
}

func (f *UnityFactory) CreateCatalogRequest(params CreateCatalogParams) (*http.Request, error) {
	operation := unity.CreateCatalog{
		Name: params.Name,
	}

	return operation.Build(f.RequestContext)
}

func (f *UnityFactory) DeleteCatalogRequest(params DeleteCatalogParams) (*http.Request, error) {

	return nil, fmt.Errorf("this operation is not supported")
}

func (f *UnityFactory) UpdateCatalogRequest(params UpdateCatalogParams) (*http.Request, error) {

	return nil, fmt.Errorf("this operation is not supported")
}

func (f *UnityFactory) ListCatalogsRequest() (*http.Request, error) {

	return nil, fmt.Errorf("this operation is not supported")
}

func (f *UnityFactory) CreateSchemaRequest(params CreateSchemaParams) (*http.Request, error) {
	return nil, fmt.Errorf("this operation is not supported")
}
