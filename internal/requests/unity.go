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

func (f *UnityFactory) CreateCatalogRequest(params RequestParams) (*http.Request, error) {
	p, ok := params.(CreateCatalogParams)
	if !ok {
		return nil, fmt.Errorf("invalid params")
	}
	operation := unity.CreateCatalog{
		Name: p.Name,
	}

	return operation.Build(f.RequestContext)
}

func (f *UnityFactory) DeleteCatalogRequest(params RequestParams) (*http.Request, error) {

	return nil, fmt.Errorf("this operation is not supported")
}

func (f *UnityFactory) UpdateCatalogRequest(params RequestParams) (*http.Request, error) {

	return nil, fmt.Errorf("this operation is not supported")
}

func (f *UnityFactory) ListCatalogsRequest() (*http.Request, error) {

	return nil, fmt.Errorf("this operation is not supported")
}

func (f *UnityFactory) CreateSchemaRequest(params RequestParams) (*http.Request, error) {
	return nil, fmt.Errorf("this operation is not supported")
}
