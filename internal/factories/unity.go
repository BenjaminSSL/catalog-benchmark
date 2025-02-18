package factories

import (
	"benchmark/internal/common"
	"benchmark/internal/execution"
	"benchmark/internal/unity"
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

func (f *UnityFactory) CreateCatalogRequest(name string) execution.Request {
	operation := unity.CreateCatalog{
		Name: name,
	}

	return operation.Build(f.RequestContext)
}

func (f *UnityFactory) DeleteCatalogRequest(name string) execution.Request {
	return execution.Request{}
}
