package factories

import "benchmark/internal/execution"

type CatalogOperationFactory interface {
	CreateCatalogRequest(name string) execution.Request
	DeleteCatalogRequest(name string) execution.Request
	UpdateCatalogRequest(name string) execution.Request
	ListCatalogsRequest() execution.Request
}
