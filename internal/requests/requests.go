package requests

import (
	"net/http"
)

type CatalogRequestFactory interface {
	CreateCatalogRequest(name string) (*http.Request, error)
	DeleteCatalogRequest(name string) (*http.Request, error)
	UpdateCatalogRequest(name string) (*http.Request, error)
	ListCatalogsRequest() (*http.Request, error)
}
