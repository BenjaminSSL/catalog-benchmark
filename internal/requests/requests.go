package requests

import (
	"net/http"
)

type CatalogRequestFactory interface {
	CreateCatalogRequest(params CreateCatalogParams) (*http.Request, error)
	DeleteCatalogRequest(params DeleteCatalogParams) (*http.Request, error)
	UpdateCatalogRequest(params UpdateCatalogParams) (*http.Request, error)
	ListCatalogsRequest() (*http.Request, error)

	CreateSchemaRequest(params CreateSchemaParams) (*http.Request, error)
}

type CreateCatalogParams struct {
	Name string
}

type DeleteCatalogParams struct {
	Name string
}

type UpdateCatalogParams struct{}

type CreateSchemaParams struct {
	Name   string
	Prefix string
}
