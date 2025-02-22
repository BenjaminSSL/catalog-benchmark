package requests

import (
	"net/http"
)

type RequestParams interface {
	Validate() error
}

type CatalogRequestFactory interface {
	CreateCatalogRequest(params RequestParams) (*http.Request, error)
	DeleteCatalogRequest(params RequestParams) (*http.Request, error)
	UpdateCatalogRequest(params RequestParams) (*http.Request, error)
	ListCatalogsRequest() (*http.Request, error)

	CreateSchemaRequest(params RequestParams) (*http.Request, error)
}

type CreateCatalogParams struct {
	Name string
}

func (p CreateCatalogParams) Validate() error {
	return nil
}

type DeleteCatalogParams struct {
	Name string
}

func (p DeleteCatalogParams) Validate() error {
	return nil
}

type UpdateCatalogParams struct {
	Name    string
	Version int
}

func (p UpdateCatalogParams) Validate() error {
	return nil
}

type CreateSchemaParams struct {
	Name   string
	Prefix string
}

func (p CreateSchemaParams) Validate() error {
	return nil
}
