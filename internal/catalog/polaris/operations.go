package polaris

import (
	"benchmark/internal/common"
	"encoding/json"
	"fmt"
	"net/http"
)

type CreateCatalog struct {
	Name string
}

func (op *CreateCatalog) Build(context common.RequestContext) (*http.Request, error) {

	body := CreateCatalogBody{
		Catalog: SchemaCatalog{
			EntityType: "INTERNAL",
			Name:       op.Name,
			Properties: CatalogProperties{
				DefaultBaseLocation: fmt.Sprintf("/%s/", op.Name),
			},
			StorageConfigInfo: CatalogStorageConfigInfo{
				StorageType: "FILE",
			},
		},
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}

	return common.NewRequestBuilder(context).SetMethod("POST").SetEndpoint("/api/management/v1/catalogs").SetJSONBody(jsonBody).Build()
}

type DeleteCatalog struct {
	Name string
}

func (op *DeleteCatalog) Build(context common.RequestContext) (*http.Request, error) {
	endpoint := fmt.Sprintf("/api/management/v1/catalogs/%s", op.Name)
	return common.NewRequestBuilder(context).SetMethod("DELETE").SetEndpoint(endpoint).Build()

}

type ListCatalogs struct {
}

func (op *ListCatalogs) Build(context common.RequestContext) (*http.Request, error) {
	return common.NewRequestBuilder(context).SetEndpoint("/api/management/v1/catalogs").Build()
}

type UpdateCatalog struct{}

func (op *UpdateCatalog) Build(context common.RequestContext) (*http.Request, error) {
	return nil, nil

}
