package polaris

import (
	"benchmark/internal/common"
	"benchmark/internal/execution"

	"encoding/json"
	"fmt"
	"net/http"
)

type CreateCatalog struct {
	Name string
}

func (op *CreateCatalog) Build(context common.RequestContext) execution.Request {
	baseURL := fmt.Sprintf("http://%s/api/management/v1/catalogs", context.Host)

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

	headers := http.Header{}
	headers.Add("Content-Type", "application/json")
	headers.Add("Authorization", fmt.Sprintf("Bearer %s", context.Token))

	return execution.Request{
		Method:  "POST",
		URL:     baseURL,
		Body:    jsonBody,
		Headers: headers,
	}
}

type DeleteCatalog struct {
	Name string
}

func (op *DeleteCatalog) Build(context common.RequestContext) execution.Request {
	baseURL := fmt.Sprintf("http://%s/api/management/v1/catalogs/%s", context.Host, op.Name)

	headers := http.Header{}
	headers.Add("Authorization", fmt.Sprintf("Bearer %s", context.Token))

	return execution.Request{
		Method:  "DELETE",
		URL:     baseURL,
		Headers: headers,
	}
}

type ListCatalogs struct {
}

func (op *ListCatalogs) Build(context common.RequestContext) execution.Request {
	baseURL := fmt.Sprintf("http://%s/api/management/v1/catalogs", context.Host)

	headers := http.Header{}
	headers.Add("Authorization", fmt.Sprintf("Bearer %s", context.Token))

	return execution.Request{
		Method:  "GET",
		URL:     baseURL,
		Headers: headers,
	}
}

type
