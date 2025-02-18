package unity

import (
	"benchmark/internal/common"
	"benchmark/internal/execution"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

type CreateCatalog struct {
	Name string
}

func (op *CreateCatalog) Build(context common.RequestContext) execution.Request {
	baseURL := fmt.Sprintf("http://%s/api/2.1/unity-catalog/catalogs", context.Host)

	body := CreateCatalogBody{
		Name: op.Name,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		log.Printf("failed to serialize the request body: %v", err)
		panic(err)
	}

	headers := http.Header{}
	headers.Add("Content-Type", "application/json")

	return execution.Request{
		Method:  "POST",
		URL:     baseURL,
		Body:    jsonBody,
		Headers: headers,
	}
}
