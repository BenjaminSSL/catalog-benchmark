package unity

import (
	"benchmark/internal/common"
	"encoding/json"
	"net/http"
)

func NewCreateCatalogRequest(context common.RequestContext, params CreateCatalogParams) (*http.Request, error) {
	body := CreateCatalogBody{
		Name: params.Name,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}

	return common.NewRequestBuilder(context).SetMethod("POST").SetEndpoint("/api/2.1/unity-catalog/catalogs").SetJSONBody(jsonBody).Build()
}
