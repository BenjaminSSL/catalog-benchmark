package unity

import (
	"benchmark/internal/common"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
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

func NewDeleteCatalogRequest(context common.RequestContext, params DeleteCatalogParams) (*http.Request, error) {
	endpoint := fmt.Sprintf("/api/2.1/unity-catalog/catalogs/%s", params.Name)
	return common.NewRequestBuilder(context).SetMethod("DELETE").SetEndpoint(endpoint).Build()
}

func NewUpdateCatalogRequest(context common.RequestContext, params UpdateCatalogParams) (*http.Request, error) {
	endpoint := fmt.Sprintf("/api/2.1/unity-catalog/catalogs/%s", params.Name)

	body := UpdateCatalogBody{
		Comment: strconv.Itoa(rand.IntN(100)),
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}

	return common.NewRequestBuilder(context).SetMethod("PATCH").SetEndpoint(endpoint).SetJSONBody(jsonBody).Build()
}

func NewListCatalogsRequest(context common.RequestContext, params ListCatalogsParams) (*http.Request, error) {

	builder := common.NewRequestBuilder(context).SetMethod("GET").SetEndpoint("/api/2.1/unity-catalog/catalogs")

	if params.PageToken != "" {
		builder.AddQueryParam("page_token", params.PageToken)
	}
	if params.MaxResults != 0 {
		builder.AddQueryParam("max_results", strconv.Itoa(params.MaxResults))
	}

	return builder.Build()
}
