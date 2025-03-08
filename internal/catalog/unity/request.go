package unity

import (
	"benchmark/internal/common"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
)

func NewCreateCatalogRequest(context common.RequestContext, name string) (*http.Request, error) {
	body := CreateCatalogBody{
		Name: name,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}

	return common.NewRequestBuilder(context).SetMethod("POST").SetEndpoint("/catalogs").SetJSONBody(jsonBody).Build()
}

func NewDeleteCatalogRequest(context common.RequestContext, name string) (*http.Request, error) {
	return common.NewRequestBuilder(context).SetMethod("DELETE").SetEndpoint(fmt.Sprintf("catalogs/%s", name)).Build()
}

func NewUpdateCatalogRequest(context common.RequestContext, name string) (*http.Request, error) {

	body := UpdateCatalogBody{
		Comment: strconv.Itoa(rand.IntN(100)),
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}

	return common.NewRequestBuilder(context).SetMethod("PATCH").SetEndpoint(fmt.Sprintf("catalogs/%s", name)).SetJSONBody(jsonBody).Build()
}

func NewListCatalogsRequest(context common.RequestContext, pageToken string, maxResults int) (*http.Request, error) {

	builder := common.NewRequestBuilder(context).SetMethod("GET").SetEndpoint("/catalogs")

	if pageToken != "" {
		builder.AddQueryParam("page_token", pageToken)
	}
	if maxResults != 0 {
		builder.AddQueryParam("max_results", strconv.Itoa(maxResults))
	}

	return builder.Build()
}
