package unity

import (
	"benchmark/internal/common"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

func NewCreateCatalogRequest(ctx context.Context, name string) (*http.Request, error) {
	body := CreateCatalogBody{
		Name: name,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}

	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/catalogs").SetJSONBody(jsonBody).Build(ctx)
}

func NewDeleteCatalogRequest(ctx context.Context, name string) (*http.Request, error) {
	return common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("catalogs/%s", name)).Build(ctx)
}

func NewUpdateCatalogRequest(ctx context.Context, name string, properties map[string]string) (*http.Request, error) {

	body := UpdateCatalogBody{
		Properties: properties,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}

	return common.NewRequestBuilder().SetMethod("PATCH").SetEndpoint(fmt.Sprintf("catalogs/%s", name)).SetJSONBody(jsonBody).Build(ctx)
}

func NewListCatalogsRequest(ctx context.Context, pageToken string, maxResults int) (*http.Request, error) {

	builder := common.NewRequestBuilder().SetMethod("GET").SetEndpoint("/catalogs")

	if pageToken != "" {
		builder.AddQueryParam("page_token", pageToken)
	}
	if maxResults != 0 {
		builder.AddQueryParam("max_results", strconv.Itoa(maxResults))
	}

	return builder.Build(ctx)
}

func NewGetCatalogRequest(ctx context.Context, name string) (*http.Request, error) {
	return common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("/catalogs/%s", name)).Build(ctx)
}
