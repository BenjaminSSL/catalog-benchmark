package unity

import (
	"benchmark/internal/common"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

var (
	Host = common.GetEnv("UNITY_HOST", "localhost:8080")
	Path = common.GetEnv("UNITY_PATH", "/api/2.1/unity-catalog")
)

func NewCreateCatalogRequest(ctx context.Context, name string) *http.Request {
	body := CreateCatalogBody{
		Name: name,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		panic(err)
	}

	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/catalogs").SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
}

func NewDeleteCatalogRequest(ctx context.Context, name string) *http.Request {
	return common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("catalogs/%s", name)).AddQueryParam("force", "true").Build(ctx, Host, Path, "")
}

func NewUpdateCatalogRequest(ctx context.Context, name string, properties map[string]string) *http.Request {

	body := UpdateCatalogBody{
		Properties: properties,
	}

	jsonBody, _ := json.Marshal(body)

	return common.NewRequestBuilder().SetMethod("PATCH").SetEndpoint(fmt.Sprintf("catalogs/%s", name)).SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
}

func NewListCatalogsRequest(ctx context.Context, pageToken string, maxResults int) *http.Request {

	builder := common.NewRequestBuilder().SetMethod("GET").SetEndpoint("/catalogs")

	if pageToken != "" {
		builder.AddQueryParam("page_token", pageToken)
	}
	if maxResults != 0 {
		builder.AddQueryParam("max_results", strconv.Itoa(maxResults))
	}

	return builder.Build(ctx, Host, Path, "")
}

func NewGetCatalogRequest(ctx context.Context, name string) *http.Request {
	return common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("/catalogs/%s", name)).Build(ctx, Host, Path, "")
}

func NewCreateSchemaRequest(ctx context.Context, name string, catalogName string) *http.Request {
	body := CreateNamespaceBody{
		Name:        name,
		CatalogName: catalogName,
		Comment:     "",
		Properties:  map[string]string{},
	}

	jsonBody, _ := json.Marshal(body)

	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/schemas").SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
}
