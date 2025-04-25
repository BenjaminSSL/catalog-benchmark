package unity

import (
	"benchmark/internal/common"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

var (
	Host = common.GetEnv("UNITY_HOST", "localhost:8080")
	Path = common.GetEnv("UNITY_PATH", "/api/2.1/unity-catalog")
)

var client = &http.Client{
	Timeout: time.Second * 30,
	Transport: &http.Transport{
		MaxIdleConns:        10000,
		MaxIdleConnsPerHost: 1000,
		MaxConnsPerHost:     1000,
		DisableKeepAlives:   false,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	},
}

type Catalog struct{}

func (c *Catalog) CreateCatalog(ctx context.Context, name string) (*http.Response, error) {
	body := CreateCatalogBody{
		Name: name,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	req, err := common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/catalogs").SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) DeleteCatalog(ctx context.Context, name string) (*http.Response, error) {
	req, err := common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("catalogs/%s", name)).AddQueryParam("force", "true").Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) UpdateCatalog(ctx context.Context, name string, params map[string]interface{}) (*http.Response, error) {
	properties, ok := params["properties"].(map[string]string)
	if !ok {
		properties = make(map[string]string)
	}
	body := UpdateCatalogBody{
		Properties: properties,
	}

	jsonBody, _ := json.Marshal(body)

	req, err := common.NewRequestBuilder().SetMethod("PATCH").SetEndpoint(fmt.Sprintf("catalogs/%s", name)).SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) ListCatalogs(ctx context.Context, params map[string]interface{}) ([]*http.Response, error) {
	pageToken, ok := params["pageToken"].(string)
	if !ok {
		pageToken = ""
	}

	maxResults, ok := params["maxResults"].(int)
	if !ok {
		maxResults = 0
	}

	responses := make([]*http.Response, 0)

	for {
		builder := common.NewRequestBuilder().SetMethod("GET").SetEndpoint("/catalogs")

		if pageToken != "" {
			builder.AddQueryParam("page_token", pageToken)
		}
		if maxResults != 0 {
			builder.AddQueryParam("max_results", strconv.Itoa(maxResults))
		}

		req, err := builder.Build(ctx, Host, Path, "")
		if err != nil {
			return nil, err
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}

		responses = append(responses, resp)

		var body struct {
			NextPageToken string `json:"next_page_token"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return nil, err
		}
		resp.Body.Close()

		if body.NextPageToken == "" {
			break
		}

		pageToken = body.NextPageToken
	}
	return responses, nil
}

func (c *Catalog) ListSchemas(ctx context.Context, catalogName string, params map[string]interface{}) ([]*http.Response, error) {
	pageToken, ok := params["pageToken"].(string)
	if !ok {
		pageToken = ""
	}

	maxResults, ok := params["maxResults"].(int)
	if !ok {
		maxResults = 0
	}

	responses := make([]*http.Response, 0)

	for {

		builder := common.NewRequestBuilder().SetMethod("GET").SetEndpoint("/schemas")

		if pageToken != "" {
			builder.AddQueryParam("page_token", pageToken)
		}
		if maxResults != 0 {
			builder.AddQueryParam("max_results", strconv.Itoa(maxResults))
		}

		builder.AddQueryParam("catalog_name", catalogName)

		req, err := builder.Build(ctx, Host, Path, "")
		if err != nil {
			return nil, err
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}

		responses = append(responses, resp)
		var body struct {
			NextPageToken string `json:"next_page_token"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return nil, err
		}
		resp.Body.Close()

		if body.NextPageToken == "" {
			break
		}

		pageToken = body.NextPageToken
	}
	return responses, nil
}

func (c *Catalog) ListTables(ctx context.Context, catalogName string, schemaName string, params map[string]interface{}) ([]*http.Response, error) {
	pageToken, ok := params["pageToken"].(string)
	if !ok {
		pageToken = ""
	}

	maxResults, ok := params["maxResults"].(int)
	if !ok {
		maxResults = 0
	}

	responses := make([]*http.Response, 0)
	for {
		builder := common.NewRequestBuilder().SetMethod("GET").SetEndpoint("/tables")

		if pageToken != "" {
			builder.AddQueryParam("page_token", pageToken)
		}
		if maxResults != 0 {
			builder.AddQueryParam("max_results", strconv.Itoa(maxResults))
		}

		builder.AddQueryParam("catalog_name", catalogName)
		builder.AddQueryParam("schema_name", schemaName)

		req, err := builder.Build(ctx, Host, Path, "")
		if err != nil {
			return nil, err
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}

		responses = append(responses, resp)

		var body struct {
			NextPageToken string `json:"next_page_token"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return nil, err
		}
		resp.Body.Close()

		if body.NextPageToken == "" {
			break
		}

		pageToken = body.NextPageToken

	}

	return responses, nil
}

func (c *Catalog) CreateSchema(ctx context.Context, catalogName string, name string) (*http.Response, error) {
	body := CreateNamespaceBody{
		Name:        name,
		CatalogName: catalogName,
		Comment:     "",
		Properties:  map[string]string{},
	}

	jsonBody, _ := json.Marshal(body)

	req, err := common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/schemas").SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)

}

func (c *Catalog) DeleteSchema(ctx context.Context, catalogName string, schemaName string) (*http.Response, error) {
	req, err := common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("/schemas/%s.%s", catalogName, schemaName)).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) CreateTable(ctx context.Context, name string, catalogName string, schemaName string) (*http.Response, error) {
	body := CreateTableBody{
		Name:             name,
		CatalogName:      catalogName,
		SchemaName:       schemaName,
		TableType:        "EXTERNAL",
		DataSourceFormat: "DELTA",
		StorageLocation:  "/",
	}
	jsonBody, _ := json.Marshal(body)

	req, err := common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/tables").SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) DeleteTable(ctx context.Context, catalogName string, schemaName string, tableName string) (*http.Response, error) {
	req, err := common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("/tables/%s.%s.%s", catalogName, schemaName, tableName)).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) GetCatalog(ctx context.Context, name string) (*http.Response, error) {
	req, err := common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("/catalogs/%s", name)).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) GetSchema(ctx context.Context, catalogName string, tableName string) (*http.Response, error) {
	req, err := common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("/schemas/%s.%s", catalogName, tableName)).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) GetTable(ctx context.Context, catalogName string, schemaName string, tableName string) (*http.Response, error) {
	req, err := common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("/tables/%s.%s.%s", catalogName, schemaName, tableName)).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) UpdateSchema(ctx context.Context, catalogName string, tableName string, params map[string]interface{}) (*http.Response, error) {
	properties, ok := params["properties"].(map[string]string)
	if !ok {
		properties = make(map[string]string)
	}
	body := UpdateSchemaBody{
		Properties: properties,
	}

	jsonBody, _ := json.Marshal(body)

	req, err := common.NewRequestBuilder().SetMethod("PATCH").SetEndpoint(fmt.Sprintf("/schemas/%s.%s", catalogName, tableName)).SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) CreateFunction(ctx context.Context, catalogName string, schemaName string, functionName string) (*http.Response, error) {
	body := CreateFunctionBody{
		Name:        functionName,
		CatalogName: catalogName,
		SchemaName:  schemaName,
		InputParams: CreateFunctionBodyInputParams{
			Parameters: []CreateFunctionBodyInputParamsParameters{
				{
					Name:     "a",
					TypeText: "int",
					TypeName: "INT",
					TypeJson: "{\\\"name\\\":\\\"a\\\",\\\"type\\\":\\\"integer\\\"}",
					Position: 0,
				},
			},
		},
		DataType:          "INT",
		FullDataType:      "int",
		RoutineBody:       "EXTERNAL",
		RoutineDefinition: "a",
		IsDeterministic:   true,
		SqlDataAccess:     "NO_SQL",
		ParameterStyle:    "S",
		IsNullCall:        false,
		SecurityType:      "DEFINER",
		SpecificName:      functionName,
	}

	wrappedBody := map[string]interface{}{
		"function_info": body,
	}

	jsonBody, _ := json.Marshal(wrappedBody)

	req, err := common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/functions").SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) DeleteFunction(ctx context.Context, catalogName string, schemaName string, functionName string) (*http.Response, error) {
	req, err := common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("/functions/%s.%s.%s", catalogName, schemaName, functionName)).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) ListFunctions(ctx context.Context, catalogName string, schemaName string, params map[string]interface{}) ([]*http.Response, error) {
	pageToken, ok := params["pageToken"].(string)
	if !ok {
		pageToken = ""
	}

	maxResults, ok := params["maxResults"].(int)
	if !ok {
		maxResults = 0
	}

	resposes := make([]*http.Response, 0)
	for {
		builder := common.NewRequestBuilder().SetMethod("GET").SetEndpoint("/functions")

		if pageToken != "" {
			builder.AddQueryParam("page_token", pageToken)
		}
		if maxResults != 0 {
			builder.AddQueryParam("max_results", strconv.Itoa(maxResults))
		}

		builder.AddQueryParam("catalog_name", catalogName)
		builder.AddQueryParam("schema_name", schemaName)

		req, err := builder.Build(ctx, Host, Path, "")
		if err != nil {
			return nil, err
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}

		resposes = append(resposes, resp)

		var body struct {
			NextPageToken string `json:"next_page_token"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return nil, err
		}
		resp.Body.Close()

		if body.NextPageToken == "" {
			break
		}

		pageToken = body.NextPageToken
	}
	return resposes, nil
}

func (c *Catalog) CreateModel(ctx context.Context, catalogName string, schemaName string, modelName string) (*http.Response, error) {
	body := CreateModelBody{
		Name:        modelName,
		CatalogName: catalogName,
		SchemaName:  schemaName,
	}

	jsonBody, _ := json.Marshal(body)

	req, err := common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/models").SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) DeleteModel(ctx context.Context, catalogName string, schemaName string, modelName string) (*http.Response, error) {
	req, err := common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("/models/%s.%s.%s", catalogName, schemaName, modelName)).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) GetModel(ctx context.Context, catalogName string, schemaName string, modelName string) (*http.Response, error) {
	req, err := common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("/models/%s.%s.%s", catalogName, schemaName, modelName)).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}
func (c *Catalog) ListModels(ctx context.Context, catalogName string, schemaName string, params map[string]interface{}) ([]*http.Response, error) {
	pageToken, ok := params["pageToken"].(string)
	if !ok {
		pageToken = ""
	}

	maxResults, ok := params["maxResults"].(int)
	if !ok {
		maxResults = 0
	}

	responses := make([]*http.Response, 0)

	for {
		builder := common.NewRequestBuilder().SetMethod("GET").SetEndpoint("/models")

		if pageToken != "" {
			builder.AddQueryParam("page_token", pageToken)
		}
		if maxResults != 0 {
			builder.AddQueryParam("max_results", strconv.Itoa(maxResults))
		}

		builder.AddQueryParam("catalog_name", catalogName)
		builder.AddQueryParam("schema_name", schemaName)

		req, err := builder.Build(ctx, Host, Path, "")
		if err != nil {
			return nil, err
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}

		responses = append(responses, resp)

		var body struct {
			NextPageToken string `json:"next_page_token"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return nil, err
		}

		resp.Body.Close()

		if body.NextPageToken == "" {
			break
		}

		pageToken = body.NextPageToken
	}
	return responses, nil
}

func (c *Catalog) UpdateModel(ctx context.Context, catalogName string, schemaName string, modelName string, params map[string]interface{}) (*http.Response, error) {
	entityVersion := params["entityVersion"].(int)

	body := UpdateModelBody{
		Comment: strconv.Itoa(entityVersion),
	}

	jsonBody, _ := json.Marshal(body)

	req, err := common.NewRequestBuilder().SetMethod("PATCH").SetEndpoint(fmt.Sprintf("/models/%s.%s.%s", catalogName, schemaName, modelName)).SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) CreateVolume(ctx context.Context, catalogName string, schemaName string, volumeName string) (*http.Response, error) {
	body := CreateVolumeBody{
		Name:            volumeName,
		CatalogName:     catalogName,
		SchemaName:      schemaName,
		VolumeType:      "EXTERNAL",
		StorageLocation: "FILE",
	}

	jsonBody, _ := json.Marshal(body)

	req, err := common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/volumes").SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) UpdateVolume(ctx context.Context, catalogName string, schemaName string, volumeName string, params map[string]interface{}) (*http.Response, error) {
	entityVersion := params["entityVersion"].(int)
	body := UpdateVolumeBody{
		Comment: strconv.Itoa(entityVersion),
	}

	jsonBody, _ := json.Marshal(body)

	req, err := common.NewRequestBuilder().SetMethod("PATCH").SetEndpoint(fmt.Sprintf("/volumes/%s.%s.%s", catalogName, schemaName, volumeName)).SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) DeleteVolume(ctx context.Context, catalogName string, schemaName string, volumeName string) (*http.Response, error) {
	req, err := common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("/volumes/%s.%s.%s", catalogName, schemaName, volumeName)).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) GetVolume(ctx context.Context, catalogName string, schemaName string, volumeName string) (*http.Response, error) {
	req, err := common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("/volumes/%s.%s.%s", catalogName, schemaName, volumeName)).Build(ctx, Host, Path, "")
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}
func (c *Catalog) ListVolumes(ctx context.Context, catalogName string, schemaName string, params map[string]interface{}) ([]*http.Response, error) {
	pageToken, ok := params["pageToken"].(string)
	if !ok {
		pageToken = ""
	}

	maxResults, ok := params["maxResults"].(int)
	if !ok {
		maxResults = 0
	}

	responses := make([]*http.Response, 0)

	for {
		builder := common.NewRequestBuilder().SetMethod("GET").SetEndpoint("/volumes")

		if pageToken != "" {
			builder.AddQueryParam("page_token", pageToken)
		}
		if maxResults != 0 {
			builder.AddQueryParam("max_results", strconv.Itoa(maxResults))
		}

		builder.AddQueryParam("catalog_name", catalogName)
		builder.AddQueryParam("schema_name", schemaName)

		req, err := builder.Build(ctx, Host, Path, "")
		if err != nil {
			return nil, err
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}

		responses = append(responses, resp)

		var body struct {
			NextPageToken string `json:"next_page_token"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return nil, err
		}
		resp.Body.Close()

		if body.NextPageToken == "" {
			break
		}

		pageToken = body.NextPageToken
	}

	return responses, nil
}

func (c *Catalog) CreatePrincipal(ctx context.Context, name string) (*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) GetPrincipal(ctx context.Context, name string) (*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) UpdatePrincipal(ctx context.Context, name string, params map[string]interface{}) (*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) DeletePrincipal(ctx context.Context, name string) (*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) ListPrincipals(ctx context.Context, params map[string]interface{}) ([]*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) CreateView(ctx context.Context, catalogName string, schemaName string, viewName string) (*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) GetView(ctx context.Context, catalogName string, schemaName string, viewName string) (*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) UpdateView(ctx context.Context, catalogName string, schemaName string, viewName string, params map[string]interface{}) (*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) DeleteView(ctx context.Context, catalogName string, schemaName string, viewName string) (*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) ListViews(ctx context.Context, catalogName string, schemaName string, params map[string]interface{}) ([]*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (c *Catalog) UpdateTable(ctx context.Context, catalogName string, schemaName string, tableName string, params map[string]interface{}) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (c *Catalog) GetFunction(ctx context.Context, catalogName string, schemaName string, functionName string) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (c *Catalog) GrantPermissionCatalog(ctx context.Context, catalogName string, params map[string]interface{}) (*http.Response, error) {
	return nil, errors.New("not implemented")

}
