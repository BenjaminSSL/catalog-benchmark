package polaris

import (
	"benchmark/internal/common"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"
)

var (
	Host           = common.GetEnv("POLARIS_HOST", "localhost:8181")
	PathManagement = common.GetEnv("POLARIS_PATH_MANAGEMENT", "/api/management/v1")
	PathCatalog    = common.GetEnv("POLARIS_PATH_CATALOG", "/api/catalog/v1")
)

var Token string

func SetToken(token string) {
	Token = token
}

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
	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
	body := CreateCatalogBody{
		Catalog: CatalogModel{
			EntityType: "INTERNAL",
			Name:       name,
			Properties: CatalogProperties{
				DefaultBaseLocation: fmt.Sprintf("file:///tmp/%s/", name),
			},
			StorageConfigInfo: CatalogStorageConfigInfo{
				StorageType: "FILE",
			},
		},
	}

	jsonBody, err := common.MarshalJSON(body)
	if err != nil {
		return nil, err
	}
	req, err := common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/catalogs").SetJSONBody(jsonBody).Build(ctx, Host, PathManagement, Token)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) GetCatalog(ctx context.Context, name string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
	req, err := common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("/catalogs/%s", name)).Build(ctx, Host, PathManagement, Token)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) DeleteCatalog(ctx context.Context, name string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()
	req, err := common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("/catalogs/%s", name)).Build(ctx, Host, PathManagement, Token)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) ListCatalogs(ctx context.Context, params map[string]interface{}) ([]*http.Response, error) {
	req, err := common.NewRequestBuilder().SetEndpoint("/catalogs").Build(ctx, Host, PathManagement, Token)
	if err != nil {
		return nil, err
	}

	responses := make([]*http.Response, 0)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	responses = append(responses, resp)
	return responses, nil
}

func (c *Catalog) GetPrincipal(ctx context.Context, name string) (*http.Response, error) {
	req, err := common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("/principals/%s", name)).Build(ctx, Host, PathManagement, Token)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) GetSchema(ctx context.Context, catalogName string, schemaName string) (*http.Response, error) {
	req, err := common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("%s/namespaces/%s", catalogName, schemaName)).Build(ctx, Host, PathCatalog, Token)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) GetTable(ctx context.Context, catalogName string, schemaName string, name string) (*http.Response, error) {
	req, err := common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/tables/%s", catalogName, schemaName, name)).Build(ctx, Host, PathCatalog, Token)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) UpdateCatalog(ctx context.Context, name string, params map[string]interface{}) (*http.Response, error) {
	entityVersion := params["entityVersion"].(int)

	var catalogProperties = CatalogProperties{
		AdditionalProps: map[string]string{
			"entityVersion": strconv.Itoa(entityVersion),
		},
	}

	body := UpdateCatalogBody{
		CurrentEntityVersion: entityVersion,
		Properties:           catalogProperties,
		StorageConfigInfo: CatalogStorageConfigInfo{
			StorageType: "FILE",
		},
	}

	jsonBody, err := common.MarshalJSON(body)
	if err != nil {
		return nil, err
	}

	req, err := common.NewRequestBuilder().SetMethod("PUT").SetEndpoint(fmt.Sprintf("/catalogs/%s", name)).SetJSONBody(jsonBody).Build(ctx, Host, PathManagement, Token)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) UpdatePrincipal(ctx context.Context, name string, params map[string]interface{}) (*http.Response, error) {
	entityVersion := params["entityVersion"].(int)

	body := UpdatePrincipalBody{
		CurrentEntityVersion: entityVersion,
		Properties: map[string]string{
			"entityVersion": strconv.Itoa(entityVersion),
		},
	}

	jsonBody, err := common.MarshalJSON(body)
	if err != nil {
		return nil, err
	}

	req, err := common.NewRequestBuilder().SetMethod("PUT").SetEndpoint(fmt.Sprintf("/principals/%s", name)).SetJSONBody(jsonBody).Build(ctx, Host, PathManagement, Token)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) UpdateSchema(ctx context.Context, catalogName string, schemaName string, params map[string]interface{}) (*http.Response, error) {
	entityVersion := params["entityVersion"].(int)

	body := UpdateNamespaceBody{
		Updates: map[string]string{
			"entityVersion": strconv.Itoa(entityVersion),
		},
	}

	jsonBody, err := common.MarshalJSON(body)
	if err != nil {
		return nil, err
	}
	req, err := common.NewRequestBuilder().SetMethod("POST").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/properties", catalogName, schemaName)).SetJSONBody(jsonBody).Build(ctx, Host, PathCatalog, Token)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) UpdateTable(ctx context.Context, catalogName string, schemaName string, tableName string, params map[string]interface{}) (*http.Response, error) {
	entityVersion := params["entityVersion"].(int)
	body := UpdateTableBody{
		Updates: []map[string]interface{}{
			{
				"action": "set-properties",
				"updates": map[string]string{
					"entityVersion": strconv.Itoa(entityVersion),
				},
			},
		},
	}

	jsonBody, err := common.MarshalJSON(body)
	if err != nil {
		return nil, err
	}

	req, err := common.NewRequestBuilder().SetMethod("POST").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/tables/%s", catalogName, schemaName, tableName)).SetJSONBody(jsonBody).Build(ctx, Host, PathCatalog, Token)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) ListPrincipals(ctx context.Context, params map[string]interface{}) ([]*http.Response, error) {
	req, err := common.NewRequestBuilder().SetEndpoint(fmt.Sprintf("/principals")).Build(ctx, Host, PathManagement, Token)
	if err != nil {
		return nil, err
	}

	responses := make([]*http.Response, 0)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	responses = append(responses, resp)
	return responses, nil
}

func (c *Catalog) CreatePrincipal(ctx context.Context, name string) (*http.Response, error) {
	body := CreatePrincipalBody{
		Principal: Principal{
			Name: name,
		},
		CredentialRotationRequired: false,
	}

	jsonBody, err := common.MarshalJSON(body)
	if err != nil {
		return nil, err
	}

	req, err := common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/principals").SetJSONBody(jsonBody).Build(ctx, Host, PathManagement, Token)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) DeletePrincipal(ctx context.Context, name string) (*http.Response, error) {
	req, err := common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("/principals/%s", name)).Build(ctx, Host, PathManagement, Token)

	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) DeleteSchema(ctx context.Context, catalogName string, schemaName string) (*http.Response, error) {
	req, err := common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("%s/namespaces/%s", catalogName, schemaName)).Build(ctx, Host, PathCatalog, Token)

	if err != nil {
		return nil, err
	}
	return client.Do(req)
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
		builder := common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("%s/namespaces", catalogName))

		if pageToken != "" {
			builder.AddQueryParam("pageToken", pageToken)
		}
		if maxResults != 0 {
			builder.AddQueryParam("pageSize", strconv.Itoa(maxResults))
		}

		req, err := builder.Build(ctx, Host, PathCatalog, Token)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}

		responses = append(responses, resp)

		var body struct {
			NextPageToken string `json:"nextPageToken"`
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

func (c *Catalog) CreateSchema(ctx context.Context, catalogName string, schemaName string) (*http.Response, error) {
	body := CreateNamespaceBody{
		Namespace:  []string{schemaName},
		Properties: map[string]string{},
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}

	req, err := common.NewRequestBuilder().SetMethod("POST").SetEndpoint(fmt.Sprintf("%s/namespaces", catalogName)).SetJSONBody(jsonBody).Build(ctx, Host, PathCatalog, Token)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) CreateTable(ctx context.Context, catalogName string, schemaName string, tableName string) (*http.Response, error) {
	body := CreateTableBody{
		Name: tableName,
		Schema: TableSchema{
			Type:   "struct",
			Fields: make([]interface{}, 0),
		},
		StageCreate: false,
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}

	req, err := common.NewRequestBuilder().SetMethod("POST").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/tables", catalogName, schemaName)).SetJSONBody(jsonBody).Build(ctx, Host, PathCatalog, Token)

	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) DeleteTable(ctx context.Context, catalogName string, schemaName string, tableName string) (*http.Response, error) {
	req, err := common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/tables/%s", catalogName, schemaName, tableName)).Build(ctx, Host, PathCatalog, Token)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) GrantPermissionCatalog(ctx context.Context, catalogName string, params map[string]interface{}) (*http.Response, error) {
	privilege := params["privilege"].(string)
	body := GrantCatalogPermissionBody{
		Grants: GrantPrivilege{
			Privilege: privilege,
			Type:      "catalog",
		},
	}

	jsonBody, err := common.MarshalJSON(body)
	if err != nil {
		return nil, err
	}

	req, err := common.NewRequestBuilder().SetMethod("PUT").SetEndpoint(fmt.Sprintf("catalogs/%s/catalog-roles/catalog_admin/grants", catalogName)).SetJSONBody(jsonBody).Build(ctx, Host, PathManagement, Token)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
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
		builder := common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/tables", catalogName, schemaName))

		if pageToken != "" {
			builder.AddQueryParam("pageToken", pageToken)
		}
		if maxResults != 0 {
			builder.AddQueryParam("pageSize", strconv.Itoa(maxResults))
		}

		req, err := builder.Build(ctx, Host, PathCatalog, Token)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}

		responses = append(responses, resp)
		var result struct {
			NextPageToken string `json:"nextPageToken"`
		}

		body, _ := io.ReadAll(resp.Body)
		err = json.Unmarshal(body, &result)
		if err != nil {
			return nil, err
		}
		resp.Body.Close()
		if result.NextPageToken == "" {
			break
		}

		pageToken = result.NextPageToken
	}

	return responses, nil
}

func (c *Catalog) CreateView(ctx context.Context, catalogName string, schemaName string, viewName string) (*http.Response, error) {
	body := CreateViewBody{
		Name:     viewName,
		Location: fmt.Sprintf("file:///tmp/%s/%s/", catalogName, schemaName),
		Schema: ViewBodySchema{
			Type:   "struct",
			Fields: []interface{}{},
		},
		ViewVersion: ViewBodyViewVersion{
			VersionId:   0,
			TimestampMs: 0,
			SchemaId:    0,
			Summary:     map[string]string{},
			Representations: []ViewBodyViewVersionRepresentation{{
				Type:    "sql",
				Sql:     "SELECT 1 AS test_column",
				Dialect: "ansi",
			}},
			DefaultCatalog: catalogName,
			DefaultNamespace: []string{
				schemaName,
			},
		},
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}

	req, err := common.NewRequestBuilder().SetMethod("POST").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/views", catalogName, schemaName)).SetJSONBody(jsonBody).Build(ctx, Host, PathCatalog, Token)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) DeleteView(ctx context.Context, catalogName string, schemaName string, viewName string) (*http.Response, error) {
	req, err := common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/views/%s", catalogName, schemaName, viewName)).Build(ctx, Host, PathCatalog, Token)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) GetView(ctx context.Context, catalogName string, schemaName string, viewName string) (*http.Response, error) {
	req, err := common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/views/%s", catalogName, schemaName, viewName)).Build(ctx, Host, PathCatalog, Token)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) ListViews(ctx context.Context, catalogName string, schemaName string, params map[string]interface{}) ([]*http.Response, error) {
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

		builder := common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/views", catalogName, schemaName))

		if pageToken != "" {
			builder.AddQueryParam("pageToken", pageToken)
		}
		if maxResults != 0 {
			builder.AddQueryParam("pageSize", strconv.Itoa(maxResults))
		}
		req, err := builder.Build(ctx, Host, PathCatalog, Token)
		if err != nil {
			return nil, err
		}

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responses = append(responses, resp)

		var body struct {
			NextPageToken string `json:"nextPageToken"`
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

func (c *Catalog) UpdateView(ctx context.Context, catalogName string, schemaName string, viewName string, params map[string]interface{}) (*http.Response, error) {
	properties, ok := params["properties"].(map[string]string)
	if !ok {
		properties = make(map[string]string)
	}
	body := UpdateViewBody{
		Updates: []map[string]interface{}{
			{
				"action":  "set-properties",
				"updates": properties,
			},
		},
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		log.Fatalf("Failed to marshal JSON: %v", err)
	}

	req, err := common.NewRequestBuilder().SetMethod("POST").SetEndpoint(fmt.Sprintf("%s/namespaces/%s/views/%s", catalogName, schemaName, viewName)).SetJSONBody(jsonBody).Build(ctx, Host, PathCatalog, Token)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (c *Catalog) CreateFunction(ctx context.Context, catalogName string, schemaName string, functionName string) (*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) GetFunction(ctx context.Context, catalogName string, schemaName string, functionName string) (*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) DeleteFunction(ctx context.Context, catalogName string, schemaName string, functionName string) (*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) ListFunctions(ctx context.Context, catalogName string, schemaName string, params map[string]interface{}) ([]*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) CreateModel(ctx context.Context, catalogName string, schemaName string, modelName string) (*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) GetModel(ctx context.Context, catalogName string, schemaName string, modelName string) (*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) UpdateModel(ctx context.Context, catalogName string, schemaName string, modelName string, params map[string]interface{}) (*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) DeleteModel(ctx context.Context, catalogName string, schemaName string, modelName string) (*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) ListModels(ctx context.Context, catalogName string, schemaName string, params map[string]interface{}) ([]*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) CreateVolume(ctx context.Context, catalogName string, schemaName string, volumeName string) (*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) GetVolume(ctx context.Context, catalogName string, schemaName string, volumeName string) (*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) UpdateVolume(ctx context.Context, catalogName string, schemaName string, volumeName string, params map[string]interface{}) (*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) DeleteVolume(ctx context.Context, catalogName string, schemaName string, volumeName string) (*http.Response, error) {
	return nil, errors.New("not implemented")
}
func (c *Catalog) ListVolumes(ctx context.Context, catalogName string, schemaName string, params map[string]interface{}) ([]*http.Response, error) {
	return nil, errors.New("not implemented")
}
