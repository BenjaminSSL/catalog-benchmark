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

func NewListSchemasRequest(ctx context.Context, catalogName string, pageToken string, maxResults int) *http.Request {

	builder := common.NewRequestBuilder().SetMethod("GET").SetEndpoint("/schemas")

	if pageToken != "" {
		builder.AddQueryParam("page_token", pageToken)
	}
	if maxResults != 0 {
		builder.AddQueryParam("max_results", strconv.Itoa(maxResults))
	}

	builder.AddQueryParam("catalog_name", catalogName)

	return builder.Build(ctx, Host, Path, "")
}

func NewListTablesRequest(ctx context.Context, catalogName string, schemaName string, pageToken string, maxResults int) *http.Request {

	builder := common.NewRequestBuilder().SetMethod("GET").SetEndpoint("/tables")

	if pageToken != "" {
		builder.AddQueryParam("page_token", pageToken)
	}
	if maxResults != 0 {
		builder.AddQueryParam("max_results", strconv.Itoa(maxResults))
	}

	builder.AddQueryParam("catalog_name", catalogName)
	builder.AddQueryParam("schema_name", schemaName)

	return builder.Build(ctx, Host, Path, "")
}

func NewCreateSchemaRequest(ctx context.Context, catalogName string, name string) *http.Request {
	body := CreateNamespaceBody{
		Name:        name,
		CatalogName: catalogName,
		Comment:     "",
		Properties:  map[string]string{},
	}

	jsonBody, _ := json.Marshal(body)

	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/schemas").SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
}

func NewDeleteSchemaRequest(ctx context.Context, catalogName string, schemaName string) *http.Request {
	return common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("/schemas/%s.%s", catalogName, schemaName)).Build(ctx, Host, Path, "")
}

func NewCreateTableRequest(ctx context.Context, name string, catalogName string, schemaName string) *http.Request {
	body := CreateTableBody{
		Name:             name,
		CatalogName:      catalogName,
		SchemaName:       schemaName,
		TableType:        "EXTERNAL",
		DataSourceFormat: "DELTA",
		StorageLocation:  "/",
	}
	jsonBody, _ := json.Marshal(body)

	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/tables").SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
}

func NewDeleteTableRequest(ctx context.Context, catalogName string, schemaName string, tableName string) *http.Request {
	return common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("/tables/%s.%s.%s", catalogName, schemaName, tableName)).Build(ctx, Host, Path, "")
}

func NewGetCatalogRequest(ctx context.Context, name string) *http.Request {
	return common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("/catalogs/%s", name)).Build(ctx, Host, Path, "")
}

func NewGetSchemaRequest(ctx context.Context, catalogName string, tableName string) *http.Request {
	return common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("/schemas/%s.%s", catalogName, tableName)).Build(ctx, Host, Path, "")
}

func NewGetTableRequest(ctx context.Context, catalogName string, schemaName string, tableName string) *http.Request {
	return common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("/tables/%s.%s.%s", catalogName, schemaName, tableName)).Build(ctx, Host, Path, "")

}

func NewUpdateSchemaRequest(ctx context.Context, catalogName string, tableName string, properties map[string]string) *http.Request {
	body := UpdateSchemaBody{
		Properties: properties,
	}

	jsonBody, _ := json.Marshal(body)

	return common.NewRequestBuilder().SetMethod("PATCH").SetEndpoint(fmt.Sprintf("/schemas/%s.%s", catalogName, tableName)).SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
}

func NewCreateFunctionRequest(ctx context.Context, catalogName string, schemaName string, functionName string) *http.Request {
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

	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/functions").SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
}

func NewDeleteFunctionRequest(ctx context.Context, catalogName string, schemaName string, functionName string) *http.Request {
	return common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("/functions/%s.%s.%s", catalogName, schemaName, functionName)).Build(ctx, Host, Path, "")
}

func NewListFunctionsRequest(ctx context.Context, catalogName string, schemaName string, pageToken string, maxResults int) *http.Request {

	builder := common.NewRequestBuilder().SetMethod("GET").SetEndpoint("/functions")

	if pageToken != "" {
		builder.AddQueryParam("page_token", pageToken)
	}
	if maxResults != 0 {
		builder.AddQueryParam("max_results", strconv.Itoa(maxResults))
	}

	builder.AddQueryParam("catalog_name", catalogName)
	builder.AddQueryParam("schema_name", schemaName)

	return builder.Build(ctx, Host, Path, "")
}

func NewCreateModelRequest(ctx context.Context, catalogName string, schemaName string, modelName string) *http.Request {
	body := CreateModelBody{
		Name:        modelName,
		CatalogName: catalogName,
		SchemaName:  schemaName,
	}

	jsonBody, _ := json.Marshal(body)

	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/models").SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
}

func NewDeleteModelRequest(ctx context.Context, catalogName string, schemaName string, modelName string) *http.Request {
	return common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("/models/%s.%s.%s", catalogName, schemaName, modelName)).Build(ctx, Host, Path, "")
}

func NewGetModelRequest(ctx context.Context, catalogName string, schemaName string, modelName string) *http.Request {
	return common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("/models/%s.%s.%s", catalogName, schemaName, modelName)).Build(ctx, Host, Path, "")
}
func NewListModelsRequest(ctx context.Context, catalogName string, schemaName string, pageToken string, maxResults int) *http.Request {
	builder := common.NewRequestBuilder().SetMethod("GET").SetEndpoint("/models")

	if pageToken != "" {
		builder.AddQueryParam("page_token", pageToken)
	}
	if maxResults != 0 {
		builder.AddQueryParam("max_results", strconv.Itoa(maxResults))
	}

	builder.AddQueryParam("catalog_name", catalogName)
	builder.AddQueryParam("schema_name", schemaName)

	return builder.Build(ctx, Host, Path, "")
}

func NewUpdateModelRequest(ctx context.Context, catalogName string, schemaName string, modelName string, comment string) *http.Request {
	body := UpdateModelBody{
		Comment: comment,
	}

	jsonBody, _ := json.Marshal(body)

	return common.NewRequestBuilder().SetMethod("PATCH").SetEndpoint(fmt.Sprintf("/models/%s.%s.%s", catalogName, schemaName, modelName)).SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
}

func NewCreateVolumeRequest(ctx context.Context, catalogName string, schemaName string, volumeName string) *http.Request {
	body := CreateVolumeBody{
		Name:            volumeName,
		CatalogName:     catalogName,
		SchemaName:      schemaName,
		VolumeType:      "EXTERNAL",
		StorageLocation: "FILE",
	}

	jsonBody, _ := json.Marshal(body)

	return common.NewRequestBuilder().SetMethod("POST").SetEndpoint("/volumes").SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
}

func NewUpdateVolumeRequest(ctx context.Context, catalogName string, schemaName string, volumeName string, comment string) *http.Request {
	body := UpdateVolumeBody{
		Comment: comment,
	}

	jsonBody, _ := json.Marshal(body)

	return common.NewRequestBuilder().SetMethod("PATCH").SetEndpoint(fmt.Sprintf("/volumes/%s.%s.%s", catalogName, schemaName, volumeName)).SetJSONBody(jsonBody).Build(ctx, Host, Path, "")
}

func NewDeleteVolumeRequest(ctx context.Context, catalogName string, schemaName string, volumeName string) *http.Request {
	return common.NewRequestBuilder().SetMethod("DELETE").SetEndpoint(fmt.Sprintf("/volumes/%s.%s.%s", catalogName, schemaName, volumeName)).Build(ctx, Host, Path, "")
}

func NewGetVolumeRequest(ctx context.Context, catalogName string, schemaName string, volumeName string) *http.Request {
	return common.NewRequestBuilder().SetMethod("GET").SetEndpoint(fmt.Sprintf("/volumes/%s.%s.%s", catalogName, schemaName, volumeName)).Build(ctx, Host, Path, "")
}
func NewListVolumesRequest(ctx context.Context, catalogName string, schemaName string, pageToken string, maxResults int) *http.Request {
	builder := common.NewRequestBuilder().SetMethod("GET").SetEndpoint("/volumes")

	if pageToken != "" {
		builder.AddQueryParam("page_token", pageToken)
	}
	if maxResults != 0 {
		builder.AddQueryParam("max_results", strconv.Itoa(maxResults))
	}

	builder.AddQueryParam("catalog_name", catalogName)
	builder.AddQueryParam("schema_name", schemaName)

	return builder.Build(ctx, Host, Path, "")
}
