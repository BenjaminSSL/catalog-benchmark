package unity

type Catalog struct {
	Name       string            `json:"name"`
	Comment    string            `json:"comment"`
	Properties map[string]string `json:"properties"`
	Owner      interface{}       `json:"owner"`
	CreatedAt  int64             `json:"created_at"`
	CreatedBy  interface{}       `json:"created_by"`
	UpdatedAt  int64             `json:"updated_at"`
	UpdatedBy  interface{}       `json:"updated_by"`
	Id         string            `json:"id"`
}
type Schema struct {
	Name        string            `json:"name"`
	CatalogName string            `json:"catalog_name"`
	Comment     interface{}       `json:"comment"`
	Properties  map[string]string `json:"properties"`
	FullName    string            `json:"full_name"`
	Owner       interface{}       `json:"owner"`
	CreatedAt   int64             `json:"created_at"`
	CreatedBy   interface{}       `json:"created_by"`
	UpdatedAt   int64             `json:"updated_at"`
	UpdatedBy   interface{}       `json:"updated_by"`
	SchemaId    string            `json:"schema_id"`
}

type Table struct {
	Name             string        `json:"name"`
	CatalogName      string        `json:"catalog_name"`
	SchemaName       string        `json:"schema_name"`
	TableType        string        `json:"table_type"`
	DataSourceFormat string        `json:"data_source_format"`
	Columns          []interface{} `json:"columns"`
	StorageLocation  string        `json:"storage_location"`
	Comment          interface{}   `json:"comment"`
	Properties       struct {
	} `json:"properties"`
	Owner     interface{} `json:"owner"`
	CreatedAt int64       `json:"created_at"`
	CreatedBy interface{} `json:"created_by"`
	UpdatedAt int64       `json:"updated_at"`
	UpdatedBy interface{} `json:"updated_by"`
	TableId   string      `json:"table_id"`
}
type ListCatalogsResponse struct {
	Catalogs      []Catalog `json:"catalogs"`
	NextPageToken string    `json:"next_page_token"`
}
type ListSchemasResponse struct {
	Schemas       []Schema `json:"schemas"`
	NextPageToken string   `json:"next_page_token"`
}
type ListTablesResponse struct {
	Tables        []Table `json:"tables"`
	NextPageToken string  `json:"next_page_token"`
}

type ListFunctionsResponse struct {
	Functions     []interface{} `json:"functions"`
	NextPageToken string        `json:"next_page_token"`
}

type ListModelsResponse struct {
	Models        []interface{} `json:"models"`
	NextPageToken string        `json:"next_page_token"`
}

type ListVolumesResponse struct {
	Volumes       []interface{} `json:"volumes"`
	NextPageToken string        `json:"next_page_token"`
}

type CreateCatalogBody struct {
	Name       string            `json:"name"`
	Comment    string            `json:"comment,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
}

type UpdateCatalogBody struct {
	Comment    string            `json:"comment,omitempty"`
	Properties map[string]string `json:"properties,omitempty"`
	NewName    string            `json:"new_name,omitempty"`
}

type CreateNamespaceBody struct {
	Name        string            `json:"name"`
	CatalogName string            `json:"catalog_name"`
	Comment     string            `json:"comment"`
	Properties  map[string]string `json:"properties"`
}

type CreateTableBody struct {
	Name             string `json:"name"`
	CatalogName      string `json:"catalog_name"`
	SchemaName       string `json:"schema_name"`
	TableType        string `json:"table_type"`
	DataSourceFormat string `json:"data_source_format"`
	StorageLocation  string `json:"storage_location"`
}

type UpdateSchemaBody struct {
	NewName    string            `json:"new_name"`
	Properties map[string]string `json:"properties"`
	Comment    string            `json:"comment,omitempty"`
}

type CreateFunctionBody struct {
	Name                string `json:"name"`
	CatalogName         string `json:"catalog_name"`
	SchemaName          string `json:"schema_name"`
	InputParams         CreateFunctionBodyInputParams
	DataType            string      `json:"data_type"`
	FullDataType        string      `json:"full_data_type"`
	ReturnParams        interface{} `json:"return_params,omitempty"`
	RoutineBody         string      `json:"routine_body"`
	RoutineDefinition   string      `json:"routine_definition"`
	RoutineDependencies interface{} `json:"routine_dependencies,omitempty"`
	ParameterStyle      string      `json:"parameter_style"`
	IsDeterministic     bool        `json:"is_deterministic"`
	SqlDataAccess       string      `json:"sql_data_access"`
	IsNullCall          bool        `json:"is_null_call"`
	SecurityType        string      `json:"security_type"`
	SpecificName        string      `json:"specific_name"`
	Comment             interface{} `json:"comment,omitempty"`
	Properties          string      `json:"properties,omitempty"`
	ExternalLanguage    string      `json:"external_language,omitempty"`
}

type CreateFunctionBodyInputParams struct {
	Parameters []CreateFunctionBodyInputParamsParameters `json:"parameters"`
}

type CreateFunctionBodyInputParamsParameters struct {
	Name     string `json:"name"`
	TypeText string `json:"type_text"`
	TypeJson string `json:"type_json"`
	TypeName string `json:"type_name"`
	Position int    `json:"position"`
}

type CreateModelBody struct {
	Name        string `json:"name"`
	CatalogName string `json:"catalog_name"`
	SchemaName  string `json:"schema_name"`
	Comment     string `json:"comment,omitempty"`
}

type UpdateModelBody struct {
	NewName string `json:"new_name"`
	Comment string `json:"comment,omitempty"`
}

type CreateVolumeBody struct {
	Name            string `json:"name"`
	CatalogName     string `json:"catalog_name"`
	SchemaName      string `json:"schema_name"`
	VolumeType      string `json:"volume_type"`
	StorageLocation string `json:"storage_location"`
}

type UpdateVolumeBody struct {
	NewName string `json:"new_name"`
	Comment string `json:"comment,omitempty"`
}
