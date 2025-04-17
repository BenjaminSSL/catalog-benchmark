package polaris

// Polaris Schemas

type CatalogStorageConfigInfo struct {
	StorageType      string   `json:"storageType"`
	AllowedLocations []string `json:"allowedLocations,omitempty"`
}
type CatalogProperties struct {
	DefaultBaseLocation string            `json:"default-base-location"`
	AdditionalProps     map[string]string `json:"-"`
}
type Catalog struct {
	EntityType          string                   `json:"type"`
	Name                string                   `json:"name"`
	Properties          CatalogProperties        `json:"properties"`
	CreateTimestamp     int64                    `json:"createTimestamp,omitempty"`
	LastUpdateTimestamp int64                    `json:"lastUpdateTimestamp,omitempty"`
	EntityVersion       int                      `json:"entityVersion,omitempty"`
	StorageConfigInfo   CatalogStorageConfigInfo `json:"storageConfigInfo"`
}

type Principal struct {
	Name                string            `json:"name"`
	ClientID            string            `json:"clientId"`
	Properties          map[string]string `json:"properties"`
	CreateTimestamp     int64             `json:"createTimestamp"`
	LastUpdateTimestamp int64             `json:"lastUpdateTimestamp"`
	EntityVersion       int               `json:"entityVersion"`
}

type TableSchema struct {
	Type   string        `json:"type"`
	Fields []interface{} `json:"fields"`
}

// Request Bodies

type CreateCatalogBody struct {
	Catalog Catalog `json:"catalog"`
}

type CreateNamespaceBody struct {
	Namespace  []string          `json:"namespace"`
	Properties map[string]string `json:"properties"`
}

type CreateSchemaBody struct{}

type UpdateCatalogBody struct {
	CurrentEntityVersion int                      `json:"currentEntityVersion"`
	Properties           CatalogProperties        `json:"properties"`
	StorageConfigInfo    CatalogStorageConfigInfo `json:"storageConfigInfo"`
}

type UpdatePrincipalBody struct {
	CurrentEntityVersion int               `json:"currentEntityVersion"`
	Properties           map[string]string `json:"properties"`
}

type UpdateNamespaceBody struct {
	Updates map[string]string `json:"updates"`
}
type UpdateTableBody struct {
	Identifier   []map[string]interface{} `json:"identifier,omitempty"`
	Requirements []map[string]interface{} `json:"requirements,omitempty"`
	Updates      []map[string]interface{} `json:"updates,omitempty"`
}

type CreateTableBody struct {
	Name        string            `json:"name"`
	Schema      TableSchema       `json:"schema"`
	StageCreate bool              `json:"stage-create"`
	Properties  map[string]string `json:"properties"`
}

type GrantPrivilege struct {
	Privilege string `json:"privilege"`
	Type      string `json:"type"`
}

// Responses

type ListCatalogsResponse struct {
	Catalogs []Catalog `json:"catalogs"`
}

type ListNamespacesResponse struct {
	Namespaces    [][]string `json:"namespaces"`
	NextPageToken string     `json:"next-page-token"`
}

type CreatePrincipalBody struct {
	Principal                  Principal `json:"principal"`
	CredentialRotationRequired bool      `json:"credentialRotationRequired"`
}
type GrantCatalogPermissionBody struct {
	Grants GrantPrivilege `json:"grant"`
}

type ListTablesResponse struct {
	Identifiers   []map[string]interface{} `json:"identifiers"`
	NextPageToken string                   `json:"next-page-token"`
}

type UpdateViewBody struct {
	Identifier   []map[string]interface{} `json:"identifier,omitempty"`
	Requirements []map[string]interface{} `json:"requirements,omitempty"`
	Updates      []map[string]interface{} `json:"updates,omitempty"`
}
type CreateViewBody struct {
	Name        string              `json:"name"`
	Location    string              `json:"location"`
	Schema      ViewBodySchema      `json:"schema"`
	ViewVersion ViewBodyViewVersion `json:"view-version"`
}
type ViewBodySchema struct {
	Type   string        `json:"type"`
	Fields []interface{} `json:"fields"`
}
type ViewBodyViewVersion struct {
	VersionId        int                                 `json:"version-id"`
	TimestampMs      int                                 `json:"timestamp-ms"`
	SchemaId         int                                 `json:"schema-id"`
	Summary          map[string]string                   `json:"summary"`
	Representations  []ViewBodyViewVersionRepresentation `json:"representations"`
	DefaultCatalog   string                              `json:"default-catalog"`
	DefaultNamespace []string                            `json:"default-namespace"`
}

type ViewBodyViewVersionRepresentation struct {
	Type    string `json:"type"`
	Sql     string `json:"sql"`
	Dialect string `json:"dialect"`
}
