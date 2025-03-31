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
