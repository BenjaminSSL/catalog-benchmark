package polaris

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

type CreateCatalogBody struct {
	Catalog Catalog `json:"catalog"`
}

type CreateSchemaBody struct{}

type UpdateCatalogBody struct {
	CurrentEntityVersion int                      `json:"currentEntityVersion"`
	Properties           CatalogProperties        `json:"properties"`
	StorageConfigInfo    CatalogStorageConfigInfo `json:"storageConfigInfo"`
}

type ListCatalogsResponse struct {
	Catalogs []Catalog `json:"catalogs"`
}
