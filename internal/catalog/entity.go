package catalog

type EntityType string

const (
	Catalog EntityType = "catalog"
	Schema  EntityType = "schema"
	Table   EntityType = "table"
)
