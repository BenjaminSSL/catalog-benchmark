package unity

type CreateCatalogParams struct {
	Name string
}

type DeleteCatalogParams struct {
	Name string
}

type UpdateCatalogParams struct {
	Name string
}

type CreateSchemaParams struct {
	Name   string
	Prefix string
}
