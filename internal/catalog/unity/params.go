package unity

type CreateCatalogParams struct {
	Name string
}

type DeleteCatalogParams struct {
	Name string
}

type UpdateCatalogParams struct {
	Name    string
	Version int
}

type CreateSchemaParams struct {
	Name   string
	Prefix string
}
