package polaris

type CreateCatalogParams struct {
	Name string
}

func (p CreateCatalogParams) Validate() error {
	return nil
}
