package polaris

import (
	"benchmark/internal/common"
	"net/http"
)

type CatalogCleaner struct {
	context common.RequestContext
	client  *http.Client
}

func NewCleaner(context common.RequestContext) *CatalogCleaner {
	return &CatalogCleaner{context: context, client: &http.Client{}}
}

func (c *CatalogCleaner) CleanCatalog() error {
	catalogs, err := ListCatalogs(c.context)
	if err != nil {
		return err
	}

	names := make([]string, len(catalogs))
	for i, catalog := range catalogs {
		names[i] = catalog.Name
	}

	for _, catalogName := range names {
		deleteParams := DeleteCatalogParams{
			Name: catalogName,
		}
		request, err := NewDeleteCatalogRequest(c.context, deleteParams)
		if err != nil {
			return err
		}

		_, err = c.client.Do(request)
		if err != nil {
			return err
		}

	}

	return nil

}
