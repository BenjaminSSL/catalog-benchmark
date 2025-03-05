package unity

import (
	"benchmark/internal/common"
	"log"
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

	catalogs, err := ListCatalogs(c.context, 100)
	if err != nil {
		return err
	}

	names := make([]string, len(catalogs))
	for i, catalog := range catalogs {
		names[i] = catalog.Name
	}

	log.Printf("Found %d catalog(s)", len(catalogs))

	for _, catalogName := range names {
		request, err := NewDeleteCatalogRequest(c.context, catalogName)
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
