package cleaner

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/catalog/unity"
	"benchmark/internal/common"
	"net/http"
)

type CatalogCleaner struct {
	context common.RequestContext
	client  *http.Client
	catalog string
}

func NewCatalogCleaner(context common.RequestContext, catalog string) *CatalogCleaner {
	return &CatalogCleaner{context: context, client: &http.Client{}, catalog: catalog}
}

func (c *CatalogCleaner) CleanCatalog() error {
	var ids []string
	switch c.catalog {
	case "polaris":
		catalogs, err := polaris.ListCatalogs(c.context)
		if err != nil {
			return err
		}
		for _, catalog := range catalogs {
			ids = append(ids, catalog.Name)
		}
	case "unity":
		catalogs, err := unity.ListCatalogs(c.context, 0)
		if err != nil {
			return err
		}
		for _, catalog := range catalogs {
			ids = append(ids, catalog.Name)
		}
	}

	for _, id := range ids {
		var deleteCatalogRequest *http.Request
		var err error

		switch c.catalog {
		case "polaris":
			deleteCatalogRequest, err = polaris.NewDeleteCatalogRequest(c.context, id)
		case "unity":
			deleteCatalogRequest, err = unity.NewDeleteCatalogRequest(c.context, id)
		}
		if err != nil {
			return err
		}

		_, err = c.client.Do(deleteCatalogRequest)
		if err != nil {
			return err
		}
	}
	return nil
}
