package cleaner

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/catalog/unity"
	"benchmark/internal/common"
	"context"
	"log"
	"net/http"
)

type CatalogCleaner struct {
	catalog string
}

func NewCatalogCleaner(catalog string) *CatalogCleaner {
	return &CatalogCleaner{catalog: catalog}
}

func (c *CatalogCleaner) CleanCatalog(ctx context.Context) error {
	var ids []string
	progressBar := common.NewProgressBar(len(ids))

	switch c.catalog {
	case "polaris":
		catalogs, err := polaris.ListCatalogs(ctx)
		if err != nil {
			return err
		}
		for _, catalog := range catalogs {
			ids = append(ids, catalog.Name)
		}
	case "unity":
		catalogs, err := unity.ListCatalogs(ctx, 1000)
		if err != nil {
			return err
		}
		for _, catalog := range catalogs {
			ids = append(ids, catalog.Name)
		}
	}

	log.Printf("Removing %d catalogs", len(ids))

	for _, id := range ids {
		var deleteCatalogRequest *http.Request
		var err error

		switch c.catalog {
		case "polaris":
			deleteCatalogRequest, err = polaris.NewDeleteCatalogRequest(ctx, id)
		case "unity":
			deleteCatalogRequest, err = unity.NewDeleteCatalogRequest(ctx, id)
		}
		if err != nil {
			return err
		}

		_, err = http.DefaultClient.Do(deleteCatalogRequest)
		progressBar.Add(1)
		if err != nil {
			return err
		}
	}
	return nil
}
