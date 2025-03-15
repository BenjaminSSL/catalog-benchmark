package cleaner

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/catalog/unity"
	"benchmark/internal/common"
	"context"
	"log"
	"net/http"
	"sync"
)

type CatalogCleaner struct {
	catalog string
	threads int
}

func NewCatalogCleaner(catalog string, threads int) *CatalogCleaner {
	return &CatalogCleaner{catalog: catalog, threads: threads}
}

func (c *CatalogCleaner) CleanCatalog(ctx context.Context) error {
	var ids []string

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

	progressBar := common.NewProgressBar(len(ids))
	progressBar.SetBufferSize(10)

	idChan := make(chan string, c.threads)
	errChan := make(chan error, len(ids))

	var wg sync.WaitGroup

	for i := 0; i < c.threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range idChan {
				var deleteCatalogRequest *http.Request
				var err error

				switch c.catalog {
				case "polaris":
					deleteCatalogRequest, err = polaris.NewDeleteCatalogRequest(ctx, id)
				case "unity":
					deleteCatalogRequest, err = unity.NewDeleteCatalogRequest(ctx, id)
				}
				if err != nil {
					errChan <- err
					continue
				}

				_, err = http.DefaultClient.Do(deleteCatalogRequest)
				if err != nil {
					errChan <- err
					continue
				}
				progressBar.Add(1)
			}
		}()
	}

	go func() {
		for _, id := range ids {
			idChan <- id
		}
		close(idChan)
	}()

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			log.Printf("Error deleting catalog: %v", err)
		}
	}

	progressBar.Flush()
	return nil
}
