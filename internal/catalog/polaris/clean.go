package polaris

import (
	"benchmark/internal/common"
	"encoding/json"
	"io"
	"net/http"
)

type CatalogCleaner struct {
	context common.RequestContext
	client  *http.Client
}

func NewCleaner(context common.RequestContext) *CatalogCleaner {
	return &CatalogCleaner{context: context, client: &http.Client{}}
}

func (c *CatalogCleaner) listCatalogsNames() ([]string, error) {
	req, err := NewListCatalogsRequest(c.context)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var result ListCatalogsResponse
	if err = json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	names := make([]string, 0, len(result.Catalogs))

	for _, catalog := range result.Catalogs {
		names = append(names, catalog.Name)
	}

	return names, nil
}

func (c *CatalogCleaner) CleanCatalog() error {
	catalogNames, err := c.listCatalogsNames()
	if err != nil {
		return err
	}

	for _, catalogName := range catalogNames {
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
