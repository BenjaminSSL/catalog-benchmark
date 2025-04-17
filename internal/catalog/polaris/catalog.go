package polaris

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
)

func ListCatalogs(ctx context.Context) ([]Catalog, error) {
	req := NewListCatalogsRequest(ctx)

	resp, err := http.DefaultClient.Do(req)
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

	return result.Catalogs, nil
}

func GrantCatalogPermissions(ctx context.Context, catalogName string) error {

	requests := []*http.Request{
		NewGrantPermissionCatalogRequest(ctx, catalogName, "TABLE_WRITE_DATA"),
		NewGrantPermissionCatalogRequest(ctx, catalogName, "TABLE_READ_DATA"),
	}

	for _, request := range requests {
		resp, err := http.DefaultClient.Do(request)
		if err != nil {
			return err
		}

		resp.Body.Close()

	}

	return nil

}
