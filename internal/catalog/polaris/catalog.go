package polaris

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
)

func ListCatalogs(ctx context.Context) ([]Catalog, error) {
	req, err := NewListCatalogsRequest(ctx)
	if err != nil {
		return nil, err
	}

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
