package unity

import (
	"benchmark/internal/common"
	"encoding/json"
	"io"
	"net/http"
)

func ListCatalogs(context common.RequestContext, params ListCatalogsParams) ([]Catalog, error) {
	var allCatalogs []Catalog
	var nextPageToken string

	for {
		if len(allCatalogs) > 0 {
			params.PageToken = nextPageToken
		}

		req, err := NewListCatalogsRequest(context, params)
		if err != nil {
			return nil, err
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close() // Using Close() directly instead of defer within the loop
		if err != nil {
			return nil, err
		}

		var result ListCatalogsResponse
		if err = json.Unmarshal(body, &result); err != nil {
			return nil, err
		}

		// Append the catalogs from this page to our results
		allCatalogs = append(allCatalogs, result.Catalogs...)

		// Check if there's a next page token
		nextPageToken = result.NextPageToken
		if nextPageToken == "" {
			// No more pages, break out of the loop
			break
		}
	}

	return allCatalogs, nil
}
