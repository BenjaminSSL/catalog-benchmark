package unity

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
)

func ListCatalogs(ctx context.Context, maxResults int) ([]Catalog, error) {
	var allCatalogs []Catalog
	var nextPageToken = ""

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:

		}

		req := NewListCatalogsRequest(ctx, nextPageToken, maxResults)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}

		body, err := io.ReadAll(resp.Body)
		err = resp.Body.Close()
		if err != nil {
			return nil, err
		}
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
			log.Println(len(allCatalogs))
			break
		}
	}

	return allCatalogs, nil
}
