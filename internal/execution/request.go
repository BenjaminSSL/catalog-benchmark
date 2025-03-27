package execution

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/catalog/unity"
	"benchmark/internal/common"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

func handleResponse(resp *http.Response, logger *common.RoutineBatchLogger, step int) {
	statusCode := resp.StatusCode

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()

	if err != nil {
		logger.Log("ERROR", step, statusCode, "", errors.New("Failed to read response body").Error())
		return
	}

	if len(body) > 1000 {
		body = body[:1000]
	}

	if statusCode >= 200 && statusCode <= 299 {
		logger.Log("INFO", step, statusCode, string(body), "")
	} else {
		logger.Log("ERROR", step, statusCode, string(body), errors.New(fmt.Sprintf("Step %d has failed", step)))
	}
}
func handleRequestError(err error, logger *common.RoutineBatchLogger, step int) {
	switch {
	case errors.Is(err, context.Canceled):
		logger.Log("ERROR", step, 0, err.Error(), errors.New("Request timed out").Error())
	case err.(*url.Error).Timeout():
		logger.Log("ERROR", step, 0, err.Error(), errors.New("Connection timeout").Error())
	default:
		logger.Log("ERROR", step, 0, err.Error(), errors.New("Request failed").Error())
	}
}

func getHttpClient() *http.Client {
	return &http.Client{
		Timeout: time.Second * 30,
		Transport: &http.Transport{
			MaxIdleConns:        10000,
			MaxIdleConnsPerHost: 1000,
			MaxConnsPerHost:     1000,
			DisableKeepAlives:   false,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}
}
func createCatalogRequest(catalog string, client *http.Client, name string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewCreateCatalogRequest(ctx, name))
	case "unity":
		return client.Do(unity.NewCreateCatalogRequest(ctx, name))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func deleteCatalogRequest(catalog string, client *http.Client, name string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewDeleteCatalogRequest(ctx, name))
	case "unity":
		return client.Do(unity.NewDeleteCatalogRequest(ctx, name))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}
