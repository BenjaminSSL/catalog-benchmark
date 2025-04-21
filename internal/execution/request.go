package execution

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/catalog/unity"
	"benchmark/internal/common"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

func handleResponse(resp *http.Response, logger *common.RoutineBatchLogger, step int, requestType string) {
	statusCode := resp.StatusCode

	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()

	if err != nil {
		logger.Log("ERROR", requestType, step, statusCode, "", errors.New("failed to read response body").Error())
		return
	}

	//if len(body) > 1000 {
	//	body = body[:1000]
	//}

	if statusCode >= 200 && statusCode <= 299 {
		logger.Log("INFO", requestType, step, statusCode, string(body), "")
	} else {
		logger.Log("ERROR", requestType, step, statusCode, string(body), errors.New(fmt.Sprintf("Step %d has failed", step)))
	}
}
func handleRequestError(err error, logger *common.RoutineBatchLogger, step int, requestType string) {
	switch {
	case errors.Is(err, context.Canceled):
		logger.Log("ERROR", requestType, step, 0, err.Error(), errors.New("request timed out").Error())
	case err.(*url.Error).Timeout():
		logger.Log("ERROR", requestType, step, 0, err.Error(), errors.New("connection timeout").Error())
	default:
		logger.Log("ERROR", requestType, step, 0, err.Error(), errors.New("request failed").Error())
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

func getCatalogRequest(catalog string, client *http.Client, name string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewGetCatalogRequest(ctx, name))
	case "unity":
		return client.Do(unity.NewGetCatalogRequest(ctx, name))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}
func getPrincipalRequest(catalog string, client *http.Client, name string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewGetPrincipalRequest(ctx, name))

	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}
func getSchemaRequest(catalog string, client *http.Client, catalogName string, name string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewGetNamespaceRequest(ctx, catalogName, name))
	case "unity":
		return client.Do(unity.NewGetSchemaRequest(ctx, catalogName, name))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}
func getTableRequest(catalog string, client *http.Client, catalogName string, namespaceName string, tableName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewGetTableRequest(ctx, catalogName, namespaceName, tableName))
	case "unity":
		return client.Do(unity.NewGetTableRequest(ctx, catalogName, namespaceName, tableName))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
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

func createPrincipalRequest(catalog string, client *http.Client, name string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewCreatePrincipalRequest(ctx, name))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}
func createSchemaRequest(catalog string, client *http.Client, catalogName string, name string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewCreateNamespaceRequest(ctx, catalogName, name))
	case "unity":
		return client.Do(unity.NewCreateSchemaRequest(ctx, catalogName, name))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func createTableRequest(catalog string, client *http.Client, catalogName string, namespaceName string, tableName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewCreateTableRequest(ctx, catalogName, namespaceName, tableName))
	case "unity":
		return client.Do(unity.NewCreateTableRequest(ctx, catalogName, namespaceName, tableName))
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

func deletePrincipalRequest(catalog string, client *http.Client, name string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewDeletePrincipalRequest(ctx, name))
	default:
		panic(fmt.Errorf("unknown catalog type: %s", catalog))
	}
}

func deleteSchemaRequest(catalog string, client *http.Client, catalogName string, name string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewDeleteNamespaceRequest(ctx, catalogName, name))
	case "unity":
		return client.Do(unity.NewDeleteSchemaRequest(ctx, catalogName, name))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)

	}
}

func deleteTableRequest(catalog string, client *http.Client, catalogName string, namespaceName string, tableName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewDeleteTableRequest(ctx, catalogName, namespaceName, tableName))
	case "unity":
		return client.Do(unity.NewDeleteTableRequest(ctx, catalogName, namespaceName, tableName))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}

}
func updateCatalogRequest(catalog string, client *http.Client, name string, entityVersion int) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewUpdateCatalogRequest(ctx, name, entityVersion, map[string]string{
			"fictive_version": strconv.Itoa(entityVersion),
		}))
	case "unity":
		return client.Do(unity.NewUpdateCatalogRequest(ctx, name,
			map[string]string{
				"fictive_version": strconv.Itoa(entityVersion),
			},
		))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func updatePrincipalRequest(catalog string, client *http.Client, name string, entityVersion int) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewUpdatePrincipalRequest(ctx, name, entityVersion,
			map[string]string{
				"fictive_version": strconv.Itoa(entityVersion),
			}))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}
func updateSchemaRequest(catalog string, client *http.Client, catalogName string, name string, entityVersion int) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		//return client.Do(polaris.NewUpdateNamespaceRequest(ctx, catalogName, name, map[string]string{
		//	"fictive_version": strconv.Itoa(entityVersion),
		//}))
		return client.Do(polaris.NewUpdateNamespaceRequest(ctx, catalogName, name, map[string]string{
			uuid.NewString(): strconv.Itoa(entityVersion),
		}))
	case "unity":
		return client.Do(unity.NewUpdateSchemaRequest(ctx, catalogName, name, map[string]string{
			"fictive_version": strconv.Itoa(entityVersion),
		}))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func updateTableRequest(catalog string, client *http.Client, catalogName string, namespaceName string, tableName string, entityVersion int) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewUpdateTableRequest(ctx, catalogName, namespaceName, tableName, map[string]string{
			"fictive_version": strconv.Itoa(entityVersion),
		}))

	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func listCatalogsRequest(catalog string, client *http.Client) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewListCatalogsRequest(ctx))
	case "unity":
		return client.Do(unity.NewListCatalogsRequest(ctx, "", 100))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}
func listPrincipalsRequest(catalog string, client *http.Client) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewListPrincipalsRequest(ctx))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func listSchemasRequest(catalog string, client *http.Client, catalogName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	nextPageToken := ""
	var resp *http.Response
	var err error

	defer cancel()
	switch catalog {
	case "polaris":
		for {
			resp, err = client.Do(polaris.NewListNamespacesRequest(ctx, catalogName, nextPageToken, 100))
			if err != nil {
				return nil, err
			}
			var result polaris.ListTablesResponse
			body, _ := io.ReadAll(resp.Body)
			err = json.Unmarshal(body, &result)

			nextPageToken = result.NextPageToken

			if nextPageToken == "" {
				return resp, nil
			}
		}
	case "unity":
		for {
			resp, err = client.Do(unity.NewListSchemasRequest(ctx, catalogName, "", 100))
			if err != nil {
				return nil, err
			}
			var result unity.ListSchemasResponse
			body, _ := io.ReadAll(resp.Body)
			err = json.Unmarshal(body, &result)

			nextPageToken = result.NextPageToken

			if nextPageToken == "" {
				return resp, nil
			}
		}
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func listTablesRequest(catalog string, client *http.Client, catalogName string, schemaName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	nextPageToken := ""
	var resp *http.Response
	var err error

	defer cancel()
	switch catalog {
	case "polaris":
		for {
			resp, err = client.Do(polaris.NewListTablesRequest(ctx, catalogName, schemaName, nextPageToken, 100))
			if err != nil {
				return nil, err
			}
			var result polaris.ListTablesResponse
			body, _ := io.ReadAll(resp.Body)
			err = json.Unmarshal(body, &result)

			nextPageToken = result.NextPageToken

			if nextPageToken == "" {
				return resp, nil
			}
		}
	case "unity":
		for {
			resp, err = client.Do(unity.NewListTablesRequest(ctx, catalogName, schemaName, nextPageToken, 100))
			if err != nil {
				return nil, err
			}
			var result unity.ListSchemasResponse
			body, _ := io.ReadAll(resp.Body)
			err = json.Unmarshal(body, &result)

			nextPageToken = result.NextPageToken

			if nextPageToken == "" {
				return resp, nil
			}
		}
	default:

		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func createViewRequest(catalog string, client *http.Client, catalogName string, schemaName string, tableName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewCreateViewRequest(ctx, catalogName, schemaName, tableName))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func deleteViewRequest(catalog string, client *http.Client, catalogName string, schemaName string, tableName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewDeleteViewRequest(ctx, catalogName, schemaName, tableName))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}
func updateViewRequest(catalog string, client *http.Client, catalogName string, schemaName string, tableName string, entityVersion int) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewUpdateViewRequest(ctx, catalogName, schemaName, tableName, map[string]string{
			"fictive_version": strconv.Itoa(entityVersion),
		}))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func listViewsRequest(catalog string, client *http.Client, catalogName string, schemaName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	nextPageToken := ""
	var resp *http.Response
	var err error

	defer cancel()
	switch catalog {
	case "polaris":
		for {
			resp, err = client.Do(polaris.NewListViewsRequest(ctx, catalogName, schemaName, nextPageToken, 100))
			if err != nil {
				return nil, err
			}
			var result polaris.ListTablesResponse
			body, _ := io.ReadAll(resp.Body)
			err = json.Unmarshal(body, &result)

			nextPageToken = result.NextPageToken

			if nextPageToken == "" {
				return resp, nil
			}
		}
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func getViewRequest(catalog string, client *http.Client, catalogName string, schemaName string, tableName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "polaris":
		return client.Do(polaris.NewGetViewRequest(ctx, catalogName, schemaName, tableName))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func createFunctionRequest(catalog string, client *http.Client, catalogName string, schemaName string, functionName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "unity":
		req := unity.NewCreateFunctionRequest(ctx, catalogName, schemaName, functionName)
		//body, _ := io.ReadAll(req.Body)
		//log.Println(string(body))
		//panic("")
		return client.Do(req)
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func deleteFunctionRequest(catalog string, client *http.Client, catalogName string, schemaName string, functionName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "unity":
		return client.Do(unity.NewDeleteFunctionRequest(ctx, catalogName, schemaName, functionName))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func listFunctionsRequest(catalog string, client *http.Client, catalogName string, schemaName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	nextPageToken := ""
	var resp *http.Response
	var err error

	defer cancel()
	switch catalog {
	case "unity":
		for {
			resp, err = client.Do(unity.NewListFunctionsRequest(ctx, catalogName, schemaName, nextPageToken, 100))
			if err != nil {
				return nil, err
			}
			var result unity.ListFunctionsResponse
			body, _ := io.ReadAll(resp.Body)
			err = json.Unmarshal(body, &result)

			nextPageToken = result.NextPageToken

			if nextPageToken == "" {
				return resp, nil
			}
		}
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func createModelRequest(catalog string, client *http.Client, catalogName string, schemaName string, modelName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "unity":
		return client.Do(unity.NewCreateModelRequest(ctx, catalogName, schemaName, modelName))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func deleteModelRequest(catalog string, client *http.Client, catalogName string, schemaName string, modelName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "unity":
		return client.Do(unity.NewDeleteModelRequest(ctx, catalogName, schemaName, modelName))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func updateModelRequest(catalog string, client *http.Client, catalogName string, schemaName string, modelName string, entityVersion int) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "unity":
		return client.Do(unity.NewUpdateModelRequest(ctx, catalogName, schemaName, modelName, strconv.Itoa(entityVersion)))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func getModelRequest(catalog string, client *http.Client, catalogName string, schemaName string, modelName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "unity":
		return client.Do(unity.NewGetModelRequest(ctx, catalogName, schemaName, modelName))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func listModelsRequest(catalog string, client *http.Client, catalogName string, schemaName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	nextPageToken := ""
	var resp *http.Response
	var err error

	defer cancel()
	switch catalog {
	case "unity":
		for {
			resp, err = client.Do(unity.NewListModelsRequest(ctx, catalogName, schemaName, nextPageToken, 100))
			if err != nil {
				return nil, err
			}
			var result unity.ListModelsResponse
			body, _ := io.ReadAll(resp.Body)
			err = json.Unmarshal(body, &result)

			nextPageToken = result.NextPageToken

			if nextPageToken == "" {
				return resp, nil
			}
		}
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func createVolumeRequest(catalog string, client *http.Client, catalogName string, schemaName string, volumeName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "unity":
		return client.Do(unity.NewCreateVolumeRequest(ctx, catalogName, schemaName, volumeName))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}
func deleteVolumeRequest(catalog string, client *http.Client, catalogName string, schemaName string, volumeName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "unity":
		return client.Do(unity.NewDeleteVolumeRequest(ctx, catalogName, schemaName, volumeName))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}
func updateVolumeRequest(catalog string, client *http.Client, catalogName string, schemaName string, volumeName string, entityVersion int) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "unity":
		return client.Do(unity.NewUpdateVolumeRequest(ctx, catalogName, schemaName, volumeName, strconv.Itoa(entityVersion)))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}

func getVolumeRequest(catalog string, client *http.Client, catalogName string, schemaName string, volumeName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch catalog {
	case "unity":
		return client.Do(unity.NewGetVolumeRequest(ctx, catalogName, schemaName, volumeName))
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}
func listVolumesRequest(catalog string, client *http.Client, catalogName string, schemaName string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

	nextPageToken := ""
	var resp *http.Response
	var err error

	defer cancel()
	switch catalog {
	case "unity":
		for {
			resp, err = client.Do(unity.NewListVolumesRequest(ctx, catalogName, schemaName, nextPageToken, 100))
			if err != nil {
				return nil, err
			}
			var result unity.ListVolumesResponse
			body, _ := io.ReadAll(resp.Body)
			err = json.Unmarshal(body, &result)

			nextPageToken = result.NextPageToken

			if nextPageToken == "" {
				return resp, nil
			}
		}
	default:
		return nil, fmt.Errorf("unknown catalog type: %s", catalog)
	}
}
