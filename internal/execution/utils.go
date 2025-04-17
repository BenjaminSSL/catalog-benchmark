package execution

import (
	"github.com/google/uuid"
	"log"
	"net/http"
)

func createCatalogAndSchema(catalog string, client *http.Client) (string, string, error) {
	catalogName, err := createCatalog(catalog, client)
	if err != nil {
		return "", "", err
	}

	schemaName, err := createSchema(catalog, client, catalogName)
	if err != nil {
		return "", "", err
	}

	return catalogName, schemaName, nil
}

func createSchema(catalog string, client *http.Client, catalogName string) (string, error) {
	schemaName := uuid.NewString()
	_, err := createSchemaRequest(catalog, client, catalogName, schemaName)
	if err != nil {
		log.Printf("Error creating schema for catalog %s: %v", catalogName, err)
		return "", err
	}

	log.Printf("Schema created: %s", schemaName)
	return schemaName, nil
}

func createView(catalog string, client *http.Client, catalogName string, schemaName string) (string, error) {
	viewName := uuid.NewString()
	_, err := createViewRequest(catalog, client, catalogName, schemaName, viewName)
	if err != nil {
		return "", err
	}

	log.Println("View created:", viewName)

	return viewName, nil
}

func createTable(catalog string, client *http.Client, catalogName string, schemaName string) (string, error) {
	tableName := uuid.NewString()
	_, err := createTableRequest(catalog, client, catalogName, schemaName, tableName)
	if err != nil {
		return "", err
	}

	log.Println("Table created:", tableName)

	return tableName, nil
}

func createCatalog(catalog string, client *http.Client) (string, error) {
	catalogName := uuid.NewString()
	_, err := createCatalogRequest(catalog, client, catalogName)
	if err != nil {
		return "", err
	}

	log.Println("Catalog created:", catalogName)

	return catalogName, nil
}

func createModel(catalog string, client *http.Client, catalogName string, schemaName string) (string, error) {
	modelName := uuid.NewString()
	_, err := createModelRequest(catalog, client, catalogName, schemaName, modelName)
	if err != nil {
		return "", err
	}

	return modelName, nil
}

func createVolume(catalog string, client *http.Client, catalogName string, schemaName string) (string, error) {
	volumeName := uuid.NewString()
	_, err := createVolumeRequest(catalog, client, catalogName, schemaName, volumeName)
	if err != nil {
		return "", err
	}

	return volumeName, nil
}

func createPrincipal(catalog string, client *http.Client) (string, error) {
	principalName := uuid.NewString()
	_, err := createPrincipalRequest(catalog, client, principalName)
	if err != nil {
		return "", err
	}

	return principalName, nil
}
