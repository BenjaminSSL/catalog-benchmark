package cmd

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/common"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
)

func init() {
	RegisterCommand(newCleanCommand())
}

func newCleanCommand() *Command {
	flags := flag.NewFlagSet("clean", flag.ExitOnError)

	// Anonymous flag config struct
	config := struct {
		Catalog string
		Entity  string
	}{
		// Default values
		Catalog: "polaris",
		Entity:  "catalog",
	}

	flags.StringVar(&config.Catalog, "catalog", config.Catalog, "Catalog")
	flags.StringVar(&config.Entity, "entity", config.Entity, "Entity")
	return &Command{
		Name:        "clean",
		Description: "Clean up a specific entity in the catalog",
		Flags:       flags,
		Handler: func() error {
			// TODO: validate the flags
			return runClean(config.Catalog, config.Entity)
		},
	}
}

func runClean(catalog string, entity string) error {
	// TODO: Clean up this test code
	context, err := common.GetRequestContextFromEnv(catalog)
	if err != nil {
		return err
	}

	listCatalogs := polaris.ListCatalogs{}
	req, err := listCatalogs.Build(context)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var result map[string]interface{}
	if err = json.Unmarshal(body, &result); err != nil {
		return err
	}

	names := make([]string, 0)

	if catalogs, ok := result["catalogs"].([]interface{}); ok {
		for _, catalog := range catalogs {
			if catalogMap, ok := catalog.(map[string]interface{}); ok {
				if name, ok := catalogMap["name"].(string); ok {
					names = append(names, name)
				}
			}
		}
	}

	log.Printf("Found %d catalog(s) in catalog %s\n", len(names), catalog)

	for _, name := range names {
		delete := polaris.DeleteCatalog{Name: name}
		operation, _ := delete.Build(context)

		respDelete, _ := http.DefaultClient.Do(operation)

		respDelete.Body.Close()
	}

	return nil
}
