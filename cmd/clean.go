package cmd

import (
	"benchmark/internal/catalog/polaris"
	"benchmark/internal/catalog/unity"
	"benchmark/internal/common"
	"flag"
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
	context, err := common.GetRequestContextFromEnv(catalog)
	if err != nil {
		return err
	}

	switch catalog {
	case "polaris":
		cleaner := polaris.NewCleaner(context)
		switch entity {
		case "catalog":
			if err = cleaner.CleanCatalog(); err != nil {
				return err
			}
		}
	case "unity":
		cleaner := unity.NewCleaner(context)
		switch entity {
		case "catalog":
			if err = cleaner.CleanCatalog(); err != nil {
				return err
			}
		}
	}

	return nil
}
