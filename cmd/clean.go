package cmd

import (
	"benchmark/internal/catalog/polaris"
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
		Entity:  "catalog-refactor",
	}

	flags.StringVar(&config.Catalog, "catalog-refactor", config.Catalog, "Catalog")
	flags.StringVar(&config.Entity, "entity", config.Entity, "Entity")
	return &Command{
		Name:        "clean",
		Description: "Clean up a specific entity in the catalog-refactor",
		Flags:       flags,
		Handler: func() error {
			// TODO: validate the flags
			return runClean(config.Catalog)
		},
	}
}

func runClean(catalog string) error {
	context, err := common.GetRequestContextFromEnv(catalog)
	if err != nil {
		return err
	}

	cleaner := polaris.NewCleaner(context)

	if err = cleaner.CleanCatalog(); err != nil {
		return err
	}
	return nil
}
