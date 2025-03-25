package cmd

import (
	"benchmark/internal/cleaner"
	"context"
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
		Threads int
	}{
		// Default values
		Catalog: "polaris",
		Entity:  "catalog",
		Threads: 10,
	}

	flags.StringVar(&config.Catalog, "catalog", config.Catalog, "Catalog")
	flags.StringVar(&config.Entity, "entity", config.Entity, "Entity")
	flags.IntVar(&config.Threads, "threads", config.Threads, "Threads")
	return &Command{
		Name:        "clean",
		Description: "Clean up a specific entity in the catalog",
		Flags:       flags,
		Handler: func() error {
			// TODO: validate the flags
			return runClean(config.Catalog, config.Entity, config.Threads)
		},
	}
}

func runClean(catalog string, entity string, threads int) error {

	ctx, cancel := context.WithCancel(context.Background())

	defer cancel()
	catalogCleaner := cleaner.NewCatalogCleaner(catalog, threads)

	switch entity {
	case "catalog":
		if err := catalogCleaner.CleanCatalog(ctx); err != nil {
			return err
		}
	}

	return nil
}
