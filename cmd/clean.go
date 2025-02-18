package cmd

import (
	"flag"
	"fmt"
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
	// TODO: add clean up logic
	fmt.Println("This feature is not implemented yet")
	return nil
}
