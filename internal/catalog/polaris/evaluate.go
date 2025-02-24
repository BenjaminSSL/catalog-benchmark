package polaris

import (
	"benchmark/internal/common"
	"log"
)

func EvaluateCreateCatalogBenchmark(context common.RequestContext, experiment common.Experiment) error {
	// Get the expected amount of catalogs
	expectedCatalogs := experiment.Threads * experiment.Repeat

	// List all the catalogs to be abel to count them
	catalogs, err := ListCatalogs(context)
	if err != nil {
		return err
	}

	log.Printf("Expected %d catalogs, got %d", expectedCatalogs, len(catalogs))
	return nil
}
