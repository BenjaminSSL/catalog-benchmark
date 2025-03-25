package common

import (
	"github.com/google/uuid"
	"os"
	"time"
)

type Experiment struct {
	ID             uuid.UUID     `json:"id"`
	Catalog        string        `json:"catalog"`
	BenchmarkID    BenchmarkType `json:"benchmark"`
	Threads        int           `json:"threads"`
	Repeat         int           `json:"repeat"`
	StartTimestamp time.Time     `json:"start_timestamp"`
	EndTimestamp   time.Time     `json:"end_timestamp"`
}

type BenchmarkType int

const (
	CreateCatalogBenchmark BenchmarkType = iota + 1
	CreateDeleteCatalogBenchmark
	CreateUpdateCatalogBenchmark
	CreateDeleteListCatalogBenchmark
	UpdatePropertiesCatalogBenchmark
	UpdateGetCatalogBenchmark
)

func GetEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
