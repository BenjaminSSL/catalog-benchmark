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
	StartTimestamp time.Time     `json:"start_timestamp"`
	EndTimestamp   time.Time     `json:"end_timestamp"`
	Duration       time.Duration `json:"duration"`
	Entity         EntityType    `json:"entity"`
}

type BenchmarkType int
type EntityType string

const (
	CreateBenchmark BenchmarkType = iota + 1
	CreateDeleteBenchmark
	UpdateBenchmark // Update the same entity across all threads
	CreateDeleteListBenchmark
	UpdateGetBenchmark
)

const (
	CatalogEntity   EntityType = "catalog"
	SchemaEntity    EntityType = "schema"
	TableEntity     EntityType = "table"
	PrincipalEntity EntityType = "principal"
	ViewEntity      EntityType = "view"
	FunctionEntity  EntityType = "function"
	ModelEntity     EntityType = "model"
	VolumeEntity    EntityType = "volume"
)

func GetEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
