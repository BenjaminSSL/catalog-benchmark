package common

type Experiment struct {
	ID          string
	Catalog     string
	BenchmarkID BenchmarkType
	Threads     int
	Repeat      int
}

type BenchmarkType int

const (
	CreateCatalogBenchmark BenchmarkType = iota + 1
	CreateDeleteCatalogBenchmark
	UpdateCatalogBenchmark
)
