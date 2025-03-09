package common

type Experiment struct {
	ID          string        `json:"id"`
	Catalog     string        `json:"catalog"`
	BenchmarkID BenchmarkType `json:"benchmark"`
	Threads     int           `json:"threads"`
	Repeat      int           `json:"repeat"`
}

type BenchmarkType int

const (
	CreateCatalogBenchmark BenchmarkType = iota + 1
	CreateDeleteCatalogBenchmark
	UpdateCatalogBenchmark
)
