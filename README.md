# Data Catalog Benchmark Driver
This repository is a part of my Bachelor's thesis project, which aims to test metadata consistency in open-source data catalogs.

## Thesis
- **Title**: Metadata Consistency in Open-Source Data Catalogs
- **Date**: 2025-05-15

## Requirements
- Go 1.24+

For testing purposes, the following data catalogs are required:
- Polaris 1.0.0
- Unity Catalog 0.30


## Installation
```bash
git clone https://github.com/BenjaminSSL/catalog-benchmark
cd catalog-benchmark
go build -o driver main.go
```

## Usage
The driver support two data catalogs: Polaris and Unity Catalog. The following command is an example of how to run the driver with the Polaris catalog:
```bash
./driver benchmark -catalog=polaris -threads=2 -benchmark-id=1 -duration=1s -entity=catalog
```

### Command line arguments
| Argument        | Description        |
|-----------------|--------------------|
| `-catalog`      | The catalog to use. Supported values: `polaris`, `unity`. |
| `-threads`      | The number of threads to use. |
| `-benchmark-id` | The ID of the benchmark to run. |
| `-duration`     | The duration of the benchmark. |
| `-entity`       | The entity to use. |

Supported entities:
- `catalog`
- `principal` (Polaris only)
- `schema`
- `table`
- `view` (Polaris only)
- `function` (Unity Catalog only)
- `model` (Unity Catalog only)
- `volume` (Polaris only)




## Benchmarks
The included test various aspects of the data catalogs.

| Benchmark ID | Benchmark Name         | Description                                     |
|--------------|------------------------|-------------------------------------------------|
| 1            | Create `entity`        | Repeatedly creates `entity`                     |
| 2            | Create & Delete `entity` | Repeatedly creates and deletes `entity`         |
| 3            | Update `entity` | Repeatedly updates the same `entity`            |
| 4            | Create & Delete & List `entity` | Repeatedly creates, deletes, and lists `entity` |
| 5            | Create & Update & Get `entity` | Repeatedly updates, and gets `entity`           |


## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details