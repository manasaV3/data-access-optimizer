# Data Access Optimizer

A Python tool for optimizing data access through parquet manifest generation and lookup functionality. This project provides efficient indexing and querying of gene and tissue data stored in S3.

## Overview

The Data Access Optimizer consists of two main components:
- **Data Generation**: Creates optimized parquet manifests from S3 file paths
- **Data Lookup**: Provides fast query capabilities for gene/tissue combinations

## Features

- **S3 Path Processing**: Efficiently list and process S3 file paths
- **Parquet Manifest Generation**: Create optimized parquet files with dictionary encoding
- **Fast Lookups**: Query gene/tissue combinations with indexed performance
- **S3 Integration**: Seamlessly work with data stored in AWS S3
- **Local Caching**: Download and cache S3 files locally for improved performance

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd data-access-optimizer

# Install the package
pip install -e .

# Or install with development dependencies
pip install -e .[dev]
```

## Dependencies

- Python >= 3.12
- boto3 (AWS SDK)
- pandas (Data manipulation)
- pyarrow (Parquet support)
- duckdb (Fast SQL queries)
- jupyter (Notebook support)

## Usage

### Command Line Tools

The package provides two main command-line tools:

#### 1. S3 Path Fetcher

List S3 files and save paths to a text file:

```bash
s3-path-fetcher my-bucket my-prefix --paths-file paths.txt --aws-profile myprofile
```

#### 2. Manifest Generator

Generate a parquet manifest from file paths:

```bash
manifest-generator paths.txt output.parquet
```

### Python API

#### Generate Manifest

```python
from src.generator.manifest_generator import generate_manifest

generate_manifest(
    source_bucket="my-bucket",
    source_prefix="data/prefix/",
    output_file="manifest.parquet",
    aws_profile="myprofile"
)
```

#### Query Data

```python
from src.lookup.manifest_lookup import ManifestLookup

# Initialize lookup with local or S3 parquet file
lookup = ManifestLookup("manifest.parquet")

# Check if a gene/tissue combination exists
exists = lookup.exists("GENE123", "tissue_45")

# Get S3 file path for a combination
s3_path = lookup.get_s3_file_path("GENE123", "tissue_45")

# Download and get local file path
local_path = lookup.get_file_path("GENE123", "tissue_45")

# Get all records for a gene
gene_records = lookup.get_records_for_gene("GENE123")

# Get all records for a tissue
tissue_records = lookup.get_records_for_tissue("tissue_45")

# Get unique genes or tissues
unique_genes = lookup.get_unique("gene_id")
unique_tissues = lookup.get_unique("tissue_id")

# Use as context manager
with ManifestLookup("manifest.parquet") as lookup:
    result = lookup.exists("GENE123", "tissue_45")
```

## File Structure

```
src/
├── __init__.py
├── generator/
│   ├── __init__.py
│   ├── manifest_generator.py      # High-level manifest generation
│   ├── parquet_indexer.py         # Core parquet generation logic
│   └── s3_path_fetcher.py         # S3 file listing utilities
└── lookup/
    ├── __init__.py
    ├── manifest_lookup.py         # Fast lookup functionality
    └── lookup_demo.ipynb          # Example usage notebook
```

## Data Format

The parquet manifest contains three columns:
- `gene_id` (string): Gene identifier extracted from file paths
- `tissue_id` (int32): Tissue identifier extracted from file paths  
- `file_path` (string): Full S3 path to the data file

File paths are expected to match the pattern:
```
.../v1/ad/{gene_id}/model_tissue_{tissue_id}.tl
```

## Performance Optimizations

- **Dictionary Encoding**: Efficient storage for repeated gene IDs
- **Snappy Compression**: Fast compression/decompression
- **DuckDB Integration**: High-performance SQL queries with indexing
- **Local Caching**: Reduces S3 API calls for repeated access

## Development

### Setup Development Environment

```bash
pip install -e .[dev]
```

## License

MIT License