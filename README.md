## Badges

(Customize these badges with your own links, and check https://shields.io/ or https://badgen.net/ to see which other badges are available.)

| fair-software.eu recommendations | |
| :-- | :--  |
| (1/5) code repository              | [![github repo badge](https://img.shields.io/badge/github-repo-000.svg?logo=github&labelColor=gray&color=blue)](https://github.com/con/bids_duckdb) |
| (2/5) license                      | [![github license badge](https://img.shields.io/github/license/con/bids_duckdb)](https://github.com/con/bids_duckdb) |
| (3/5) community registry           | [![RSD](https://img.shields.io/badge/rsd-bids_duckdb-00a3e3.svg)](https://www.research-software.nl/software/bids_duckdb) [![workflow pypi badge](https://img.shields.io/pypi/v/bids_duckdb.svg?colorB=blue)](https://pypi.python.org/project/bids_duckdb/) |
| (4/5) citation                     | [![DOI](https://zenodo.org/badge/DOI/<replace-with-created-DOI>.svg)](https://doi.org/<replace-with-created-DOI>)|
| (5/5) checklist                    | [![workflow cii badge](https://bestpractices.coreinfrastructure.org/projects/<replace-with-created-project-identifier>/badge)](https://bestpractices.coreinfrastructure.org/projects/<replace-with-created-project-identifier>) |
| howfairis                          | [![fair-software badge](https://img.shields.io/badge/fair--software.eu-%E2%97%8F%20%20%E2%97%8F%20%20%E2%97%8F%20%20%E2%97%8F%20%20%E2%97%8B-yellow)](https://fair-software.eu) |
| **Other best practices**           | &nbsp; |
| Static analysis                    | [![workflow scq badge](https://sonarcloud.io/api/project_badges/measure?project=con_bids_duckdb&metric=alert_status)](https://sonarcloud.io/dashboard?id=con_bids_duckdb) |
| Coverage                           | [![workflow scc badge](https://sonarcloud.io/api/project_badges/measure?project=con_bids_duckdb&metric=coverage)](https://sonarcloud.io/dashboard?id=con_bids_duckdb) || Documentation                      | [![Documentation Status](https://readthedocs.org/projects/bids_duckdb/badge/?version=latest)](https://bids_duckdb.readthedocs.io/en/latest/?badge=latest) || **GitHub Actions**                 | &nbsp; |
| Build                              | [![build](https://github.com/con/bids_duckdb/actions/workflows/build.yml/badge.svg)](https://github.com/con/bids_duckdb/actions/workflows/build.yml) |
| Citation data consistency          | [![cffconvert](https://github.com/con/bids_duckdb/actions/workflows/cffconvert.yml/badge.svg)](https://github.com/con/bids_duckdb/actions/workflows/cffconvert.yml) || SonarCloud                         | [![sonarcloud](https://github.com/con/bids_duckdb/actions/workflows/sonarcloud.yml/badge.svg)](https://github.com/con/bids_duckdb/actions/workflows/sonarcloud.yml) |## How to use bids_duckdb

Python package to provide DuckDB interface to BIDS datasets with three alternative approaches for loading data, allowing performance comparison and flexibility.

### Overview

`bids_duckdb` loads BIDS (Brain Imaging Data Structure) datasets into DuckDB, automatically extracting metadata from BIDS-compliant filenames and hierarchies. It supports loading both `.tsv` files and `.json` sidecar files with entity information parsed from filenames (e.g., `sub-01_ses-pre_task-rest_bold.nii.gz` → `subject=01, session=pre, task=rest`).

**Key Features:**
- 🔄 **Three switchable approaches** for loading data (SQL regex, Python preprocessor, Table functions)
- 🗺️ **Dynamic BIDS schema loading** - fetches entity definitions from the official BIDS specification
- 🏷️ **Entity name expansion** - maps short names (`sub`) to full names (`subject`)
- ⚡ **Performance benchmarking** - compare approaches on your dataset
- 🔍 **SQL queries** on BIDS data with full DuckDB capabilities

### Quick Start

```python
import bids_duckdb

# Choose your approach
loader = bids_duckdb.SQLRegexLoader("/path/to/bids/dataset")

# Load TSV files
loader.load_tsv_files()

# Query with SQL
result = loader.query("""
    SELECT subject, task, COUNT(*) as n
    FROM bids_tsv_data
    WHERE task = 'rest'
    GROUP BY subject, task
""").fetchdf()

print(result)
```

### The Three Approaches

#### Approach 1: SQL Regex Parsing (Lazy)

Uses DuckDB's built-in regex functions to parse entities directly in SQL.

```python
from bids_duckdb import SQLRegexLoader

loader = SQLRegexLoader("/path/to/bids/dataset")
loader.load_tsv_files()
```

**Pros:**
- Minimal Python overhead
- Lazy evaluation - only parses what's needed
- Leverages DuckDB's optimized regex engine

**Cons:**
- SQL regex can be complex for debugging
- Need to know entities upfront
- Potentially slower regex performance vs Python

**Best for:** Simple datasets, when you want minimal overhead

#### Approach 2: Python Preprocessor (Eager)

Scans files in Python, parses entities upfront, creates materialized tables.

```python
from bids_duckdb import PythonPreprocessLoader

loader = PythonPreprocessLoader("/path/to/bids/dataset")
loader.load_tsv_files()
loader.load_json_files()  # Also load JSON sidecars
```

**Pros:**
- Full control over parsing logic
- Can handle complex BIDS patterns
- Better error handling and debugging
- Can load JSON content directly

**Cons:**
- Higher memory usage (materializes all metadata)
- Slower initial load time
- Requires scanning all files upfront

**Best for:** Complex BIDS datasets, when you need full control

#### Approach 3: Table Functions (Lazy)

Creates Python table-valued functions that generate rows on-demand.

```python
from bids_duckdb import TableFunctionLoader

loader = TableFunctionLoader("/path/to/bids/dataset")
loader.load_tsv_files()  # Creates views, not tables

# Optionally materialize for repeated queries
loader.materialize_view("bids_tsv_data")
```

**Pros:**
- Lazy evaluation - only processes what's needed
- Memory efficient
- Flexible - can implement complex logic
- Can filter before full processing

**Cons:**
- Python function call overhead
- May be slower for full table scans

**Best for:** Selective queries, memory-constrained environments

### Benchmarking

Compare all approaches on your dataset:

```python
from bids_duckdb import run_benchmark

results = run_benchmark(
    "/path/to/bids/dataset",
    approaches=["sql", "python", "table_function"]
)
```

Or use the included script:

```bash
python examples/benchmark_comparison.py /path/to/bids/dataset
```

### BIDS Schema Integration

The library dynamically loads BIDS entity definitions from the official specification:

```python
from bids_duckdb import BIDSSchema

schema = BIDSSchema()  # Fetches from bids-standard/bids-specification
mapping = schema.get_entity_mapping()
# {'sub': 'subject', 'ses': 'session', 'task': 'task', ...}
```

This allows:
- **Automatic entity detection** based on the latest BIDS spec
- **Full name expansion** in query results (subject instead of sub)
- **Version flexibility** - can specify different BIDS spec versions

### Examples

See the `examples/` directory for:
- `basic_usage.py` - Individual approach examples
- `benchmark_comparison.py` - Performance comparison

The project setup is documented in [project_setup.md](project_setup.md).

## Installation

To install bids_duckdb from GitHub repository, do:

```console
git clone git@github.com:con/bids_duckdb.git
cd bids_duckdb
python -m pip install .
```

## Documentation

Include a link to your project's full documentation here.

## Contributing

If you want to contribute to the development of bids_duckdb,
have a look at the [contribution guidelines](CONTRIBUTING.md).

## Credits

This package was created with [Copier](https://github.com/copier-org/copier) and the [NLeSC/python-template](https://github.com/NLeSC/python-template).
