"""BIDS DuckDB - Load BIDS datasets into DuckDB with multiple approaches."""

import logging

from bids_duckdb.approach1_sql import SQLRegexLoader
from bids_duckdb.approach2_python import PythonPreprocessLoader
from bids_duckdb.approach3_table_function import TableFunctionLoader
from bids_duckdb.benchmark import BenchmarkSuite
from bids_duckdb.benchmark import run_benchmark
from bids_duckdb.schema import BIDSSchema

logging.getLogger(__name__).addHandler(logging.NullHandler())

__author__ = "Yaroslav Halchenko"
__email__ = "debian@onerussian.com"
__version__ = "0.1.0"

__all__ = [
    "SQLRegexLoader",
    "PythonPreprocessLoader",
    "TableFunctionLoader",
    "BIDSSchema",
    "BenchmarkSuite",
    "run_benchmark",
]
