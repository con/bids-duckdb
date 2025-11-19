"""Benchmarking utilities for comparing BIDS loader approaches."""

import logging
import time
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any

import duckdb

from bids_duckdb.approach1_sql import SQLRegexLoader
from bids_duckdb.approach2_python import PythonPreprocessLoader
from bids_duckdb.approach3_table_function import TableFunctionLoader

logger = logging.getLogger(__name__)


@dataclass
class BenchmarkResult:
    """Results from a single benchmark run."""

    approach_name: str
    load_time: float
    query_times: dict[str, float] = field(default_factory=dict)
    row_counts: dict[str, int] = field(default_factory=dict)
    memory_usage: int = 0
    errors: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        """Format benchmark result as string."""
        lines = [
            f"\n{'='*60}",
            f"Approach: {self.approach_name}",
            f"{'='*60}",
            f"Load Time: {self.load_time:.3f}s",
        ]

        if self.row_counts:
            lines.append("\nRow Counts:")
            for table, count in self.row_counts.items():
                lines.append(f"  {table}: {count:,}")

        if self.query_times:
            lines.append("\nQuery Times:")
            for query_name, time_s in self.query_times.items():
                lines.append(f"  {query_name}: {time_s:.3f}s")

        if self.memory_usage:
            lines.append(f"\nMemory Usage: {self.memory_usage / 1024 / 1024:.2f} MB")

        if self.errors:
            lines.append("\nErrors:")
            for error in self.errors:
                lines.append(f"  - {error}")

        return "\n".join(lines)


class BenchmarkSuite:
    """Benchmark suite for comparing BIDS loader approaches."""

    def __init__(self, bids_root: str | Path) -> None:
        """Initialize benchmark suite.

        Args:
            bids_root: Path to BIDS dataset
        """
        self.bids_root = Path(bids_root)
        self.results: list[BenchmarkResult] = []

    def run_all(
        self,
        approaches: list[str] | None = None,
        queries: dict[str, str] | None = None,
    ) -> list[BenchmarkResult]:
        """Run benchmarks for all approaches.

        Args:
            approaches: List of approach names to test (default: all)
            queries: Dictionary of query name -> SQL to benchmark

        Returns:
            List of benchmark results
        """
        if approaches is None:
            approaches = ["sql", "python", "table_function"]

        if queries is None:
            queries = self._get_default_queries()

        self.results = []

        for approach in approaches:
            logger.info(f"\n{'='*60}")
            logger.info(f"Benchmarking: {approach}")
            logger.info(f"{'='*60}")

            try:
                result = self._benchmark_approach(approach, queries)
                self.results.append(result)
            except Exception as e:
                logger.exception(f"Benchmark failed for {approach}: {e}")
                self.results.append(
                    BenchmarkResult(
                        approach_name=approach,
                        load_time=0,
                        errors=[str(e)],
                    )
                )

        return self.results

    def _benchmark_approach(
        self, approach: str, queries: dict[str, str]
    ) -> BenchmarkResult:
        """Benchmark a single approach.

        Args:
            approach: Approach name
            queries: Queries to run

        Returns:
            Benchmark result
        """
        result = BenchmarkResult(approach_name=approach)

        # Create fresh connection for each approach
        con = duckdb.connect()

        # Create loader
        loader = self._create_loader(approach, con)

        # Benchmark loading
        logger.info("Loading data...")
        start = time.perf_counter()

        try:
            loader.load_tsv_files()
            load_time = time.perf_counter() - start
            result.load_time = load_time
            logger.info(f"Load completed in {load_time:.3f}s")
        except Exception as e:
            logger.exception(f"Load failed: {e}")
            result.errors.append(f"Load error: {e}")
            return result

        # Get row counts
        try:
            stats = loader.get_statistics()
            result.row_counts = {
                table: info["row_count"]
                for table, info in stats.get("tables", {}).items()
            }
        except Exception as e:
            logger.warning(f"Failed to get statistics: {e}")

        # Benchmark queries
        for query_name, sql in queries.items():
            logger.info(f"Running query: {query_name}")
            try:
                start = time.perf_counter()
                loader.query(sql).fetchall()
                query_time = time.perf_counter() - start
                result.query_times[query_name] = query_time
                logger.info(f"  Completed in {query_time:.3f}s")
            except Exception as e:
                logger.warning(f"Query failed: {e}")
                result.errors.append(f"Query '{query_name}' error: {e}")

        # Get memory usage estimate
        try:
            # DuckDB database size
            result.memory_usage = con.execute(
                "SELECT SUM(estimated_size) FROM duckdb_tables()"
            ).fetchone()[0]
        except Exception:
            pass

        con.close()
        return result

    def _create_loader(self, approach: str, con: duckdb.DuckDBPyConnection):
        """Create loader instance for approach.

        Args:
            approach: Approach name
            con: DuckDB connection

        Returns:
            Loader instance
        """
        loaders = {
            "sql": SQLRegexLoader,
            "python": PythonPreprocessLoader,
            "table_function": TableFunctionLoader,
        }

        loader_class = loaders.get(approach)
        if loader_class is None:
            msg = f"Unknown approach: {approach}"
            raise ValueError(msg)

        return loader_class(self.bids_root, connection=con)

    def _get_default_queries(self) -> dict[str, str]:
        """Get default benchmark queries.

        Returns:
            Dictionary of query name -> SQL
        """
        return {
            "count_all": "SELECT COUNT(*) FROM bids_tsv_data",
            "distinct_subjects": "SELECT COUNT(DISTINCT subject) FROM bids_tsv_data WHERE subject IS NOT NULL",
            "group_by_subject": """
                SELECT subject, COUNT(*) as n
                FROM bids_tsv_data
                WHERE subject IS NOT NULL
                GROUP BY subject
            """,
            "filter_by_task": """
                SELECT *
                FROM bids_tsv_data
                WHERE task IS NOT NULL
                LIMIT 100
            """,
        }

    def print_summary(self) -> None:
        """Print summary of all benchmark results."""
        print("\n" + "=" * 60)
        print("BENCHMARK SUMMARY")
        print("=" * 60)

        for result in self.results:
            print(result)

        # Comparison table
        if len(self.results) > 1:
            self._print_comparison_table()

    def _print_comparison_table(self) -> None:
        """Print comparison table of results."""
        print("\n" + "=" * 60)
        print("COMPARISON TABLE")
        print("=" * 60)

        # Load times
        print("\nLoad Times:")
        print(f"{'Approach':<20} {'Time (s)':>12} {'Relative':>12}")
        print("-" * 50)

        min_load_time = min(r.load_time for r in self.results if r.load_time > 0)
        for result in sorted(self.results, key=lambda r: r.load_time):
            if result.load_time > 0:
                relative = result.load_time / min_load_time
                print(
                    f"{result.approach_name:<20} {result.load_time:>12.3f} {relative:>12.2f}x"
                )

        # Query times (if any)
        query_names = set()
        for result in self.results:
            query_names.update(result.query_times.keys())

        for query_name in sorted(query_names):
            print(f"\nQuery: {query_name}")
            print(f"{'Approach':<20} {'Time (s)':>12} {'Relative':>12}")
            print("-" * 50)

            times = {
                r.approach_name: r.query_times.get(query_name, 0) for r in self.results
            }
            min_time = min(t for t in times.values() if t > 0) if times else 1

            for result in sorted(
                self.results, key=lambda r: r.query_times.get(query_name, float("inf"))
            ):
                time_val = result.query_times.get(query_name)
                if time_val is not None and time_val > 0:
                    relative = time_val / min_time
                    print(f"{result.approach_name:<20} {time_val:>12.3f} {relative:>12.2f}x")


def run_benchmark(
    bids_root: str | Path,
    approaches: list[str] | None = None,
    queries: dict[str, str] | None = None,
) -> list[BenchmarkResult]:
    """Convenience function to run benchmark suite.

    Args:
        bids_root: Path to BIDS dataset
        approaches: Approaches to test (default: all)
        queries: Custom queries to run

    Returns:
        List of benchmark results
    """
    suite = BenchmarkSuite(bids_root)
    results = suite.run_all(approaches=approaches, queries=queries)
    suite.print_summary()
    return results
