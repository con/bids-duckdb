"""Benchmark comparison of different BIDS loading approaches."""

import logging
import sys
from pathlib import Path

import bids_duckdb

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def main():
    """Run benchmark comparison."""
    # Check for dataset path argument
    if len(sys.argv) > 1:
        bids_root = sys.argv[1]
    else:
        # Default dataset path
        bids_root = "/home/yoh/datasets/1076_spacetop"

    bids_path = Path(bids_root)
    if not bids_path.exists():
        print(f"Error: BIDS dataset not found at {bids_root}")
        print("\nUsage: python benchmark_comparison.py [BIDS_DATASET_PATH]")
        sys.exit(1)

    print(f"\nBenchmarking BIDS dataset: {bids_root}")

    # Define custom queries for benchmarking
    custom_queries = {
        # Basic count
        "count_all": "SELECT COUNT(*) FROM bids_tsv_data",
        # Distinct subjects
        "distinct_subjects": """
            SELECT COUNT(DISTINCT subject)
            FROM bids_tsv_data
            WHERE subject IS NOT NULL
        """,
        # Group by subject
        "group_by_subject": """
            SELECT subject, COUNT(*) as n
            FROM bids_tsv_data
            WHERE subject IS NOT NULL
            GROUP BY subject
        """,
        # Filter and group by multiple entities
        "filter_task_and_group": """
            SELECT subject, task, session, COUNT(*) as n
            FROM bids_tsv_data
            WHERE task IS NOT NULL AND subject IS NOT NULL
            GROUP BY subject, task, session
        """,
        # Top 10 subjects by data count
        "top_subjects": """
            SELECT subject, COUNT(*) as n
            FROM bids_tsv_data
            WHERE subject IS NOT NULL
            GROUP BY subject
            ORDER BY n DESC
            LIMIT 10
        """,
    }

    # Run benchmarks for all approaches
    print("\n" + "=" * 60)
    print("Starting Benchmark Suite")
    print("=" * 60)

    results = bids_duckdb.run_benchmark(
        bids_root=bids_root,
        approaches=["sql", "python", "table_function"],
        queries=custom_queries,
    )

    # Results are automatically printed by run_benchmark()

    # You can also access individual results
    print("\n" + "=" * 60)
    print("RECOMMENDATIONS")
    print("=" * 60)

    if results:
        # Find fastest for loading
        fastest_load = min(results, key=lambda r: r.load_time if r.load_time > 0 else float("inf"))
        print(f"\nFastest for initial loading: {fastest_load.approach_name}")

        # Find fastest for queries (average)
        query_averages = {}
        for result in results:
            if result.query_times:
                avg_time = sum(result.query_times.values()) / len(result.query_times)
                query_averages[result.approach_name] = avg_time

        if query_averages:
            fastest_query = min(query_averages.items(), key=lambda x: x[1])
            print(f"Fastest for queries (avg): {fastest_query[0]}")

        print("\nApproach recommendations:")
        print("  - SQL Regex: Best for simple datasets, minimal Python overhead")
        print("  - Python Preprocessor: Best for complex BIDS patterns, full control")
        print("  - Table Function: Best for selective queries, memory efficient")


if __name__ == "__main__":
    main()
