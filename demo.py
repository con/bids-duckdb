#!/usr/bin/env python
"""Quick demo of bids_duckdb library.

This script demonstrates all three approaches on a BIDS dataset.
Usage: python demo.py [path/to/bids/dataset]
"""

import logging
import sys
from pathlib import Path

import bids_duckdb

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s",
)


def demo_approach(loader_class, approach_name, bids_root):
    """Demo a single approach."""
    print(f"\n{'=' * 60}")
    print(f"{approach_name}")
    print(f"{'=' * 60}")

    # Create loader
    loader = loader_class(bids_root)

    # Load TSV files
    print("Loading TSV files...")
    loader.load_tsv_files()

    # Show statistics
    stats = loader.get_statistics()
    print(f"\nTables created: {list(stats['tables'].keys())}")

    for table, info in stats["tables"].items():
        print(f"  {table}: {info['row_count']:,} rows")

    # Run a sample query
    print("\nSample query - distinct subjects:")
    try:
        result = loader.query(
            """
            SELECT DISTINCT subject
            FROM bids_tsv_data
            WHERE subject IS NOT NULL
            ORDER BY subject
            LIMIT 10
            """
        ).fetchall()

        if result:
            subjects = [row[0] for row in result]
            print(f"  Found {len(subjects)} subjects: {', '.join(subjects[:5])}...")
        else:
            print("  No subjects found with 'subject' column")
    except Exception as e:
        print(f"  Query failed: {e}")

    # Show first few rows
    print("\nFirst 3 rows of data:")
    try:
        df = loader.query("SELECT * FROM bids_tsv_data LIMIT 3").fetchdf()
        print(df.to_string())
    except Exception as e:
        print(f"  Failed to fetch data: {e}")


def main():
    """Run demo."""
    # Get BIDS root from command line or use default
    if len(sys.argv) > 1:
        bids_root = sys.argv[1]
    else:
        # Try common locations
        possible_paths = [
            "/home/yoh/datasets/1076_spacetop",
            Path.home() / "datasets" / "1076_spacetop",
            "test_dataset",
        ]

        bids_root = None
        for path in possible_paths:
            if Path(path).exists():
                bids_root = str(path)
                break

        if bids_root is None:
            print("Error: No BIDS dataset found.")
            print("\nUsage: python demo.py [path/to/bids/dataset]")
            print("\nTried the following locations:")
            for path in possible_paths:
                print(f"  - {path}")
            sys.exit(1)

    bids_path = Path(bids_root)
    if not bids_path.exists():
        print(f"Error: BIDS dataset not found at {bids_root}")
        sys.exit(1)

    print("=" * 60)
    print("BIDS DuckDB Demo")
    print("=" * 60)
    print(f"Dataset: {bids_root}")

    # Show BIDS schema info
    print("\n" + "=" * 60)
    print("BIDS Schema Information")
    print("=" * 60)

    schema = bids_duckdb.BIDSSchema()
    mapping = schema.get_entity_mapping()

    print(f"\nEntity mappings ({len(mapping)} total):")
    for short, full in sorted(mapping.items())[:10]:
        print(f"  {short:10} -> {full}")
    if len(mapping) > 10:
        print(f"  ... and {len(mapping) - 10} more")

    # Demo each approach
    demo_approach(bids_duckdb.SQLRegexLoader, "Approach 1: SQL Regex", bids_root)
    demo_approach(bids_duckdb.PythonPreprocessLoader, "Approach 2: Python Preprocessor", bids_root)
    demo_approach(bids_duckdb.TableFunctionLoader, "Approach 3: Table Function", bids_root)

    # Summary
    print("\n" + "=" * 60)
    print("Demo Complete!")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. Run benchmarks: python examples/benchmark_comparison.py")
    print("  2. Try the examples: python examples/basic_usage.py")
    print("  3. Use in your own code:")
    print("     import bids_duckdb")
    print("     loader = bids_duckdb.SQLRegexLoader('/path/to/bids')")
    print("     loader.load_tsv_files()")
    print("     result = loader.query('SELECT * FROM bids_tsv_data').fetchdf()")


if __name__ == "__main__":
    main()
