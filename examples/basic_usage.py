"""Basic usage examples for bids_duckdb."""

import logging

import bids_duckdb

# Configure logging to see what's happening
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# Path to BIDS dataset
# Replace with your dataset path
BIDS_ROOT = "/home/yoh/datasets/1076_spacetop"


def example_sql_approach():
    """Example using SQL regex approach (Approach 1)."""
    print("\n" + "=" * 60)
    print("APPROACH 1: SQL Regex Parsing")
    print("=" * 60)

    # Create loader
    loader = bids_duckdb.SQLRegexLoader(BIDS_ROOT)

    # Load TSV files
    loader.load_tsv_files()

    # Run some queries
    print("\nTotal rows:")
    result = loader.query("SELECT COUNT(*) FROM bids_tsv_data").fetchone()
    print(f"  {result[0]:,} rows")

    print("\nSubjects found:")
    result = loader.query(
        "SELECT DISTINCT subject FROM bids_tsv_data WHERE subject IS NOT NULL"
    ).fetchall()
    print(f"  {len(result)} subjects")

    print("\nFirst 5 rows:")
    result = loader.query("SELECT * FROM bids_tsv_data LIMIT 5").fetchdf()
    print(result)


def example_python_approach():
    """Example using Python preprocessor approach (Approach 2)."""
    print("\n" + "=" * 60)
    print("APPROACH 2: Python Preprocessor")
    print("=" * 60)

    loader = bids_duckdb.PythonPreprocessLoader(BIDS_ROOT)

    # Load TSV files
    loader.load_tsv_files()

    # Load JSON sidecars
    loader.load_json_files()

    # Run queries
    print("\nTables created:")
    for table in loader.get_table_names():
        count = loader.query(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count:,} rows")

    print("\nSubjects with their data counts:")
    result = loader.query(
        """
        SELECT subject, COUNT(*) as n
        FROM bids_tsv_data
        WHERE subject IS NOT NULL
        GROUP BY subject
        ORDER BY n DESC
        LIMIT 10
        """
    ).fetchdf()
    print(result)


def example_table_function_approach():
    """Example using table function approach (Approach 3)."""
    print("\n" + "=" * 60)
    print("APPROACH 3: Table Function (Lazy)")
    print("=" * 60)

    loader = bids_duckdb.TableFunctionLoader(BIDS_ROOT)

    # Load TSV files (creates views, not materialized tables)
    loader.load_tsv_files()

    # Query the view
    print("\nQuerying view (lazy evaluation):")
    result = loader.query(
        """
        SELECT subject, task, COUNT(*) as n
        FROM bids_tsv_data
        WHERE subject IS NOT NULL AND task IS NOT NULL
        GROUP BY subject, task
        LIMIT 10
        """
    ).fetchdf()
    print(result)

    # Optionally materialize the view for better performance
    print("\nMaterializing view...")
    loader.materialize_view("bids_tsv_data")

    print("\nTables after materialization:")
    for table in loader.get_table_names():
        count = loader.query(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count:,} rows")


def example_custom_queries():
    """Example showing various custom queries."""
    print("\n" + "=" * 60)
    print("CUSTOM QUERIES")
    print("=" * 60)

    # Use whichever approach you prefer
    loader = bids_duckdb.SQLRegexLoader(BIDS_ROOT)
    loader.load_tsv_files()

    # Query by specific task
    print("\nRows for a specific task:")
    result = loader.query(
        """
        SELECT COUNT(*) as n, subject
        FROM bids_tsv_data
        WHERE task = 'rest'
        GROUP BY subject
        """
    ).fetchdf()
    print(result)

    # Export results to pandas for further analysis
    print("\nExporting to pandas DataFrame:")
    df = loader.query("SELECT * FROM bids_tsv_data LIMIT 100").fetchdf()
    print(f"DataFrame shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")


def example_schema_inspection():
    """Example showing how to inspect the BIDS schema."""
    print("\n" + "=" * 60)
    print("SCHEMA INSPECTION")
    print("=" * 60)

    schema = bids_duckdb.BIDSSchema()

    print("\nEntity short name to full name mapping:")
    mapping = schema.get_entity_mapping()
    for short, full in sorted(mapping.items())[:10]:
        print(f"  {short:10} -> {full}")

    print("\nAll entity short names:")
    names = schema.get_all_entity_short_names()
    print(f"  {', '.join(sorted(names))}")


if __name__ == "__main__":
    # Run examples
    # Uncomment the ones you want to try

    # example_sql_approach()
    # example_python_approach()
    # example_table_function_approach()
    # example_custom_queries()
    example_schema_inspection()

    print("\n" + "=" * 60)
    print("Examples completed!")
    print("=" * 60)
