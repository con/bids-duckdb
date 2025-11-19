"""Approach 1: SQL-based regex parsing (lazy evaluation in DuckDB)."""

import logging
from pathlib import Path

from bids_duckdb.base import BIDSLoader

logger = logging.getLogger(__name__)


class SQLRegexLoader(BIDSLoader):
    """Load BIDS data using SQL regex extraction.

    This approach uses DuckDB's built-in regex functions to parse entity
    values directly in SQL. The parsing happens lazily when queries are executed.

    Pros:
    - Minimal Python overhead
    - Lazy evaluation - only parses what's needed
    - Leverages DuckDB's optimized regex engine

    Cons:
    - SQL regex can be complex
    - Need to know entities upfront
    - Potentially slower regex performance vs Python
    """

    def load_tsv_files(self, pattern: str = "**/*.tsv", use_full_names: bool = True) -> str:
        """Load TSV files using SQL regex extraction.

        Args:
            pattern: Glob pattern for files to load
            use_full_names: Use full entity names (e.g., 'subject' vs 'sub')

        Returns:
            Name of the created table
        """
        table_name = "bids_tsv_data"
        glob_pattern = str(self.bids_root / pattern)

        # Build entity extraction columns dynamically
        entity_extracts = []
        for short_name in self.schema.get_all_entity_short_names():
            col_name = self.get_entity_column_name(short_name, use_full_names)
            # Regex pattern: match entity-value in filename
            # Using regexp_extract(string, pattern, group)
            entity_extracts.append(
                f"regexp_extract(filename, '{short_name}-([a-zA-Z0-9]+)', 1) AS {col_name}"
            )

        entity_columns = ",\n        ".join(entity_extracts)

        query = f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT
            *,
            filename,
            {entity_columns}
        FROM read_csv(
            '{glob_pattern}',
            delim='\\t',
            auto_detect=true,
            filename=true,
            ignore_errors=true
        )
        """

        logger.info(f"Loading TSV files with SQL regex approach: {glob_pattern}")
        logger.debug(f"SQL query:\n{query}")

        self.con.execute(query)

        # Log statistics
        count = self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"Loaded {count} rows into {table_name}")

        return table_name

    def load_json_files(self, pattern: str = "**/*.json", use_full_names: bool = True) -> str:
        """Load JSON sidecar files using SQL regex extraction.

        Args:
            pattern: Glob pattern for files to load
            use_full_names: Use full entity names

        Returns:
            Name of the created table
        """
        table_name = "bids_json_metadata"
        glob_pattern = str(self.bids_root / pattern)

        # Build entity extraction columns
        entity_extracts = []
        for short_name in self.schema.get_all_entity_short_names():
            col_name = self.get_entity_column_name(short_name, use_full_names)
            entity_extracts.append(
                f"regexp_extract(filepath, '{short_name}-([a-zA-Z0-9]+)', 1) AS {col_name}"
            )

        entity_columns = ",\n        ".join(entity_extracts)

        # Read JSON files - we'll read content as struct
        query = f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT
            '{glob_pattern}' as search_pattern,
            file as filepath,
            {entity_columns}
        FROM glob('{glob_pattern}') AS files(file)
        """

        logger.info(f"Loading JSON files with SQL regex approach: {glob_pattern}")
        self.con.execute(query)

        count = self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"Loaded {count} JSON file references into {table_name}")

        return table_name

    def load_json_content(self, table_name: str = "bids_json_metadata") -> None:
        """Load actual JSON content for files in metadata table.

        Args:
            table_name: Name of table containing file paths
        """
        # Create a new table with JSON content
        query = f"""
        CREATE OR REPLACE TABLE {table_name}_content AS
        SELECT
            t.*,
            read_json_auto(t.filepath) as json_data
        FROM {table_name} AS t
        """

        self.con.execute(query)
        logger.info(f"Loaded JSON content into {table_name}_content")
