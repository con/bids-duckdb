"""Approach 3: Table function with lazy loading."""

import json
import logging
from pathlib import Path
from typing import Iterator

from bids_duckdb.base import BIDSLoader

logger = logging.getLogger(__name__)


class TableFunctionLoader(BIDSLoader):
    """Load BIDS data using DuckDB table functions.

    This approach creates Python table-valued functions that generate rows
    on-demand. Data is parsed lazily when accessed.

    Pros:
    - Lazy evaluation - only processes what's needed
    - Memory efficient
    - Flexible - can implement complex logic
    - Can filter before full processing

    Cons:
    - Requires DuckDB table function support
    - Python function call overhead
    - May be slower for full table scans
    """

    def __init__(self, *args, **kwargs) -> None:
        """Initialize table function loader."""
        super().__init__(*args, **kwargs)
        self._functions_registered = False

    def _register_functions(self, use_full_names: bool = True) -> None:
        """Register table-valued functions with DuckDB.

        Args:
            use_full_names: Use full entity names
        """
        if self._functions_registered:
            return

        # Create closure to capture self
        loader = self

        def scan_tsv_files(pattern: str = "**/*.tsv") -> Iterator[dict]:
            """Scan TSV files and yield row data with entities.

            Args:
                pattern: Glob pattern for files

            Yields:
                Dictionary with filepath and entity columns
            """
            tsv_files = list(loader.bids_root.glob(pattern))
            logger.info(f"Scanning {len(tsv_files)} TSV files")

            for tsv_path in tsv_files:
                entities = loader.parse_bids_entities(tsv_path)

                # Convert to full names if requested
                if use_full_names:
                    entities = {
                        loader.get_entity_full_name(k): v for k, v in entities.items()
                    }

                # Read TSV file with DuckDB
                try:
                    result = loader.con.execute(
                        f"""
                        SELECT * FROM read_csv(
                            '{tsv_path}',
                            delim='\\t',
                            auto_detect=true,
                            ignore_errors=true
                        )
                        """
                    ).fetchall()

                    # Get column names
                    columns = [desc[0] for desc in loader.con.description]

                    # Yield each row with entities
                    for row in result:
                        row_dict = dict(zip(columns, row))
                        row_dict.update(
                            {
                                "filepath": str(tsv_path),
                                "filename": tsv_path.name,
                                **entities,
                            }
                        )
                        yield row_dict

                except Exception as e:
                    logger.warning(f"Failed to read {tsv_path}: {e}")

        def scan_json_files(pattern: str = "**/*.json") -> Iterator[dict]:
            """Scan JSON files and yield metadata.

            Args:
                pattern: Glob pattern for files

            Yields:
                Dictionary with filepath, entities, and JSON content
            """
            json_files = list(loader.bids_root.glob(pattern))
            logger.info(f"Scanning {len(json_files)} JSON files")

            for json_path in json_files:
                entities = loader.parse_bids_entities(json_path)

                # Convert to full names if requested
                if use_full_names:
                    entities = {
                        loader.get_entity_full_name(k): v for k, v in entities.items()
                    }

                # Read JSON content
                try:
                    with json_path.open() as f:
                        json_data = json.load(f)
                        json_str = json.dumps(json_data)
                except Exception as e:
                    logger.warning(f"Failed to read {json_path}: {e}")
                    json_str = "{}"

                yield {
                    "filepath": str(json_path),
                    "filename": json_path.name,
                    "json_content": json_str,
                    **entities,
                }

        # Register functions with DuckDB
        try:
            self.con.create_function("bids_scan_tsv", scan_tsv_files)
            self.con.create_function("bids_scan_json", scan_json_files)
            self._functions_registered = True
            logger.info("Registered BIDS table functions")
        except Exception as e:
            logger.exception(f"Failed to register table functions: {e}")
            raise

    def load_tsv_files(self, pattern: str = "**/*.tsv", use_full_names: bool = True) -> str:
        """Load TSV files using table function.

        Args:
            pattern: Glob pattern for files to load
            use_full_names: Use full entity names

        Returns:
            Name of the created view/table
        """
        self._register_functions(use_full_names)
        table_name = "bids_tsv_data"

        # Create a view that uses the table function
        query = f"""
        CREATE OR REPLACE VIEW {table_name} AS
        SELECT * FROM bids_scan_tsv('{pattern}')
        """

        logger.info(f"Creating view {table_name} using table function")
        self.con.execute(query)

        # Optionally materialize the view
        # self.con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {table_name}")

        return table_name

    def load_json_files(self, pattern: str = "**/*.json", use_full_names: bool = True) -> str:
        """Load JSON files using table function.

        Args:
            pattern: Glob pattern for files to load
            use_full_names: Use full entity names

        Returns:
            Name of the created view/table
        """
        self._register_functions(use_full_names)
        table_name = "bids_json_metadata"

        query = f"""
        CREATE OR REPLACE VIEW {table_name} AS
        SELECT * FROM bids_scan_json('{pattern}')
        """

        logger.info(f"Creating view {table_name} using table function")
        self.con.execute(query)

        return table_name

    def materialize_view(self, view_name: str) -> str:
        """Convert a view to a materialized table.

        Args:
            view_name: Name of the view to materialize

        Returns:
            Name of the materialized table
        """
        table_name = f"{view_name}_materialized"

        query = f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM {view_name}
        """

        logger.info(f"Materializing view {view_name} to {table_name}")
        self.con.execute(query)

        count = self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"Materialized {count} rows")

        return table_name
