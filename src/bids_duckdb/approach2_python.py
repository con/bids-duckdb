"""Approach 2: Python preprocessor with eager loading."""

import json
import logging
from pathlib import Path

from bids_duckdb.base import BIDSLoader

logger = logging.getLogger(__name__)


class PythonPreprocessLoader(BIDSLoader):
    """Load BIDS data using Python preprocessing.

    This approach scans files in Python, parses entities upfront, and creates
    tables with entity columns already populated.

    Pros:
    - Full control over parsing logic
    - Can handle complex BIDS patterns
    - Python regex tends to be well-optimized
    - Better error handling

    Cons:
    - More memory usage (materializes all metadata)
    - Slower initial load time
    - Requires scanning all files upfront
    """

    def load_tsv_files(self, pattern: str = "**/*.tsv", use_full_names: bool = True) -> str:
        """Load TSV files using Python preprocessing.

        Args:
            pattern: Glob pattern for files to load
            use_full_names: Use full entity names

        Returns:
            Name of the created table
        """
        table_name = "bids_tsv_data"

        # Scan all TSV files
        tsv_files = list(self.bids_root.glob(pattern))
        logger.info(f"Found {len(tsv_files)} TSV files matching {pattern}")

        if not tsv_files:
            logger.warning("No TSV files found!")
            # Create empty table
            self.con.execute(f"CREATE OR REPLACE TABLE {table_name} (filepath VARCHAR)")
            return table_name

        # Parse entities for each file
        file_metadata = []
        for tsv_path in tsv_files:
            entities = self.parse_bids_entities(tsv_path)

            # Convert to full names if requested
            if use_full_names:
                entities = {
                    self.get_entity_full_name(k): v for k, v in entities.items()
                }

            file_metadata.append(
                {
                    "filepath": str(tsv_path),
                    "filename": tsv_path.name,
                    **entities,
                }
            )

        # Read all TSV files and union them
        union_parts = []
        for idx, file_info in enumerate(file_metadata):
            filepath = file_info["filepath"]

            # Build entity columns for this file
            entity_cols = []
            for key, value in file_info.items():
                if key not in ("filepath", "filename"):
                    # Quote string values
                    entity_cols.append(f"'{value}' AS {key}")

            entity_select = ", ".join(entity_cols) if entity_cols else ""

            # Read TSV and add entity columns
            select = f"""
                SELECT
                    '{filepath}' AS filepath,
                    '{file_info['filename']}' AS filename,
                    {entity_select + ',' if entity_select else ''}
                    *
                FROM read_csv('{filepath}', delim='\\t', auto_detect=true, ignore_errors=true)
            """

            union_parts.append(select)

        # Combine all files
        union_query = " UNION ALL ".join(union_parts)
        create_query = f"CREATE OR REPLACE TABLE {table_name} AS {union_query}"

        logger.info(f"Creating table {table_name} with {len(union_parts)} file(s)")
        logger.debug(f"Union query length: {len(create_query)} characters")

        self.con.execute(create_query)

        count = self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"Loaded {count} rows into {table_name}")

        return table_name

    def load_json_files(self, pattern: str = "**/*.json", use_full_names: bool = True) -> str:
        """Load JSON sidecar files using Python preprocessing.

        Args:
            pattern: Glob pattern for files to load
            use_full_names: Use full entity names

        Returns:
            Name of the created table
        """
        table_name = "bids_json_metadata"

        # Scan all JSON files
        json_files = list(self.bids_root.glob(pattern))
        logger.info(f"Found {len(json_files)} JSON files matching {pattern}")

        if not json_files:
            logger.warning("No JSON files found!")
            self.con.execute(f"CREATE OR REPLACE TABLE {table_name} (filepath VARCHAR)")
            return table_name

        # Parse entities and read JSON content
        records = []
        for json_path in json_files:
            entities = self.parse_bids_entities(json_path)

            # Convert to full names if requested
            if use_full_names:
                entities = {
                    self.get_entity_full_name(k): v for k, v in entities.items()
                }

            # Read JSON content
            try:
                with json_path.open() as f:
                    json_data = json.load(f)
                    # Convert to JSON string for storage
                    json_str = json.dumps(json_data)
            except Exception as e:
                logger.warning(f"Failed to read {json_path}: {e}")
                json_str = "{}"

            records.append(
                {
                    "filepath": str(json_path),
                    "filename": json_path.name,
                    "json_content": json_str,
                    **entities,
                }
            )

        # Create table from records
        self.con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM ?", [records])

        count = self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"Loaded {count} JSON files into {table_name}")

        return table_name

    def load_participants_tsv(self) -> str | None:
        """Load participants.tsv file specifically.

        Returns:
            Table name if file exists, None otherwise
        """
        participants_file = self.bids_root / "participants.tsv"

        if not participants_file.exists():
            logger.warning("participants.tsv not found")
            return None

        table_name = "bids_participants"

        query = f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM read_csv(
            '{participants_file}',
            delim='\\t',
            auto_detect=true
        )
        """

        self.con.execute(query)
        count = self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"Loaded {count} participants from participants.tsv")

        return table_name
