"""Base class for BIDS DuckDB loaders."""

import logging
import re
from abc import ABC
from abc import abstractmethod
from pathlib import Path
from typing import Any

import duckdb

from bids_duckdb.schema import BIDSSchema
from bids_duckdb.schema import get_default_schema

logger = logging.getLogger(__name__)


class BIDSLoader(ABC):
    """Abstract base class for BIDS data loaders."""

    def __init__(
        self,
        bids_root: str | Path,
        schema: BIDSSchema | None = None,
        connection: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        """Initialize BIDS loader.

        Args:
            bids_root: Path to BIDS dataset root
            schema: BIDSSchema instance (default: use latest schema)
            connection: Existing DuckDB connection (default: create new)
        """
        self.bids_root = Path(bids_root)
        self.schema = schema or get_default_schema()
        self.con = connection or duckdb.connect()

        # Entity mappings
        self.entity_short_to_full = self.schema.get_entity_mapping()
        self.entity_display_names = self.schema.get_entity_display_names()

        if not self.bids_root.exists():
            msg = f"BIDS root does not exist: {self.bids_root}"
            raise FileNotFoundError(msg)

    @abstractmethod
    def load_tsv_files(self, pattern: str = "**/*.tsv") -> str:
        """Load TSV files from BIDS dataset.

        Args:
            pattern: Glob pattern for files to load

        Returns:
            Name of the created table
        """

    @abstractmethod
    def load_json_files(self, pattern: str = "**/*.json") -> str:
        """Load JSON sidecar files from BIDS dataset.

        Args:
            pattern: Glob pattern for files to load

        Returns:
            Name of the created table
        """

    def parse_bids_entities(self, filepath: str | Path) -> dict[str, str]:
        """Parse BIDS entities from filename.

        Args:
            filepath: Path to BIDS file

        Returns:
            Dictionary of entity short names to values
        """
        filename = Path(filepath).name
        entities = {}

        # Extract key-value pairs using BIDS pattern: key-value_key-value_...
        # Pattern matches alphanumeric keys followed by dash and alphanumeric values
        pattern = r"([a-zA-Z0-9]+)-([a-zA-Z0-9]+)"

        for match in re.finditer(pattern, filename):
            key, value = match.groups()
            # Only keep known BIDS entities
            if key in self.entity_short_to_full:
                entities[key] = value

        return entities

    def get_entity_full_name(self, short_name: str) -> str:
        """Convert entity short name to full name.

        Args:
            short_name: Entity short name (e.g., 'sub')

        Returns:
            Full entity name (e.g., 'subject')
        """
        return self.entity_short_to_full.get(short_name, short_name)

    def get_entity_column_name(self, short_name: str, use_full_names: bool = True) -> str:
        """Get column name for entity.

        Args:
            short_name: Entity short name (e.g., 'sub')
            use_full_names: If True, use full names; otherwise use short names

        Returns:
            Column name for the entity
        """
        if use_full_names:
            return self.get_entity_full_name(short_name)
        return short_name

    def query(self, sql: str) -> duckdb.DuckDBPyRelation:
        """Execute SQL query on the connection.

        Args:
            sql: SQL query string

        Returns:
            DuckDB relation result
        """
        return self.con.execute(sql)

    def get_table_names(self) -> list[str]:
        """Get list of all tables in the database.

        Returns:
            List of table names
        """
        result = self.con.execute("SHOW TABLES").fetchall()
        return [row[0] for row in result]

    def get_statistics(self) -> dict[str, Any]:
        """Get statistics about loaded data.

        Returns:
            Dictionary with statistics
        """
        stats = {"tables": {}}

        for table_name in self.get_table_names():
            count = self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            stats["tables"][table_name] = {"row_count": count}

        return stats
