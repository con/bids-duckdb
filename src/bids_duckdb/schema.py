"""BIDS schema loader and entity mapping."""

import logging
from functools import lru_cache
from typing import Any

import requests
import yaml

logger = logging.getLogger(__name__)


class BIDSSchema:
    """Load and parse BIDS schema for entity definitions."""

    def __init__(self, schema_version: str = "master") -> None:
        """Initialize BIDS schema loader.

        Args:
            schema_version: Git branch/tag of BIDS specification (default: "master")
        """
        self.schema_version = schema_version
        self._entities: dict[str, dict[str, Any]] | None = None
        self._entity_short_to_full: dict[str, str] | None = None

    @property
    def base_url(self) -> str:
        """Get base URL for BIDS schema."""
        return f"https://raw.githubusercontent.com/bids-standard/bids-specification/{self.schema_version}/src/schema/objects"

    def load_entities(self) -> dict[str, dict[str, Any]]:
        """Load entity definitions from BIDS schema.

        Returns:
            Dictionary mapping entity full names to their definitions
        """
        if self._entities is not None:
            return self._entities

        url = f"{self.base_url}/entities.yaml"
        logger.info(f"Loading BIDS entities from {url}")

        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            self._entities = yaml.safe_load(response.text)
            logger.info(f"Loaded {len(self._entities)} entities from BIDS schema")
            return self._entities
        except Exception as e:
            logger.exception(f"Failed to load BIDS schema: {e}")
            # Fall back to minimal entity set
            return self._get_fallback_entities()

    def _get_fallback_entities(self) -> dict[str, dict[str, Any]]:
        """Provide fallback entity definitions if schema loading fails."""
        logger.warning("Using fallback entity definitions")
        return {
            "subject": {"name": "sub", "display_name": "Subject", "type": "string"},
            "session": {"name": "ses", "display_name": "Session", "type": "string"},
            "task": {"name": "task", "display_name": "Task", "type": "string"},
            "acquisition": {"name": "acq", "display_name": "Acquisition", "type": "string"},
            "ceagent": {"name": "ce", "display_name": "Contrast Enhancing Agent", "type": "string"},
            "reconstruction": {"name": "rec", "display_name": "Reconstruction", "type": "string"},
            "direction": {"name": "dir", "display_name": "Phase Encoding Direction", "type": "string"},
            "run": {"name": "run", "display_name": "Run", "type": "string", "format": "index"},
            "echo": {"name": "echo", "display_name": "Echo", "type": "string", "format": "index"},
            "space": {"name": "space", "display_name": "Space", "type": "string"},
            "description": {"name": "desc", "display_name": "Description", "type": "string"},
        }

    def get_entity_mapping(self) -> dict[str, str]:
        """Get mapping from short entity names to full names.

        Returns:
            Dictionary mapping short names (e.g., 'sub') to full names (e.g., 'subject')
        """
        if self._entity_short_to_full is not None:
            return self._entity_short_to_full

        entities = self.load_entities()
        self._entity_short_to_full = {}

        for full_name, definition in entities.items():
            short_name = definition.get("name")
            if short_name:
                self._entity_short_to_full[short_name] = full_name

        logger.info(f"Created mapping for {len(self._entity_short_to_full)} entities")
        return self._entity_short_to_full

    def get_entity_display_names(self) -> dict[str, str]:
        """Get mapping from short entity names to display names.

        Returns:
            Dictionary mapping short names (e.g., 'sub') to display names (e.g., 'Subject')
        """
        entities = self.load_entities()
        mapping = {}

        for definition in entities.values():
            short_name = definition.get("name")
            display_name = definition.get("display_name", definition.get("name", "").title())
            if short_name:
                mapping[short_name] = display_name

        return mapping

    def get_all_entity_short_names(self) -> list[str]:
        """Get list of all entity short names.

        Returns:
            List of entity short names (e.g., ['sub', 'ses', 'task', ...])
        """
        return list(self.get_entity_mapping().keys())


@lru_cache(maxsize=1)
def get_default_schema() -> BIDSSchema:
    """Get cached default BIDS schema instance.

    Returns:
        BIDSSchema instance with loaded entities
    """
    schema = BIDSSchema()
    schema.load_entities()  # Pre-load
    return schema
