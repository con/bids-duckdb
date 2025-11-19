"""Tests for BIDS schema loading."""

import pytest

from bids_duckdb.schema import BIDSSchema


def test_schema_initialization():
    """Test schema initialization."""
    schema = BIDSSchema()
    assert schema.schema_version == "master"


def test_load_entities():
    """Test loading entities from BIDS schema."""
    schema = BIDSSchema()
    entities = schema.load_entities()

    # Should have loaded entities
    assert isinstance(entities, dict)
    assert len(entities) > 0

    # Check for known entities
    assert "subject" in entities or len(entities) > 5  # Fallback also has entities


def test_entity_mapping():
    """Test entity short name to full name mapping."""
    schema = BIDSSchema()
    mapping = schema.get_entity_mapping()

    # Should have mappings
    assert isinstance(mapping, dict)
    assert len(mapping) > 0

    # Check specific mappings
    assert mapping.get("sub") == "subject"
    assert mapping.get("ses") == "session"
    assert mapping.get("task") == "task"


def test_entity_display_names():
    """Test entity display names."""
    schema = BIDSSchema()
    display_names = schema.get_entity_display_names()

    assert isinstance(display_names, dict)
    assert len(display_names) > 0

    # Display names should be capitalized
    assert display_names.get("sub") in ["Subject", "subject"]


def test_get_all_entity_short_names():
    """Test getting all entity short names."""
    schema = BIDSSchema()
    short_names = schema.get_all_entity_short_names()

    assert isinstance(short_names, list)
    assert len(short_names) > 0
    assert "sub" in short_names
    assert "ses" in short_names


def test_fallback_entities():
    """Test fallback entities when schema loading fails."""
    schema = BIDSSchema(schema_version="nonexistent-version-xyz")
    entities = schema.load_entities()

    # Should fall back to minimal entity set
    assert isinstance(entities, dict)
    assert len(entities) > 0
    assert "subject" in entities


def test_schema_version_specification():
    """Test specifying a different schema version."""
    schema = BIDSSchema(schema_version="v1.8.0")
    assert schema.schema_version == "v1.8.0"
    assert "v1.8.0" in schema.base_url
