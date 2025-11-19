"""Tests for base loader functionality."""

import tempfile
from pathlib import Path

import pytest

from bids_duckdb.approach1_sql import SQLRegexLoader
from bids_duckdb.base import BIDSLoader


@pytest.fixture
def temp_bids_dir():
    """Create a temporary BIDS directory structure."""
    with tempfile.TemporaryDirectory() as tmpdir:
        bids_root = Path(tmpdir)

        # Create minimal BIDS structure
        (bids_root / "dataset_description.json").write_text('{"Name": "Test Dataset"}')

        # Create a participant
        sub_dir = bids_root / "sub-01"
        sub_dir.mkdir()

        # Create func directory
        func_dir = sub_dir / "func"
        func_dir.mkdir()

        # Create a TSV file with BIDS naming
        events_file = func_dir / "sub-01_task-rest_events.tsv"
        events_file.write_text("onset\tduration\ttrial_type\n1.0\t2.0\tA\n3.0\t2.0\tB\n")

        # Create another subject with session
        sub_dir2 = bids_root / "sub-02" / "ses-pre" / "func"
        sub_dir2.mkdir(parents=True)

        events_file2 = sub_dir2 / "sub-02_ses-pre_task-rest_run-01_events.tsv"
        events_file2.write_text("onset\tduration\ttrial_type\n1.0\t2.0\tX\n")

        # Create participants.tsv
        participants_file = bids_root / "participants.tsv"
        participants_file.write_text(
            "participant_id\tage\tsex\nsub-01\t25\tM\nsub-02\t30\tF\n"
        )

        yield bids_root


def test_loader_initialization(temp_bids_dir):
    """Test loader initialization."""
    loader = SQLRegexLoader(temp_bids_dir)
    assert loader.bids_root == temp_bids_dir
    assert loader.con is not None
    assert loader.schema is not None


def test_loader_initialization_with_invalid_path():
    """Test loader initialization with invalid path."""
    with pytest.raises(FileNotFoundError):
        SQLRegexLoader("/nonexistent/path")


def test_parse_bids_entities(temp_bids_dir):
    """Test parsing BIDS entities from filename."""
    loader = SQLRegexLoader(temp_bids_dir)

    # Parse simple filename
    entities = loader.parse_bids_entities("sub-01_task-rest_events.tsv")
    assert entities["sub"] == "01"
    assert entities["task"] == "rest"

    # Parse complex filename
    entities = loader.parse_bids_entities("sub-02_ses-pre_task-rest_run-01_events.tsv")
    assert entities["sub"] == "02"
    assert entities["ses"] == "pre"
    assert entities["task"] == "rest"
    assert entities["run"] == "01"


def test_get_entity_full_name(temp_bids_dir):
    """Test converting short names to full names."""
    loader = SQLRegexLoader(temp_bids_dir)

    assert loader.get_entity_full_name("sub") == "subject"
    assert loader.get_entity_full_name("ses") == "session"
    assert loader.get_entity_full_name("task") == "task"
    assert loader.get_entity_full_name("unknown") == "unknown"


def test_get_entity_column_name(temp_bids_dir):
    """Test getting column names for entities."""
    loader = SQLRegexLoader(temp_bids_dir)

    # With full names
    assert loader.get_entity_column_name("sub", use_full_names=True) == "subject"

    # With short names
    assert loader.get_entity_column_name("sub", use_full_names=False) == "sub"


def test_query_execution(temp_bids_dir):
    """Test executing SQL queries."""
    loader = SQLRegexLoader(temp_bids_dir)

    # Simple query
    result = loader.query("SELECT 1 as test").fetchone()
    assert result[0] == 1


def test_get_table_names(temp_bids_dir):
    """Test getting table names."""
    loader = SQLRegexLoader(temp_bids_dir)

    # Initially empty
    tables = loader.get_table_names()
    assert isinstance(tables, list)

    # Create a table
    loader.con.execute("CREATE TABLE test (id INTEGER)")
    tables = loader.get_table_names()
    assert "test" in tables
