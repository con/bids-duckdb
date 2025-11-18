"""Basic tests for the bids_duckdb package."""

import bids_duckdb


def test_import():
    """Test that the package can be imported."""
    assert bids_duckdb is not None


def test_version():
    """Test that version is set."""
    assert hasattr(bids_duckdb, "__version__")
    assert bids_duckdb.__version__ is not None
