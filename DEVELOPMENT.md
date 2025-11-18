# Development Guide

## Setting up a development environment

1. Clone the repository:
```bash
git clone https://github.com/con/bids-duckdb.git
cd bids-duckdb
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install in development mode with all dependencies:
```bash
pip install -e ".[all]"
```

## Running tests

Run all tests:
```bash
pytest
```

Run tests with coverage:
```bash
pytest --cov=bids_duckdb --cov-report=html
```

## Code quality

### Linting

Run linters:
```bash
tox -e lint
```

Or manually:
```bash
flake8 bids_duckdb
codespell bids_duckdb
```

### Type checking

Run type checking:
```bash
tox -e typing
```

Or manually:
```bash
mypy bids_duckdb
```

### Pre-commit hooks

Install pre-commit hooks:
```bash
pre-commit install
```

Run pre-commit on all files:
```bash
pre-commit run --all-files
```

## Testing with tox

Run all tests in all environments:
```bash
tox
```

Run specific environment:
```bash
tox -e lint
tox -e typing
tox -e py39
```

## Building the package

Build source distribution and wheel:
```bash
python -m build
```

## Version management

This project uses [versioneer](https://github.com/warner/python-versioneer) for automatic version management based on git tags.

To create a new release:
1. Create and push a git tag:
```bash
git tag -a v0.1.0 -m "Release version 0.1.0"
git push origin v0.1.0
```

2. The version will be automatically set based on the tag.
