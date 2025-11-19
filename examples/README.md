# BIDS DuckDB Examples

This directory contains examples demonstrating different approaches for loading BIDS datasets into DuckDB.

## Files

- **basic_usage.py**: Examples of using each approach individually
- **benchmark_comparison.py**: Performance comparison of all three approaches

## Running the Examples

### Basic Usage

Edit `basic_usage.py` to set your BIDS dataset path, then uncomment the examples you want to run:

```bash
python examples/basic_usage.py
```

### Benchmark Comparison

Run the benchmark suite on your BIDS dataset:

```bash
python examples/benchmark_comparison.py /path/to/your/bids/dataset
```

Or use the default path specified in the script:

```bash
python examples/benchmark_comparison.py
```

## Expected Output

The benchmark script will output:

1. **Load times** for each approach
2. **Query performance** for various operations
3. **Row counts** and table statistics
4. **Comparison table** showing relative performance
5. **Recommendations** based on the results

## Interpreting Results

- **SQL Regex Approach**: Usually fastest for simple datasets, minimal overhead
- **Python Preprocessor**: More control, better for complex BIDS patterns
- **Table Function**: Memory efficient, good for selective queries

Choose the approach that best fits your use case based on:
- Dataset size
- Query patterns (full scans vs selective)
- Memory constraints
- Need for custom parsing logic
