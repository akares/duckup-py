# Duckup Examples

This directory contains example scripts demonstrating how to use the duckup package.

## Basic Migration Example

The `example_migration.py` script demonstrates the core functionality of duckup:

1. Running database upgrades
2. Running database downgrades

### How to Run

1. Make sure you have duckup installed:
   ```bash
   pip install duckup
   ```

2. Run the example script:
   ```bash
   python example_migration.py
   ```

### What the Example Does

1. Connects to a DuckDB database (`example.duckdb`)
2. Runs the migration to create a users table
3. Runs the downgrade to remove the users table

### Expected Output

You should see log messages indicating:
- The successful upgrade
- The successful downgrade

### Files Used

- `example.duckdb`: The DuckDB database file (created during execution)
- `example_migrations/001_create_users.py`: The migration module

### Notes

- The example uses a simple users table schema
- All operations are logged using Python's logging module
- The script handles errors gracefully and provides informative messages
- All files are accessed relative to the example script's location
