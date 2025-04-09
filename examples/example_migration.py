#!/usr/bin/env python3
"""
Basic example demonstrating how to use duckup for database migrations.

This example shows:
1. Setting up logging
2. Running database upgrades
3. Running database downgrades
"""

import logging
import pathlib

import duckdb
import duckup

# Configure logging for the example
logger = logging.getLogger("example")
logging.basicConfig(
    format="%(name)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)

# Configure logging for duckup
logging.getLogger("duckup").setLevel(logging.INFO)


def main(db_path: str, migration_dir: str) -> None:
    """Run the example migration workflow."""
    try:
        # Connect to the database
        conn = duckdb.connect(db_path)

        # Run upgrade
        logger.info("Running upgrade...")
        duckup.upgrade(conn, migration_dir)

        # Read the created table
        logger.info("Reading table...")
        result = conn.execute("SELECT * FROM users")
        logger.info(result.fetchall())

        # Run downgrade
        # NOTE: always pass target_version explicitly when downgrading
        logger.info("Running downgrade...")
        duckup.downgrade(conn, migration_dir, target_version=0)

        # Ensure the table is dropped
        logger.info("Checking if table is dropped...")
        tables = conn.execute("SHOW TABLES").fetchall()
        assert "users" not in [table[0] for table in tables]
        logger.info("Table is dropped")

        # Close the connection
        conn.close()

        logger.info("Example completed successfully!")

    except duckup.MigrationError as e:
        logger.error(f"Migration error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


if __name__ == "__main__":
    script_dir = pathlib.Path(__file__).parent
    migrations_dir = script_dir / "example_migrations"
    db_path = script_dir / "example.duckdb"

    main(
        db_path=str(db_path),
        migration_dir=str(migrations_dir),
    )
