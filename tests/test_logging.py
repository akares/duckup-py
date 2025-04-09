import io
import logging
import tempfile
from pathlib import Path
from typing import Generator, Tuple

import duckdb
import pytest
from duckdb import DuckDBPyConnection
from duckup.migrate import downgrade, upgrade

# Configure root logger for tests
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


@pytest.fixture
def temp_db() -> Generator[Tuple[DuckDBPyConnection, str], None, None]:
    """Create a temporary database file."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "test.db"
        conn = duckdb.connect(str(db_path))
        yield conn, str(db_path)
        conn.close()


@pytest.fixture
def temp_migrations_dir() -> Generator[str, None, None]:
    """Create a temporary directory with a simple migration."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)

        # Create a simple migration
        migration = path / "001_test_migration.py"
        with open(migration, "w") as f:
            f.write(
                """
def upgrade(conn):
    conn.execute("CREATE TABLE test_table (id INTEGER)")
    conn.execute("INSERT INTO test_table VALUES (1)")

def downgrade(conn):
    conn.execute("DROP TABLE test_table")
"""
            )

        yield str(path)


def test_migration_logging(
    temp_db: Tuple[DuckDBPyConnection, str], temp_migrations_dir: str
) -> None:
    """Test that migrations are properly logged."""
    conn, _ = temp_db

    # Configure logging to capture output
    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler.setFormatter(formatter)

    # Get the root logger and add our handler
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # Remove any existing handlers
    for h in logger.handlers[:]:
        logger.removeHandler(h)
    logger.addHandler(handler)

    try:
        # Perform upgrade
        upgrade(conn, temp_migrations_dir)

        # Check log output for upgrade
        log_output = log_stream.getvalue()
        assert "INFO - Current database version: 0" in log_output
        assert (
            "INFO - Applying migration: test_migration (version 001)"
            in log_output
        )
        assert "INFO - Database upgrade complete" in log_output

        # Clear log for downgrade test
        log_stream.truncate(0)
        log_stream.seek(0)

        # Perform downgrade
        downgrade(conn, temp_migrations_dir, target_version=0)

        # Check log output for downgrade
        log_output = log_stream.getvalue()
        assert "INFO - Current database version: 1" in log_output
        assert (
            "INFO - Reverting migration: test_migration (version 001)"
            in log_output
        )
        assert "INFO - Database downgrade complete" in log_output
    finally:
        # Clean up handler
        logger.removeHandler(handler)


def test_debug_logging_level(
    temp_db: Tuple[DuckDBPyConnection, str], temp_migrations_dir: str
) -> None:
    """Test that debug level logging works properly."""
    conn, _ = temp_db

    # Configure logging to capture output
    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler.setFormatter(formatter)

    # Get the root logger and add our handler
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    # Perform upgrade
    upgrade(conn, temp_migrations_dir)

    # Check for debug messages
    log_output = log_stream.getvalue()
    assert "DEBUG - Loading migrations from directory:" in log_output
    assert "DEBUG - Found migration file:" in log_output

    # Clean up handler
    logger.removeHandler(handler)
    logger.setLevel(logging.INFO)


def test_custom_format(
    temp_db: Tuple[DuckDBPyConnection, str], temp_migrations_dir: str
) -> None:
    """Test that custom format works properly."""
    conn, _ = temp_db

    # Configure logging with custom format
    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter("QUACKUP_TEST - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)

    # Get the root logger and add our handler
    logger = logging.getLogger()
    logger.addHandler(handler)

    # Perform upgrade
    upgrade(conn, temp_migrations_dir)

    # Check for custom format
    log_output = log_stream.getvalue()
    assert "QUACKUP_TEST - INFO - Current database version: 0" in log_output

    # Clean up handler
    logger.removeHandler(handler)


def test_detailed_debug_logging(
    temp_db: Tuple[DuckDBPyConnection, str], temp_migrations_dir: str
) -> None:
    """Test detailed debug logging for migrations."""
    conn, _ = temp_db

    # Configure logging to capture all debug output
    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler.setFormatter(formatter)

    # Get the root logger and add our handler
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    # Perform upgrade
    upgrade(conn, temp_migrations_dir)

    # Check log output for upgrade debug messages
    log_output = log_stream.getvalue()

    # Check for database details
    assert "DEBUG - Database connection details:" in log_output

    # Check for migration list details
    assert "DEBUG - Migrations to apply:" in log_output
    assert "test_migration (version 001)" in log_output

    # Check for execution timing
    assert (
        "DEBUG - Migration test_migration (version 001) executed in"
        in log_output
    )
    assert "seconds" in log_output

    # Check for transaction logging
    assert "DEBUG - Beginning transaction for migration" in log_output
    assert "DEBUG - Updated database version to 1" in log_output

    # Clear log for downgrade test
    log_stream.truncate(0)
    log_stream.seek(0)

    # Perform downgrade
    downgrade(conn, temp_migrations_dir, target_version=0)

    # Check log output for downgrade debug messages
    log_output = log_stream.getvalue()

    # Check for downgrade specific debug messages
    assert "DEBUG - Migrations to downgrade:" in log_output
    assert (
        "DEBUG - Migration test_migration (version 001) downgrade executed in"
        in log_output
    )
    assert "DEBUG - Found previous migration:" in log_output
    assert "DEBUG - Updated database version to 0" in log_output

    # Clean up handler
    logger.removeHandler(handler)
    logger.setLevel(logging.INFO)


def test_format_string_parameter() -> None:
    """Test format_string parameter with custom handler."""
    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    custom_format = "%(levelname)s - TEST - %(message)s"
    formatter = logging.Formatter(custom_format)
    handler.setFormatter(formatter)

    # Get the root logger and add our handler
    logger = logging.getLogger()
    logger.addHandler(handler)

    logger.info("Test message")

    assert "INFO - TEST - Test message" in log_stream.getvalue()
    assert handler.formatter._fmt == custom_format

    # Clean up handler
    logger.removeHandler(handler)


def test_format_string_with_default_handler() -> None:
    """Test format_string with default handler."""
    logger = logging.getLogger()
    original_handlers = logger.handlers.copy()
    original_level = logger.level

    try:
        custom_format = "%(levelname)s :: %(message)s"
        formatter = logging.Formatter(custom_format)
        for handler in logger.handlers:
            handler.setFormatter(formatter)

        # Test with actual logging
        log_stream = io.StringIO()
        handler = logging.StreamHandler(log_stream)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        logger.info("Test message")
        assert "INFO :: Test message" in log_stream.getvalue()

        # Clean up test handler
        logger.removeHandler(handler)
    finally:
        # Restore original handlers and formatters
        for h in logger.handlers[:]:
            logger.removeHandler(h)
        for h in original_handlers:
            logger.addHandler(h)
        logger.setLevel(original_level)
