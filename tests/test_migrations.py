import os
import tempfile
from pathlib import Path
from typing import Generator, Tuple
from unittest.mock import MagicMock, patch

import duckdb
import pytest
from duckdb import DuckDBPyConnection
from duckup import (
    MigrationDirectoryError,
    MigrationFileError,
    MigrationVersionError,
    downgrade,
    upgrade,
)
from duckup.queries import transaction


@pytest.fixture
def temp_db() -> Generator[Tuple[DuckDBPyConnection, str], None, None]:
    """Create a temporary database file."""
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as tmp_dir:
        db_path = os.path.join(tmp_dir, "test.db")
        # Create a fresh DB
        conn = duckdb.connect(db_path)
        yield conn, db_path
        conn.close()


@pytest.fixture
def temp_migrations_dir() -> Generator[str, None, None]:
    """Create a temporary directory for migrations."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)

        # Create a first migration
        migration1 = path / "001_create_users.py"
        with open(migration1, "w") as f:
            f.write(
                """
def upgrade(conn):
    conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")

def downgrade(conn):
    conn.execute("DROP TABLE users")
"""
            )

        # Create a second migration
        migration2 = path / "002_add_email.py"
        with open(migration2, "w") as f:
            f.write(
                """
def upgrade(conn):
    conn.execute("ALTER TABLE users ADD COLUMN email VARCHAR")
    conn.execute("UPDATE users SET email = 'alice@example.com' WHERE id = 1")
    conn.execute("UPDATE users SET email = 'bob@example.com' WHERE id = 2")

def downgrade(conn):
    # In DuckDB we can't drop a column, so we need to recreate the table
    conn.execute("CREATE TABLE users_temp (id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO users_temp SELECT id, name FROM users")
    conn.execute("DROP TABLE users")
    conn.execute("ALTER TABLE users_temp RENAME TO users")
"""
            )

        yield tmp_dir


@pytest.fixture
def empty_migrations_dir() -> Generator[str, None, None]:
    """Create an empty temporary directory for migrations."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield tmp_dir


def test_upgrade(
    temp_db: Tuple[DuckDBPyConnection, str], temp_migrations_dir: str
) -> None:
    """Test upgrading a database."""
    conn, db_path = temp_db

    # Apply migrations
    upgrade(conn, temp_migrations_dir)

    # Check that the users table exists with the right columns
    result = conn.execute("SELECT * FROM users").fetchall()
    assert len(result) == 2
    assert len(result[0]) == 3  # id, name, email

    # Check that the data is correct
    assert conn.execute(
        "SELECT name, email FROM users WHERE id = 1"
    ).fetchone() == ("Alice", "alice@example.com")
    assert conn.execute(
        "SELECT name, email FROM users WHERE id = 2"
    ).fetchone() == ("Bob", "bob@example.com")

    # Check the migrations table
    assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 2


def test_upgrade_with_target(
    temp_db: Tuple[DuckDBPyConnection, str], temp_migrations_dir: str
) -> None:
    """Test upgrading a database to a specific version."""
    conn, db_path = temp_db

    # Apply only the first migration
    upgrade(conn, temp_migrations_dir, target_version=1)

    # Check that the users table exists with only the original columns
    result = conn.execute("SELECT * FROM users").fetchall()
    assert len(result) == 2
    assert len(result[0]) == 2  # id, name, no email

    # Check the migrations table
    assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 1


def test_downgrade(
    temp_db: Tuple[DuckDBPyConnection, str], temp_migrations_dir: str
) -> None:
    """Test downgrading a database."""
    conn, db_path = temp_db

    # First upgrade to latest
    upgrade(conn, temp_migrations_dir)

    # Now downgrade to version 1
    downgrade(conn, temp_migrations_dir, "migrations", target_version=1)

    # Check that the users table exists with only the original columns
    result = conn.execute("SELECT * FROM users").fetchall()
    assert len(result) == 2
    assert len(result[0]) == 2  # Only id and name, no email

    # Check that the data is still correct
    assert (
        conn.execute("SELECT name FROM users WHERE id = 1").fetchone()[0]
        == "Alice"
    )
    assert (
        conn.execute("SELECT name FROM users WHERE id = 2").fetchone()[0]
        == "Bob"
    )

    # Check the migrations table
    assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 1


def test_full_downgrade(
    temp_db: Tuple[DuckDBPyConnection, str], temp_migrations_dir: str
) -> None:
    """Test downgrading a database to version 0 (initial state)."""
    conn, db_path = temp_db

    # First upgrade to latest
    upgrade(conn, temp_migrations_dir)

    # Now downgrade to version 0
    downgrade(conn, temp_migrations_dir, "migrations", target_version=0)

    # Check that the users table no longer exists
    with pytest.raises(duckdb.Error):
        conn.execute("SELECT * FROM users")

    # Check the migrations table
    assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 0


def test_downgrade_default_behavior(
    temp_db: Tuple[DuckDBPyConnection, str], temp_migrations_dir: str
) -> None:
    """Test downgrade when target_version is not specified."""
    conn, db_path = temp_db

    # First upgrade to latest
    upgrade(conn, temp_migrations_dir)

    # Downgrade without specifying target_version should not change version
    downgrade(conn, temp_migrations_dir)

    # Check that we're still at the latest version
    assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 2


def test_invalid_upgrade(
    temp_db: Tuple[DuckDBPyConnection, str], temp_migrations_dir: str
) -> None:
    """Test upgrading to an invalid version."""
    conn, db_path = temp_db

    # Apply migrations
    upgrade(conn, temp_migrations_dir)

    # Try to upgrade to a lower version
    with pytest.raises(MigrationVersionError):
        upgrade(conn, temp_migrations_dir, target_version=1)


def test_invalid_downgrade(
    temp_db: Tuple[DuckDBPyConnection, str], temp_migrations_dir: str
) -> None:
    """Test downgrading to an invalid version."""
    conn, db_path = temp_db

    # Apply first migration only
    upgrade(conn, temp_migrations_dir, target_version=1)

    # Try to downgrade to a higher version
    with pytest.raises(MigrationVersionError):
        downgrade(conn, temp_migrations_dir, "migrations", target_version=2)


def test_empty_migrations_dir(
    temp_db: Tuple[DuckDBPyConnection, str], empty_migrations_dir: str
) -> None:
    """Test behavior with an empty migrations directory."""
    conn, db_path = temp_db

    # Apply migrations (should do nothing)
    upgrade(conn, empty_migrations_dir)

    # Check if migrations table exists
    query = "SELECT * FROM duckdb_tables WHERE table_name = 'migrations'"
    result = conn.execute(query).fetchall()
    if result:
        # If table exists, check the version
        assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 0
    else:
        # If table doesn't exist, that's acceptable for empty migrations dir
        pass

    # Downgrade should also do nothing
    downgrade(conn, empty_migrations_dir)

    # Same check after downgrade
    result = conn.execute(query).fetchall()
    if result:
        assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 0


def test_missing_migrations_dir(
    temp_db: Tuple[DuckDBPyConnection, str],
) -> None:
    """Test behavior with a non-existent migrations directory."""
    conn, db_path = temp_db

    # Try to upgrade with a non-existent directory
    with pytest.raises(Exception):
        upgrade(conn, "/path/that/does/not/exist")


def test_invalid_migration_file(
    temp_db: Tuple[DuckDBPyConnection, str], empty_migrations_dir: str
) -> None:
    """Test behavior with an invalid migration file."""
    conn, db_path = temp_db

    # Create an invalid migration file (missing required functions)
    path = Path(empty_migrations_dir)
    invalid_migration = path / "001_invalid.py"
    with open(invalid_migration, "w") as f:
        f.write(
            """
def some_other_function(conn):
    pass
"""
        )

    # Try to upgrade with the invalid migration
    with pytest.raises(MigrationFileError):
        upgrade(conn, empty_migrations_dir)


def test_upgrade_target_none(
    temp_db: Tuple[DuckDBPyConnection, str], temp_migrations_dir: str
) -> None:
    """Test that upgrade with target_version=None uses maximum version."""
    conn, db_path = temp_db

    # Explicitly call with target_version=None
    upgrade(conn, temp_migrations_dir, target_version=None)

    # Check that we upgraded to the maximum version (2)
    assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 2

    # Check that both migrations were applied
    result = conn.execute("SELECT * FROM users").fetchall()
    assert len(result) == 2
    assert len(result[0]) == 3  # id, name, email

    # Create a third migration
    path = Path(temp_migrations_dir)
    migration3 = path / "003_add_admin.py"
    with open(migration3, "w") as f:
        f.write(
            """
def upgrade(conn):
    conn.execute("ALTER TABLE users ADD COLUMN is_admin BOOLEAN DEFAULT false")
    conn.execute("UPDATE users SET is_admin = true WHERE id = 1")

def downgrade(conn):
    # In DuckDB we can't drop a column, so we need to recreate the table
    conn.execute("CREATE TABLE users_temp (id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO users_temp SELECT id, name FROM users")
    conn.execute("DROP TABLE users")
    conn.execute("ALTER TABLE users_temp RENAME TO users")
"""
        )

    # Upgrade again with target_version=None
    upgrade(conn, temp_migrations_dir, target_version=None)

    # Check that we upgraded to the new maximum version (3)
    assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 3

    # Check that the third migration was applied
    result = conn.execute("SELECT * FROM users").fetchall()
    assert len(result) == 2
    assert len(result[0]) == 4  # id, name, email, is_admin
    assert (
        conn.execute("SELECT is_admin FROM users WHERE id = 1").fetchone()[0]
        is True
    )


def test_empty_migrations_with_target_none(
    temp_db: Tuple[DuckDBPyConnection, str], empty_migrations_dir: str
) -> None:
    """Test upgrade with empty migrations and target_version=None."""
    conn, db_path = temp_db

    # Apply migrations with explicit target_version=None
    upgrade(conn, empty_migrations_dir, target_version=None)

    # Check if migrations table exists
    query = "SELECT * FROM duckdb_tables WHERE table_name = 'migrations'"
    result = conn.execute(query).fetchall()
    if result:
        # If table exists, check the version
        assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 0
    else:
        # Empty migrations directory might not create the table
        pass


def test_downgrade_skip_higher_version(
    temp_db: Tuple[DuckDBPyConnection, str], temp_migrations_dir: str
) -> None:
    """Test that downgrade skips migrations with version > db_version."""
    conn, db_path = temp_db

    # First upgrade to version 1 only
    upgrade(conn, temp_migrations_dir, target_version=1)
    assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 1

    # Create a higher version migration file that wouldn't be applied yet
    path = Path(temp_migrations_dir)
    migration3 = path / "003_higher_version.py"
    with open(migration3, "w") as f:
        f.write(
            """
def upgrade(conn):
    conn.execute("ALTER TABLE users ADD COLUMN test VARCHAR")

def downgrade(conn):
    pass
"""
        )

    # Now downgrade to version 0
    # This should skip the version 3 migration since db_version is only 1
    downgrade(conn, temp_migrations_dir, target_version=0)

    # Verify we're at version 0
    assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 0

    # Verify the table no longer exists (downgraded properly)
    with pytest.raises(duckdb.Error):
        conn.execute("SELECT * FROM users")


def test_non_migration_files(
    temp_db: Tuple[DuckDBPyConnection, str], temp_migrations_dir: str
) -> None:
    """Test that non-migration files are ignored."""
    conn, db_path = temp_db

    # Add a file that doesn't match the migration pattern
    path = Path(temp_migrations_dir)
    non_migration = path / "README.md"
    with open(non_migration, "w") as f:
        f.write("# Migrations\n\nThis directory contains database migrations.")

    # Add another file with wrong extension
    wrong_ext = path / "003_wrong_extension.txt"
    with open(wrong_ext, "w") as f:
        f.write(
            """
def upgrade(conn):
    pass

def downgrade(conn):
    pass
"""
        )

    # Run upgrade - should work normally despite non-migration files
    upgrade(conn, temp_migrations_dir)

    # Check that we upgraded to version 2 (ignoring non-migration files)
    assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 2


@patch("importlib.util.spec_from_file_location")
def test_invalid_module_spec(
    mock_spec_from_file: MagicMock,
    temp_db: Tuple[DuckDBPyConnection, str],
    temp_migrations_dir: str,
) -> None:
    """Test handling of invalid module specs."""
    conn, db_path = temp_db

    # Create a mock spec with loader=None
    mock_spec = MagicMock()
    mock_spec.loader = None
    mock_spec_from_file.return_value = mock_spec

    # Run upgrade - should work but skip the files with invalid specs
    upgrade(conn, temp_migrations_dir)

    # Check if migrations table exists
    query = "SELECT * FROM duckdb_tables WHERE table_name = 'migrations'"
    result = conn.execute(query).fetchall()
    if result:
        # If table exists, version should be 0
        assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 0

    # Verify that no users table was created since migrations were skipped
    with pytest.raises(duckdb.Error):
        conn.execute("SELECT * FROM users")


def test_transaction_error_handling(
    temp_db: Tuple[DuckDBPyConnection, str],
) -> None:
    """Test transaction error handling in queries module."""
    conn, db_path = temp_db

    # Create a test table
    conn.execute("CREATE TABLE test_table (id INTEGER)")

    # Test that transaction commits successfully
    with transaction(conn):
        conn.execute("INSERT INTO test_table VALUES (1)")

    # Verify the data was committed
    assert conn.execute("SELECT * FROM test_table").fetchone()[0] == 1

    # Test that transaction rolls back on exception
    with pytest.raises(duckdb.Error):
        with transaction(conn):
            conn.execute("INSERT INTO test_table VALUES (2)")
            # This should cause an error
            conn.execute("SELECT * FROM nonexistent_table")

    # Verify that the second insert was rolled back
    assert len(conn.execute("SELECT * FROM test_table").fetchall()) == 1
    assert conn.execute("SELECT * FROM test_table").fetchone()[0] == 1


def test_multistep_downgrade_versioning(
    temp_db: Tuple[DuckDBPyConnection, str],
) -> None:
    """Test correct version calculation during multistep downgrades."""
    conn, db_path = temp_db

    # Create a migrations directory with 3 migrations
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)

        # Create three migrations: 001, 003, 005 (intentional gaps)
        migration1 = path / "001_first.py"
        with open(migration1, "w") as f:
            f.write(
                """
def upgrade(conn):
    conn.execute("CREATE TABLE test1 (id INTEGER)")
    conn.execute("INSERT INTO test1 VALUES (1)")

def downgrade(conn):
    conn.execute("DROP TABLE test1")
"""
            )

        migration2 = path / "003_second.py"
        with open(migration2, "w") as f:
            f.write(
                """
def upgrade(conn):
    conn.execute("CREATE TABLE test2 (id INTEGER)")
    conn.execute("INSERT INTO test2 VALUES (2)")

def downgrade(conn):
    conn.execute("DROP TABLE test2")
"""
            )

        migration3 = path / "005_third.py"
        with open(migration3, "w") as f:
            f.write(
                """
def upgrade(conn):
    conn.execute("CREATE TABLE test3 (id INTEGER)")
    conn.execute("INSERT INTO test3 VALUES (3)")

def downgrade(conn):
    conn.execute("DROP TABLE test3")
"""
            )

        # Upgrade to the latest version (5)
        upgrade(conn, tmp_dir)

        # Verify we're at version 5
        assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 5

        # Check all tables exist
        assert conn.execute("SELECT * FROM test1").fetchone()[0] == 1
        assert conn.execute("SELECT * FROM test2").fetchone()[0] == 2
        assert conn.execute("SELECT * FROM test3").fetchone()[0] == 3

        # Downgrade to version 1
        downgrade(conn, tmp_dir, target_version=1)

        # Verify we're at version 1
        assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 1

        # Check test1 still exists but test2 and test3 are gone
        assert conn.execute("SELECT * FROM test1").fetchone()[0] == 1
        with pytest.raises(duckdb.Error):
            conn.execute("SELECT * FROM test2")
        with pytest.raises(duckdb.Error):
            conn.execute("SELECT * FROM test3")

        # Upgrade back to version 3
        upgrade(conn, tmp_dir, target_version=3)

        # Verify we're at version 3
        assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 3

        # Check test1 and test2 exist but test3 is still gone
        assert conn.execute("SELECT * FROM test1").fetchone()[0] == 1
        assert conn.execute("SELECT * FROM test2").fetchone()[0] == 2
        with pytest.raises(duckdb.Error):
            conn.execute("SELECT * FROM test3")


def test_migrations_dir_is_file(
    temp_db: Tuple[DuckDBPyConnection, str],
) -> None:
    """Test error when migrations_dir exists but is a file, not a directory."""
    conn, db_path = temp_db

    with tempfile.NamedTemporaryFile() as tmp_file:
        # Try to use a file as migrations directory
        with pytest.raises(MigrationDirectoryError) as exc_info:
            upgrade(conn, tmp_file.name)

        # Check the error message
        assert f"{tmp_file.name} exists but is not a directory" in str(
            exc_info.value
        )

        # Also test downgrade
        with pytest.raises(MigrationDirectoryError) as exc_info:
            downgrade(conn, tmp_file.name, target_version=0)

        # Check the error message
        assert f"{tmp_file.name} exists but is not a directory" in str(
            exc_info.value
        )


def test_upgrade_already_at_target_version(
    temp_db: Tuple[DuckDBPyConnection, str], temp_migrations_dir: str
) -> None:
    """Test upgrading when database is already at the target version."""
    conn, db_path = temp_db

    # First perform initial upgrade to version 1
    upgrade(conn, temp_migrations_dir, target_version=1)

    # Verify we're at version 1
    assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 1

    # Run upgrade to version 1 again (same version)
    with patch("duckup.migrate.logger") as mock_logger:
        upgrade(conn, temp_migrations_dir, target_version=1)

        # Verify the log message is generated
        mock_logger.info.assert_any_call(
            "Database already at target version %d, no upgrade needed",
            1,
        )

    # Verify we're still at version 1 and no migrations were applied
    assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 1

    # Verify the database state is unchanged
    result = conn.execute("SELECT * FROM users").fetchall()
    assert len(result) == 2
    assert len(result[0]) == 2  # Only id and name columns


def test_no_migrations_to_apply(
    temp_db: Tuple[DuckDBPyConnection, str], temp_migrations_dir: str
) -> None:
    """Test upgrading when there are no migrations to apply."""
    conn, db_path = temp_db

    # First perform initial upgrade to version 1
    upgrade(conn, temp_migrations_dir, target_version=1)

    # Verify we're at version 1
    assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 1

    # When we upgrade to version 2, migrations in range 1 < m.version <= 2
    # should be applied. But if we have no migrations in that range,
    # to_apply will be empty. Create a modified migrations directory
    # with that property
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)

        # Create migration with version 1
        migration1 = path / "001_create_table.py"
        with open(migration1, "w") as f:
            f.write(
                """
def upgrade(conn):
    conn.execute("CREATE TABLE test_table (id INTEGER)")

def downgrade(conn):
    conn.execute("DROP TABLE test_table")
"""
            )

        # Create migration with version 3 (skipping version 2)
        migration3 = path / "003_add_column.py"
        with open(migration3, "w") as f:
            f.write(
                """
def upgrade(conn):
    conn.execute("ALTER TABLE test_table ADD COLUMN name VARCHAR")

def downgrade(conn):
    # Can't drop column in DuckDB
    conn.execute("CREATE TABLE temp_table (id INTEGER)")
    conn.execute("INSERT INTO temp_table SELECT id FROM test_table")
    conn.execute("DROP TABLE test_table")
    conn.execute("ALTER TABLE temp_table RENAME TO test_table")
"""
            )

        # Initialize database with version 1
        conn.execute("DROP TABLE IF EXISTS migrations")
        conn.execute("CREATE TABLE migrations (version INTEGER)")
        conn.execute("INSERT INTO migrations VALUES (1)")

        # Create the test table
        conn.execute("CREATE TABLE test_table (id INTEGER)")

        # Try to upgrade to version 2
        # There are no migrations with 1 < version <= 2, so to_apply is empty
        with patch("duckup.migrate.logger") as mock_logger:
            upgrade(conn, tmp_dir, target_version=2)

            # Verify the log message is generated
            mock_logger.info.assert_any_call("No migrations to apply")

        # Verify we're still at version 1
        # When there are no migrations to apply, the version isn't updated
        assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 1

        # Verify the table still exists but doesn't have the name column
        # (confirming migration 3 wasn't applied)
        result = conn.execute("PRAGMA table_info(test_table)").fetchall()
        column_names = [col[1] for col in result]
        assert "id" in column_names
        assert "name" not in column_names


def test_migration_source_read_failure(
    temp_db: Tuple[DuckDBPyConnection, str], temp_migrations_dir: str
) -> None:
    """Test graceful handling when reading migration source file fails."""
    conn, db_path = temp_db

    # Patch open to raise IOError when trying to read the migration file
    orig_open = open

    # Type annotation workaround for the complex open function signature
    # This mock function will raise IOError for .py files
    with patch(
        "builtins.open",
        side_effect=lambda f, *a, **k: (
            orig_open(f, *a, **k)
            if not str(f).endswith(".py")
            else exec('raise IOError("Failed to read file")')
        ),
    ):
        with patch("duckup.migrate.logger") as mock_logger:
            # Should complete successfully despite file read errors
            upgrade(conn, temp_migrations_dir)

            # Verify the debug log message is generated
            mock_logger.debug.assert_any_call(
                "Could not read source for migration %s (version %03d)",
                "create_users",  # First migration name
                1,  # First migration version
            )

    # Verify migrations were still successfully applied
    assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 2

    # Verify the table structure is correct
    result = conn.execute("SELECT * FROM users").fetchall()
    assert len(result) == 2
    assert len(result[0]) == 3  # id, name, email columns


def test_migration_execution_error(
    temp_db: Tuple[DuckDBPyConnection, str], empty_migrations_dir: str
) -> None:
    """Test error handling when a migration execution fails."""
    conn, db_path = temp_db

    # Create a migration that will fail during execution
    path = Path(empty_migrations_dir)
    failing_migration = path / "001_failing_migration.py"
    with open(failing_migration, "w") as f:
        f.write(
            """
def upgrade(conn):
    # This will raise a SQL error
    conn.execute("CREATE TABLE valid_table (id INTEGER)")
    conn.execute("SELECT * FROM nonexistent_table")  # This will fail

def downgrade(conn):
    conn.execute("DROP TABLE IF EXISTS valid_table")
"""
        )

    # Try to apply the failing migration
    with patch("duckup.migrate.logger") as mock_logger:
        with pytest.raises(duckdb.Error) as exc_info:
            upgrade(conn, empty_migrations_dir)

        # Verify the error message contains relevant information
        assert "nonexistent_table" in str(exc_info.value)

        # Verify the error was logged with the migration details
        mock_logger.error.assert_any_call(
            "Error executing migration %s (version %03d): %s",
            "failing_migration",  # Migration name
            1,  # Migration version
            str(exc_info.value),  # Error message
        )

    # Verify that due to transaction rollback, the valid_table wasn't created
    with pytest.raises(duckdb.Error):
        conn.execute("SELECT * FROM valid_table")

    # Verify the migrations version wasn't updated
    try:
        version = conn.execute("SELECT version FROM migrations").fetchone()[0]
        assert version == 0  # Should still be at version 0
    except duckdb.Error:
        # If migrations table doesn't exist, that's also fine
        pass


def test_no_migrations_to_downgrade(
    temp_db: Tuple[DuckDBPyConnection, str], temp_migrations_dir: str
) -> None:
    """Test downgrading when there are no migrations to downgrade."""
    conn, db_path = temp_db

    # Create a scenario where no migrations match
    # target_version < m.version <= db_version
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir)

        # Create migration with version 10
        migration10 = path / "010_first_migration.py"
        with open(migration10, "w") as f:
            f.write(
                """
def upgrade(conn):
    conn.execute("CREATE TABLE test_table (id INTEGER)")

def downgrade(conn):
    conn.execute("DROP TABLE test_table")
"""
            )

        # Create the migrations table manually
        conn.execute("DROP TABLE IF EXISTS migrations")
        conn.execute("CREATE TABLE migrations (version INTEGER)")

        # Set the version to 5
        conn.execute("INSERT INTO migrations VALUES (5)")

        # When we try to downgrade from version 5 to 3, with only migration
        # version 10 available, there are no migrations with 3 < version <= 5
        # This should trigger the "No migrations to downgrade" case
        with patch("duckup.migrate.logger") as mock_logger:
            downgrade(conn, tmp_dir, target_version=3)

            # Verify the "No migrations to downgrade" message is logged
            mock_logger.info.assert_any_call("No migrations to downgrade")

        # Verify we're still at version 5 (unchanged)
        assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 5


def test_downgrade_already_at_target_version(
    temp_db: Tuple[DuckDBPyConnection, str], temp_migrations_dir: str
) -> None:
    """Test downgrading when already at target version."""
    conn, db_path = temp_db

    # First upgrade to version 1
    upgrade(conn, temp_migrations_dir, target_version=1)

    # Verify current version
    assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 1

    # Try to downgrade to the same version
    with patch("duckup.migrate.logger") as mock_logger:
        downgrade(conn, temp_migrations_dir, target_version=1)
        # Verify that the appropriate log message was emitted
        msg = "Database already at target version %d, no downgrade needed"
        mock_logger.info.assert_any_call(msg, 1)

    # Verify version hasn't changed
    assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 1


def test_downgrade_execution_error(
    temp_db: Tuple[DuckDBPyConnection, str], empty_migrations_dir: str
) -> None:
    """Test error handling when a migration fails during downgrade."""
    conn, db_path = temp_db

    # Create a migration that will fail during downgrade
    path = Path(empty_migrations_dir)
    failing_migration = path / "001_failing_downgrade.py"
    with open(failing_migration, "w") as f:
        f.write(
            """
def upgrade(conn):
    conn.execute("CREATE TABLE downgrade_test (id INTEGER)")

def downgrade(conn):
    # This will raise a SQL error during downgrade
    conn.execute("SELECT * FROM nonexistent_table")  # This will fail
    conn.execute("DROP TABLE downgrade_test")
"""
        )

    # First upgrade to apply the migration
    upgrade(conn, empty_migrations_dir)

    # Check that we're at version 1
    assert conn.execute("SELECT version FROM migrations").fetchone()[0] == 1

    # Verify the table was created
    result = conn.execute("SELECT * FROM downgrade_test").fetchall()
    assert len(result) == 0  # Empty table, but it exists

    # Try to downgrade the failing migration
    with patch("duckup.migrate.logger") as mock_logger:
        with pytest.raises(duckdb.Error) as exc_info:
            downgrade(conn, empty_migrations_dir, target_version=0)

        # Verify the error message contains relevant information
        assert "nonexistent_table" in str(exc_info.value)

        # Verify the error was logged with the migration details
        mock_logger.error.assert_any_call(
            "Error reverting migration %s (version %03d): %s",
            "failing_downgrade",  # Migration name
            1,  # Migration version
            str(exc_info.value),  # Error message
        )

    # Verify that due to transaction rollback, the table wasn't dropped
    result = conn.execute("SELECT * FROM downgrade_test").fetchall()
    assert len(result) == 0  # Table should still exist

    # Verify the migrations version wasn't updated
    version = conn.execute("SELECT version FROM migrations").fetchone()[0]
    assert version == 1  # Should still be at version 1
