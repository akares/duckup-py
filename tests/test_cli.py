import logging
import shutil
import tempfile
from pathlib import Path
from typing import Generator
from unittest.mock import Mock, patch

import pytest
from duckup import MigrationError
from duckup.cli import (
    Command,
    CreateMigrationCommand,
    DowngradeCommand,
    ListCommand,
    UpgradeCommand,
    create_migration,
    main,
)


class TestCommand:
    def test_abstract_run(self) -> None:
        """Test that the base Command class raises NotImplementedError when
        run is called."""
        cmd = Command("test", "Test command")
        with pytest.raises(NotImplementedError):
            cmd.run(None)

    def test_add_arguments(self) -> None:
        """Test the base Command's add_arguments method."""
        cmd = Command("test", "Test command")
        parser_mock = Mock()
        cmd.parser = parser_mock
        cmd.add_arguments()
        # Verify no methods were called on the parser
        parser_mock.assert_not_called()


class TestCreateMigrationCommand:
    def test_create_migration(self) -> None:
        """Test creating a migration file."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a migration
            cmd = CreateMigrationCommand()
            with patch(
                "sys.argv",
                ["duckup", "create", "test_migration", "--dir", tmp_dir],
            ):
                # Mock the args
                args = type(
                    "Args", (), {"directory": tmp_dir, "name": "test_migration"}
                )()
                cmd.run(args)

            # Check that the migration file was created
            migration_files = list(Path(tmp_dir).glob("*.py"))
            assert len(migration_files) == 1
            assert migration_files[0].name == "001_test_migration.py"

            # Verify file contents
            with open(migration_files[0], "r") as f:
                content = f.read()
                assert "test_migration migration." in content
                assert "def upgrade(conn: DuckDBPyConnection)" in content
                assert "def downgrade(conn: DuckDBPyConnection)" in content

    def test_create_migration_nonexistent_dir(self) -> None:
        """Test creating a migration in a directory that doesn't exist."""
        with tempfile.TemporaryDirectory() as tmp_base:
            # Create a path to a non-existent directory
            nonexistent_dir = Path(tmp_base) / "nonexistent"

            try:
                # Ensure the directory doesn't exist
                if nonexistent_dir.exists():
                    shutil.rmtree(nonexistent_dir)

                # Create a migration
                create_migration(str(nonexistent_dir), "test_migration")

                # Check that the directory was created
                assert nonexistent_dir.exists()

                # Check that the migration file was created
                migration_files = list(nonexistent_dir.glob("*.py"))
                assert len(migration_files) == 1
                assert migration_files[0].name == "001_test_migration.py"
            finally:
                # Clean up
                if nonexistent_dir.exists():
                    shutil.rmtree(nonexistent_dir)


class TestUpgradeCommand:
    @patch("duckup.cli.upgrade")
    @patch("duckup.cli.duckdb.connect")
    def test_upgrade_command(
        self, mock_connect: Mock, mock_upgrade: Mock
    ) -> None:
        """Test the upgrade command."""
        cmd = UpgradeCommand()
        # Mock the args
        args = type(
            "Args",
            (),
            {
                "database": "test.db",
                "directory": "migrations",
                "table": "migrations",
                "version": None,
            },
        )()

        # Mock connection
        mock_conn = mock_connect.return_value

        cmd.run(args)

        # Assert connect was called with right db
        mock_connect.assert_called_once_with("test.db")
        # Assert upgrade was called with right params
        mock_upgrade.assert_called_once_with(
            mock_conn, "migrations", "migrations", None
        )
        # Assert connection was closed
        mock_conn.close.assert_called_once()

    @patch("duckup.cli.upgrade")
    @patch("duckup.cli.duckdb.connect")
    @patch("duckup.cli.sys.exit")
    def test_upgrade_command_error(
        self, mock_exit: Mock, mock_connect: Mock, mock_upgrade: Mock
    ) -> None:
        """Test the upgrade command when an error occurs."""
        cmd = UpgradeCommand()
        # Mock the args
        args = type(
            "Args",
            (),
            {
                "database": "test.db",
                "directory": "migrations",
                "table": "migrations",
                "version": None,
            },
        )()

        # Mock connection
        mock_conn = mock_connect.return_value

        # Simulate an error during upgrade
        mock_upgrade.side_effect = MigrationError("Test error")

        # Make sys.exit raise SystemExit to stop execution
        mock_exit.side_effect = SystemExit(1)

        # Call run and expect it to exit
        with pytest.raises(SystemExit):
            cmd.run(args)

        # Assert sys.exit was called with code 1
        mock_exit.assert_called_once_with(1)

        # Assert connection was closed even after error
        mock_conn.close.assert_called_once()

    @patch("duckup.cli.upgrade")
    @patch("duckup.cli.duckdb.connect")
    @patch("duckup.cli.sys.exit")
    def test_upgrade_command_unhandled_error(
        self, mock_exit: Mock, mock_connect: Mock, mock_upgrade: Mock
    ) -> None:
        """Test the upgrade command when an unhandled error occurs."""
        cmd = UpgradeCommand()
        # Mock the args
        args = type(
            "Args",
            (),
            {
                "database": "test.db",
                "directory": "migrations",
                "table": "migrations",
                "version": None,
            },
        )()

        # Mock connection
        mock_conn = mock_connect.return_value

        # Simulate an unhandled error during upgrade
        mock_upgrade.side_effect = ValueError("Unhandled error")

        # Make sys.exit raise SystemExit to stop execution
        mock_exit.side_effect = SystemExit(2)

        # Call run and expect it to exit
        with pytest.raises(SystemExit):
            cmd.run(args)

        # Assert sys.exit was called with code 2
        mock_exit.assert_called_once_with(2)

        # Assert connection was closed even after error
        mock_conn.close.assert_called_once()


class TestDowngradeCommand:
    @patch("duckup.cli.downgrade")
    @patch("duckup.cli.duckdb.connect")
    def test_downgrade_command(
        self, mock_connect: Mock, mock_downgrade: Mock
    ) -> None:
        """Test the downgrade command."""
        cmd = DowngradeCommand()
        # Mock the args
        args = type(
            "Args",
            (),
            {
                "database": "test.db",
                "directory": "migrations",
                "table": "migrations",
                "version": 1,
            },
        )()

        # Mock connection
        mock_conn = mock_connect.return_value

        cmd.run(args)

        # Assert connect was called with right db
        mock_connect.assert_called_once_with("test.db")
        # Assert downgrade was called with right params
        mock_downgrade.assert_called_once_with(
            mock_conn, "migrations", "migrations", 1
        )
        # Assert connection was closed
        mock_conn.close.assert_called_once()

    @patch("duckup.cli.downgrade")
    @patch("duckup.cli.duckdb.connect")
    @patch("duckup.cli.sys.exit")
    def test_downgrade_command_error(
        self, mock_exit: Mock, mock_connect: Mock, mock_downgrade: Mock
    ) -> None:
        """Test the downgrade command when an error occurs."""
        cmd = DowngradeCommand()
        # Mock the args
        args = type(
            "Args",
            (),
            {
                "database": "test.db",
                "directory": "migrations",
                "table": "migrations",
                "version": 1,
            },
        )()

        # Mock connection
        mock_conn = mock_connect.return_value

        # Simulate an error during downgrade
        mock_downgrade.side_effect = MigrationError("Test error")

        # Make sys.exit raise SystemExit to stop execution
        mock_exit.side_effect = SystemExit(1)

        # Call run and expect it to exit
        with pytest.raises(SystemExit):
            cmd.run(args)

        # Assert sys.exit was called with code 1
        mock_exit.assert_called_once_with(1)

        # Assert connection was closed even after error
        mock_conn.close.assert_called_once()

    @patch("duckup.cli.downgrade")
    @patch("duckup.cli.duckdb.connect")
    @patch("duckup.cli.sys.exit")
    def test_downgrade_command_unhandled_error(
        self, mock_exit: Mock, mock_connect: Mock, mock_downgrade: Mock
    ) -> None:
        """Test the downgrade command when an unhandled error occurs."""
        cmd = DowngradeCommand()
        # Mock the args
        args = type(
            "Args",
            (),
            {
                "database": "test.db",
                "directory": "migrations",
                "table": "migrations",
                "version": 1,
            },
        )()

        # Mock connection
        mock_conn = mock_connect.return_value

        # Simulate an unhandled error during downgrade
        mock_downgrade.side_effect = ValueError("Unhandled error")

        # Make sys.exit raise SystemExit to stop execution
        mock_exit.side_effect = SystemExit(2)

        # Call run and expect it to exit
        with pytest.raises(SystemExit):
            cmd.run(args)

        # Assert sys.exit was called with code 2
        mock_exit.assert_called_once_with(2)

        # Assert connection was closed even after error
        mock_conn.close.assert_called_once()


class TestListCommand:
    @patch("duckup.cli.load_migrations")
    def test_list_command(self, mock_load_migrations: Mock) -> None:
        """Test the list command."""
        # Create test migrations with proper attributes
        mock1 = Mock()
        mock1.version = 1
        mock1.name = "initial_schema"
        mock1.module = Mock()
        mock1.module.__file__ = "/path/to/001_initial_schema.py"

        mock2 = Mock()
        mock2.version = 2
        mock2.name = "add_users"
        mock2.module = Mock()
        mock2.module.__file__ = "/path/to/002_add_users.py"

        mock_migrations = [mock1, mock2]
        mock_load_migrations.return_value = mock_migrations

        cmd = ListCommand()
        # Mock the args
        args = type(
            "Args",
            (),
            {
                "directory": "migrations",
            },
        )()

        with patch("duckup.cli.logger") as mock_logger:
            cmd.run(args)

            # Verify load_migrations was called with the right directory
            mock_load_migrations.assert_called_once_with("migrations")

            # Verify logger.info was called for each migration
            assert mock_logger.info.call_count >= 3  # Header + 2 migrations
            mock_logger.info.assert_any_call("Available migrations:")
            mock_logger.info.assert_any_call(
                "Version %03d: %s (%s)",
                1,
                "initial_schema",
                "001_initial_schema.py",
            )
            mock_logger.info.assert_any_call(
                "Version %03d: %s (%s)",
                2,
                "add_users",
                "002_add_users.py",
            )

    @patch("duckup.cli.load_migrations")
    def test_list_command_no_migrations(
        self, mock_load_migrations: Mock
    ) -> None:
        """Test the list command when no migrations are found."""
        mock_load_migrations.return_value = []

        cmd = ListCommand()
        args = type(
            "Args",
            (),
            {
                "directory": "empty_dir",
            },
        )()

        with patch("duckup.cli.logger") as mock_logger:
            cmd.run(args)

            # Verify load_migrations was called
            mock_load_migrations.assert_called_once_with("empty_dir")

            # Verify appropriate message was logged
            mock_logger.info.assert_called_once_with(
                "No migrations found in directory %s", "empty_dir"
            )

    @patch("duckup.cli.load_migrations")
    @patch("duckup.cli.sys.exit")
    def test_list_command_error(
        self, mock_exit: Mock, mock_load_migrations: Mock
    ) -> None:
        """Test the list command when an error occurs."""
        # Simulate an error during loading migrations
        error = MigrationError("Test error")
        mock_load_migrations.side_effect = error

        cmd = ListCommand()
        args = type(
            "Args",
            (),
            {
                "directory": "migrations",
            },
        )()

        # Make sys.exit raise SystemExit to stop execution
        mock_exit.side_effect = SystemExit(1)

        with patch("duckup.cli.logger") as mock_logger:
            # Call run and expect it to exit
            with pytest.raises(SystemExit):
                cmd.run(args)

            # Assert error was logged with the actual exception object
            mock_logger.error.assert_called_once_with(
                "Error listing migrations: %s", error
            )

            # Assert sys.exit was called with code 1
            mock_exit.assert_called_once_with(1)


class TestMainFunction:
    @patch("duckup.cli.sys.exit")
    @patch("duckup.cli.argparse.ArgumentParser.parse_args")
    def test_main_no_command(
        self, mock_parse_args: Mock, mock_exit: Mock
    ) -> None:
        """Test main function with no command."""
        # Mock args with no command
        mock_parse_args.return_value.command = None

        # Make sys.exit raise SystemExit to stop execution
        mock_exit.side_effect = SystemExit(1)

        # Call main and expect it to exit
        with pytest.raises(SystemExit):
            main()

        # Assert sys.exit was called with code 1
        mock_exit.assert_called_once_with(1)

    @patch("duckup.cli.CreateMigrationCommand.run")
    @patch("duckup.cli.argparse.ArgumentParser.parse_args")
    def test_main_with_create_command(
        self, mock_parse_args: Mock, mock_run: Mock, temp_migrations_dir: str
    ) -> None:
        """Test main function with create command."""
        # Mock args with create command
        mock_args = mock_parse_args.return_value
        mock_args.command = "create"
        mock_args.directory = temp_migrations_dir
        mock_args.name = "test_migration"

        main()

        # Assert run was called once with args
        mock_run.assert_called_once_with(mock_args)

    @patch("duckup.cli.UpgradeCommand.run")
    @patch("duckup.cli.argparse.ArgumentParser.parse_args")
    def test_main_with_upgrade_command(
        self, mock_parse_args: Mock, mock_run: Mock
    ) -> None:
        """Test main function with upgrade command."""
        # Mock args with upgrade command
        mock_args = mock_parse_args.return_value
        mock_args.command = "upgrade"

        main()

        # Assert run was called once with args
        mock_run.assert_called_once_with(mock_args)

    @patch("duckup.cli.DowngradeCommand.run")
    @patch("duckup.cli.argparse.ArgumentParser.parse_args")
    def test_main_with_downgrade_command(
        self, mock_parse_args: Mock, mock_run: Mock
    ) -> None:
        """Test main function with downgrade command."""
        # Mock args with downgrade command
        mock_args = mock_parse_args.return_value
        mock_args.command = "downgrade"

        main()

        # Assert run was called once with args
        mock_run.assert_called_once_with(mock_args)

    @patch("duckup.cli.ListCommand.run")
    @patch("duckup.cli.argparse.ArgumentParser.parse_args")
    def test_main_with_list_command(
        self, mock_parse_args: Mock, mock_run: Mock
    ) -> None:
        """Test that main calls ListCommand.run when list command is used."""
        args = Mock()
        args.command = "list"
        args.verbose = False
        args.quiet = False
        mock_parse_args.return_value = args

        # Run main
        main()

        # Check that ListCommand.run was called with the arguments
        mock_run.assert_called_once_with(args)

    @patch("duckup.cli.logging.basicConfig")
    @patch("duckup.cli.argparse.ArgumentParser.parse_args")
    def test_main_with_verbose_flag(
        self,
        mock_parse_args: Mock,
        mock_basic_config: Mock,
        temp_migrations_dir: str,
    ) -> None:
        """Test main function with verbose flag."""
        # Mock args with verbose=True
        mock_args = mock_parse_args.return_value
        mock_args.command = "create"
        mock_args.directory = temp_migrations_dir
        mock_args.name = "test_migration"
        mock_args.verbose = True
        mock_args.quiet = False

        main()

        # Assert basicConfig was called with DEBUG level
        mock_basic_config.assert_called_once()
        call_args = mock_basic_config.call_args[1]
        assert call_args["level"] == logging.DEBUG
        assert call_args["format"] == "[%(levelname)s] %(message)s"
        assert len(call_args["handlers"]) == 1
        assert isinstance(call_args["handlers"][0], logging.StreamHandler)

    @patch("duckup.cli.logging.basicConfig")
    @patch("duckup.cli.argparse.ArgumentParser.parse_args")
    def test_main_with_quiet_flag(
        self,
        mock_parse_args: Mock,
        mock_basic_config: Mock,
        temp_migrations_dir: str,
    ) -> None:
        """Test main function with quiet flag."""
        # Mock args with quiet=True
        mock_args = mock_parse_args.return_value
        mock_args.command = "create"
        mock_args.directory = temp_migrations_dir
        mock_args.verbose = False
        mock_args.quiet = True

        main()

        # Assert basicConfig was called with ERROR level
        mock_basic_config.assert_called_once()
        call_args = mock_basic_config.call_args[1]
        assert call_args["level"] == logging.ERROR
        assert call_args["format"] == "[%(levelname)s] %(message)s"
        assert len(call_args["handlers"]) == 1
        assert isinstance(call_args["handlers"][0], logging.StreamHandler)

    @patch("duckup.cli.logging.basicConfig")
    @patch("duckup.cli.argparse.ArgumentParser.parse_args")
    def test_default_log_level(
        self,
        mock_parse_args: Mock,
        mock_basic_config: Mock,
        temp_migrations_dir: str,
    ) -> None:
        """Test that default log level is INFO."""
        # Mock args with default values (no verbosity flags)
        mock_args = mock_parse_args.return_value
        mock_args.command = "create"
        mock_args.directory = temp_migrations_dir
        mock_args.verbose = False
        mock_args.quiet = False

        main()

        # Assert basicConfig was called with INFO level (new default)
        mock_basic_config.assert_called_once()
        call_args = mock_basic_config.call_args[1]
        assert call_args["level"] == logging.INFO
        assert call_args["format"] == "[%(levelname)s] %(message)s"
        assert len(call_args["handlers"]) == 1
        assert isinstance(call_args["handlers"][0], logging.StreamHandler)


@pytest.fixture
def temp_migrations_dir() -> Generator[str, None, None]:
    """Create a temporary migrations directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


def test_create_migration_highest_version(temp_migrations_dir: str) -> None:
    """Test create_migration with multiple files with different versions."""
    dir_path = Path(temp_migrations_dir)

    # Create some existing migration files
    migrations = [
        "001_first.py",
        "002_second.py",
        "005_fifth.py",  # Not sequential to test max finding
        "invalid_file.py",  # Should be ignored
        "README.md",  # Should be ignored
    ]

    for filename in migrations:
        with open(dir_path / filename, "w") as f:
            f.write("# Test file")

    # Run create_migration function
    create_migration(temp_migrations_dir, "new_migration")

    # Check that a new file with version 6 was created
    new_file = dir_path / "006_new_migration.py"
    assert new_file.exists()

    # Verify file content
    content = new_file.read_text()
    assert "new_migration migration." in content
    assert "def upgrade(conn: DuckDBPyConnection)" in content
    assert "def downgrade(conn: DuckDBPyConnection)" in content


def test_create_migration_with_negative_version(
    temp_migrations_dir: str,
) -> None:
    """Test create_migration with a negative migration version."""
    dir_path = Path(temp_migrations_dir)

    # Create a migration file with negative version
    with open(dir_path / "-01_negative.py", "w") as f:
        f.write("# Negative version file")

    # Run create_migration function
    create_migration(temp_migrations_dir, "after_negative")

    # The function should ignore negative versions and start from 1
    new_file = dir_path / "001_after_negative.py"
    assert new_file.exists()

    # Verify file content
    content = new_file.read_text()
    assert "after_negative migration." in content


def test_create_migration_with_large_version(temp_migrations_dir: str) -> None:
    """Test create_migration with a version number larger than int32."""
    dir_path = Path(temp_migrations_dir)

    # Create a migration file with a very large version number (beyond int32)
    # Just beyond int32 max (2,147,483,647)
    large_version = 2_147_483_648
    with open(dir_path / f"{large_version}_large_int.py", "w") as f:
        f.write("# Large integer version file")

    # Run create_migration function
    create_migration(temp_migrations_dir, "after_large")

    # The function should handle large ints correctly and increment by 1
    new_file = dir_path / f"{large_version + 1:d}_after_large.py"
    assert new_file.exists()

    # Verify file content
    content = new_file.read_text()
    assert "after_large migration." in content


def test_create_migration_with_float_version(temp_migrations_dir: str) -> None:
    """Test create_migration with a float version number."""
    dir_path = Path(temp_migrations_dir)

    # Create a migration file with a float version
    # (this will be ignored as it won't parse as int)
    with open(dir_path / "1.5_float_version.py", "w") as f:
        f.write("# Float version file")

    # Create a valid migration file for comparison
    with open(dir_path / "002_valid.py", "w") as f:
        f.write("# Valid file")

    # Run create_migration function
    create_migration(temp_migrations_dir, "after_float")

    # The function should ignore the float version
    # and use the highest valid integer version
    new_file = dir_path / "003_after_float.py"
    assert new_file.exists()

    # Verify file content
    content = new_file.read_text()
    assert "after_float migration." in content
