#!/usr/bin/env python

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import duckdb
from duckup import MigrationError, downgrade, upgrade
from duckup.migrate import load_migrations

logger = logging.getLogger()


class Command:
    def __init__(self, name: str, help: str) -> None:
        self.name = name
        self.help = help
        self.parser: Optional[argparse.ArgumentParser] = None

    def setup_parser(self, subparsers: Any) -> None:
        self.parser = subparsers.add_parser(
            self.name,
            help=self.help,
        )
        self.add_arguments()

    def add_arguments(self) -> None:
        pass

    def run(self, args: argparse.Namespace) -> None:
        raise NotImplementedError


class CreateMigrationCommand(Command):
    def __init__(self) -> None:
        super().__init__("create", "Create a new migration")

    def add_arguments(self) -> None:
        self.parser.add_argument(
            "name",
            metavar="MIGRATION_NAME",
            type=str,
            help="Name of the migration (e.g., 'add_users_table')",
        )
        self.parser.add_argument(
            "--dir",
            "-d",
            dest="directory",
            metavar="MIGRATIONS_DIR",
            type=str,
            default="migrations",
            help="Directory where migration files are stored",
        )

    def run(self, args: argparse.Namespace) -> None:
        create_migration(args.directory, args.name)


class UpgradeCommand(Command):
    def __init__(self) -> None:
        super().__init__("upgrade", "Upgrade database to a specific version")

    def add_arguments(self) -> None:
        self.parser.add_argument(
            "database",
            metavar="DATABASE_FILE",
            type=str,
            help="Path to the DuckDB database file",
        )
        self.parser.add_argument(
            "--dir",
            "-d",
            dest="directory",
            metavar="MIGRATIONS_DIR",
            type=str,
            default="migrations",
            help=(
                "Directory containing migration files " "(default: %(default)s)"
            ),
        )
        self.parser.add_argument(
            "--table",
            "-t",
            dest="table",
            metavar="VERSION_TABLE",
            type=str,
            default="migrations",
            help=(
                "Table name for tracking migration versions "
                "(default: %(default)s)"
            ),
        )
        self.parser.add_argument(
            "--version",
            "-v",
            dest="version",
            metavar="TARGET_VERSION",
            type=int,
            help="Version number to upgrade to (default: latest)",
        )

    def run(self, args: argparse.Namespace) -> None:
        logger.info("Upgrading database %s", args.database)
        conn = duckdb.connect(args.database)
        try:
            upgrade(conn, args.directory, args.table, args.version)
            logger.info("Database upgraded successfully")
        except MigrationError as e:
            logger.error("Error upgrading database: %s", e)
            sys.exit(1)
        except Exception as e:
            logger.error("Unhandled error during database upgrade: %s", e)
            sys.exit(2)
        finally:
            conn.close()


class DowngradeCommand(Command):
    def __init__(self) -> None:
        super().__init__(
            "downgrade", "Downgrade database to a specific version"
        )

    def add_arguments(self) -> None:
        self.parser.add_argument(
            "database",
            metavar="DATABASE_FILE",
            type=str,
            help="Path to the DuckDB database file",
        )
        self.parser.add_argument(
            "version",
            metavar="TARGET_VERSION",
            type=int,
            help="Version number to downgrade to",
        )
        self.parser.add_argument(
            "--dir",
            "-d",
            dest="directory",
            metavar="MIGRATIONS_DIR",
            type=str,
            default="migrations",
            help="Directory containing migration files",
        )
        self.parser.add_argument(
            "--table",
            "-t",
            dest="table",
            metavar="VERSION_TABLE",
            type=str,
            default="migrations",
            help="Table name for tracking migration versions",
        )

    def run(self, args: argparse.Namespace) -> None:
        logger.info(
            "Downgrading database %s to version %d", args.database, args.version
        )
        conn = duckdb.connect(args.database)
        try:
            downgrade(conn, args.directory, args.table, args.version)
            logger.info("Database downgraded successfully")
        except MigrationError as e:
            logger.error("Error downgrading database: %s", e)
            sys.exit(1)
        except Exception as e:
            logger.error("Unhandled error during database downgrade: %s", e)
            sys.exit(2)
        finally:
            conn.close()


class ListCommand(Command):
    def __init__(self) -> None:
        super().__init__("list", "List all migrations")

    def add_arguments(self) -> None:
        self.parser.add_argument(
            "--dir",
            "-d",
            dest="directory",
            metavar="MIGRATIONS_DIR",
            type=str,
            default="migrations",
            help="Directory containing migration files",
        )

    def run(self, args: argparse.Namespace) -> None:
        try:
            migrations = load_migrations(args.directory)
            if not migrations:
                logger.info(
                    "No migrations found in directory %s", args.directory
                )
                return

            logger.info("Available migrations:")
            for migration in sorted(migrations, key=lambda m: m.version):
                module_file = getattr(migration.module, "__file__", "Unknown")
                file_name = Path(module_file).name if module_file else "Unknown"
                logger.info(
                    "Version %03d: %s (%s)",
                    migration.version,
                    migration.name,
                    file_name,
                )
        except MigrationError as e:
            logger.error("Error listing migrations: %s", e)
            sys.exit(1)
        except Exception as e:  # pragma: no cover
            logger.error("Unhandled error while listing migrations: %s", e)
            sys.exit(2)


def create_migration(directory: str, name: str) -> None:
    directory_path = Path(directory)

    if not directory_path.exists():
        logger.info("Creating migrations directory: %s", directory)
        os.makedirs(directory_path)

    highest_version = 0
    for item in directory_path.glob("*.py"):
        try:
            version = int(item.name.split("_")[0])
            highest_version = max(highest_version, version)
        except (ValueError, IndexError):
            pass

    new_version = highest_version + 1
    file_name = f"{new_version:03d}_{name}.py"
    file_path = directory_path / file_name

    with open(file_path, "w") as f:
        f.write(
            f'''"""
{name} migration.
"""

from duckdb import DuckDBPyConnection


def upgrade(conn: DuckDBPyConnection) -> None:
    pass


def downgrade(conn: DuckDBPyConnection) -> None:
    pass
'''
        )

    logger.info("Created migration file: %s", file_path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="DuckDB migration tool.",
    )

    # Add global arguments
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Suppress all output except errors",
    )

    subparsers = parser.add_subparsers(
        dest="command",
        help="Available commands:",
    )

    commands: Dict[str, Command] = {
        cmd.name: cmd
        for cmd in [
            CreateMigrationCommand(),
            ListCommand(),
            UpgradeCommand(),
            DowngradeCommand(),
        ]
    }

    for cmd in commands.values():
        cmd.setup_parser(subparsers)

    args = parser.parse_args()

    log_level = logging.INFO
    if args.quiet:
        log_level = logging.ERROR
    elif args.verbose:
        log_level = logging.DEBUG

    logging.basicConfig(
        level=log_level,
        format="[%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()],
    )

    if log_level == logging.DEBUG:
        logger = logging.getLogger()
        logger.debug(
            "Debug logging enabled - showing detailed migration information"
        )

    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands[args.command].run(args)


if __name__ == "__main__":  # pragma: no cover
    main()
