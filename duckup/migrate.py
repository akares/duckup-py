import importlib.util
import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType

from duckdb import DuckDBPyConnection as DBConn

from . import queries

logger = logging.getLogger("duckup")


class MigrationError(Exception):
    pass


class MigrationDirectoryError(MigrationError):
    pass


class MigrationFileError(MigrationError):
    pass


class MigrationVersionError(MigrationError):
    pass


@dataclass
class Migration:
    version: int
    module: ModuleType
    name: str


def upgrade(
    conn: DBConn,
    migrations_dir: str,
    migrations_table: str = "migrations",
    target_version: int = None,
) -> None:
    migrations = load_migrations(migrations_dir)
    if not migrations:
        logger.info("No migrations found in directory %s", migrations_dir)
        return

    db_version = queries.get_or_create_version(conn, migrations_table)
    logger.info("Current database version: %d", db_version)

    # Log detailed database state for debugging
    logger.debug(
        "Database connection details: %s",
        conn.execute("SELECT * FROM pragma_database_list").fetchall(),
    )

    # If target_version is None, we'll upgrade to the latest version
    if target_version is None:
        target_version = max(m.version for m in migrations)
        logger.info(
            "Target version not specified, using latest: %d", target_version
        )
    else:
        logger.info("Target upgrade version: %d", target_version)

    if target_version < db_version:
        logger.error(
            "Cannot upgrade to version %d because database is already at "
            "version %d",
            target_version,
            db_version,
        )
        raise MigrationVersionError(
            f"Cannot upgrade to version {target_version} because database is "
            f"already at version {db_version}"
        )

    if target_version == db_version:
        logger.info(
            "Database already at target version %d, no upgrade needed",
            target_version,
        )
        return

    # Count migrations to be applied
    to_apply = [
        m
        for m in sorted(migrations, key=lambda m: m.version)
        if db_version < m.version <= target_version
    ]
    if not to_apply:
        logger.info("No migrations to apply")
        return

    # Log the list of migrations to be applied for debug purposes
    if logger.isEnabledFor(logging.DEBUG):
        migration_list = ", ".join(f"{m.name} (v{m.version})" for m in to_apply)
        logger.debug("Migrations to apply: %s", migration_list)

    logger.info("Applying %d migration(s)", len(to_apply))

    for migration in sorted(migrations, key=lambda m: m.version):
        if migration.version <= db_version:
            logger.debug(
                "Skipping migration %s (version %03d): already applied",
                migration.name,
                migration.version,
            )
            continue
        if migration.version > target_version:
            logger.debug(
                "Skipping migration %s (version %03d): beyond target version "
                "%03d",
                migration.name,
                migration.version,
                target_version,
            )
            break

        logger.info(
            "Applying migration: %s (version %03d)",
            migration.name,
            migration.version,
        )

        # Debug log source code of the migration if available
        try:
            module_file = migration.module.__file__
            if module_file and logger.isEnabledFor(logging.DEBUG):
                with open(module_file, "r") as f:
                    logger.debug(
                        "Migration source for %s (version %03d):\n%s",
                        migration.name,
                        migration.version,
                        f.read(),
                    )
        except (AttributeError, FileNotFoundError, IOError):
            logger.debug(
                "Could not read source for migration %s (version %03d)",
                migration.name,
                migration.version,
            )

        with queries.transaction(conn):
            logger.debug(
                "Beginning transaction for migration %s", migration.name
            )

            # Start timing the execution
            start_time = time.time()

            # Execute the migration
            try:
                migration.module.upgrade(conn)
                execution_time = time.time() - start_time
                logger.debug(
                    "Migration %s (version %03d) executed in %.2f seconds",
                    migration.name,
                    migration.version,
                    execution_time,
                )
            except Exception as e:
                logger.error(
                    "Error executing migration %s (version %03d): %s",
                    migration.name,
                    migration.version,
                    str(e),
                )
                raise

            # Update the version in the database
            queries.update_version(conn, migrations_table, migration.version)
            logger.debug(
                "Updated database version to %d in %s table",
                migration.version,
                migrations_table,
            )

    logger.info("Database upgrade complete. Final version: %d", target_version)


def downgrade(
    conn: DBConn,
    migrations_dir: str,
    migrations_table: str = "migrations",
    target_version: int = None,
) -> None:
    migrations = load_migrations(migrations_dir)
    if not migrations:
        logger.info("No migrations found in directory %s", migrations_dir)
        return

    db_version = queries.get_or_create_version(conn, migrations_table)
    logger.info("Current database version: %d", db_version)

    # Log detailed database state for debugging
    logger.debug(
        "Database connection details: %s",
        conn.execute("SELECT * FROM pragma_database_list").fetchall(),
    )

    if target_version is None:
        target_version = db_version
        logger.info(
            "Target version not specified, staying at current version %d",
            db_version,
        )
        return
    else:
        logger.info("Target downgrade version: %d", target_version)

    if target_version > db_version:
        logger.error(
            "Cannot downgrade to version %d because database is already at "
            "version %d",
            target_version,
            db_version,
        )
        raise MigrationVersionError(
            f"Cannot downgrade to version {target_version} because database is "
            f"already at version {db_version}"
        )

    if target_version == db_version:
        logger.info(
            "Database already at target version %d, no downgrade needed",
            target_version,
        )
        return

    # Count migrations to be downgraded
    to_apply = [
        m
        for m in sorted(migrations, key=lambda m: m.version, reverse=True)
        if target_version < m.version <= db_version
    ]
    if not to_apply:
        logger.info("No migrations to downgrade")
        return

    # Log the list of migrations to be downgraded for debug purposes
    if logger.isEnabledFor(logging.DEBUG):
        migration_list = ", ".join(f"{m.name} (v{m.version})" for m in to_apply)
        logger.debug("Migrations to downgrade: %s", migration_list)

    logger.info("Downgrading %d migration(s)", len(to_apply))

    for migration in sorted(migrations, key=lambda m: m.version, reverse=True):
        if migration.version <= target_version:
            logger.debug(
                "Skipping downgrade of migration %s (version %03d): "
                "at or below target",
                migration.name,
                migration.version,
            )
            break
        if migration.version > db_version:
            logger.debug(
                "Skipping downgrade of migration %s (version %03d): "
                "beyond current version %d",
                migration.name,
                migration.version,
                db_version,
            )
            continue

        logger.info(
            "Reverting migration: %s (version %03d)",
            migration.name,
            migration.version,
        )

        # Debug log source code of the migration if available
        try:
            module_file = migration.module.__file__
            if module_file and logger.isEnabledFor(logging.DEBUG):
                with open(module_file, "r") as f:
                    logger.debug(
                        "Downgrade source for %s (version %03d):\n%s",
                        migration.name,
                        migration.version,
                        f.read(),
                    )
        except (AttributeError, FileNotFoundError, IOError):  # pragma: no cover
            logger.debug(
                "Could not read source for migration %s (version %03d)",
                migration.name,
                migration.version,
            )

        with queries.transaction(conn):
            logger.debug(
                "Beginning transaction for reverting migration %s",
                migration.name,
            )

            # Start timing the execution
            start_time = time.time()

            # Execute the downgrade
            try:
                migration.module.downgrade(conn)
                execution_time = time.time() - start_time
                logger.debug(
                    "Migration %s (version %03d) downgrade executed in %.2f "
                    "seconds",
                    migration.name,
                    migration.version,
                    execution_time,
                )
            except Exception as e:
                logger.error(
                    "Error reverting migration %s (version %03d): %s",
                    migration.name,
                    migration.version,
                    str(e),
                )
                raise

            # Find the previous version to downgrade to
            prev_version = 0  # Default to 0 if no earlier migration exists
            prev_name = None
            for m in sorted(migrations, key=lambda m: m.version):
                if m.version >= migration.version:
                    break
                prev_version = m.version
                prev_name = m.name

            logger.debug(
                "Found previous migration: %s (version %03d)",
                prev_name if prev_name else "None",
                prev_version,
            )

            # If we're at the target version or below, use target_version
            if prev_version <= target_version:
                prev_version = target_version
                logger.debug(
                    "Using target version %03d as previous version",
                    target_version,
                )

            queries.update_version(conn, migrations_table, prev_version)
            logger.debug(
                "Updated database version to %d in %s table",
                prev_version,
                migrations_table,
            )

    logger.info(
        "Database downgrade complete. Final version: %d", target_version
    )


def load_migrations(migrations_dir: str) -> list[Migration]:
    """Load migration modules from the given directory.

    Migrations files should be named like: 001_initial_schema.py,
    002_add_users.py, etc. The version is extracted from the filename.
    """
    logger.debug("Loading migrations from directory: %s", migrations_dir)
    migrations = []
    migration_pattern = re.compile(r"^(\d+)_(.*)\.py$")

    migrations_path = Path(migrations_dir)
    if not migrations_path.exists():
        logger.error("Migrations directory does not exist: %s", migrations_dir)
        raise MigrationDirectoryError(
            f"Migrations directory {migrations_dir} does not exist"
        )
    if not migrations_path.is_dir():
        logger.error("Path exists but is not a directory: %s", migrations_dir)
        raise MigrationDirectoryError(
            f"{migrations_dir} exists but is not a directory"
        )

    for filename in sorted(os.listdir(migrations_path)):
        match = migration_pattern.match(filename)
        if not match:
            logger.debug("Skipping non-migration file: %s", filename)
            continue

        version = int(match.group(1))
        migration_name = match.group(2)
        file_path = migrations_path / filename
        logger.debug(
            "Found migration file: %s (version %d, name: %s)",
            filename,
            version,
            migration_name,
        )

        # Load the module
        spec = importlib.util.spec_from_file_location(
            f"migration_{version}", file_path
        )
        if spec is None or spec.loader is None:
            logger.warning("Could not load migration file: %s", filename)
            continue

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Check that the module has the required functions
        if not hasattr(module, "upgrade") or not hasattr(module, "downgrade"):
            logger.error(
                "Migration %s is missing required functions "
                "(upgrade/downgrade)",
                filename,
            )
            raise MigrationFileError(
                f"Migration {filename} is missing required functions "
                "(upgrade/downgrade)"
            )

        migrations.append(
            Migration(version=version, module=module, name=migration_name)
        )

    logger.debug("Found %d migrations", len(migrations))
    return migrations
