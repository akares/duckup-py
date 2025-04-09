<!-- [![PyPI](https://img.shields.io/pypi/v/skreader)](https://pypi.org/project/duckup/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/duckup)](https://pypi.org/project/duckup/) -->
[![CI](https://github.com/akares/duckup-py/actions/workflows/ci.yml/badge.svg)](https://github.com/akares/duckup-py/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/akares/duckup-py/branch/main/graph/badge.svg)](https://codecov.io/gh/akares/duckup-py)

# duckup

<img src="https://raw.githubusercontent.com/akares/duckup-py/main/doc/logo.svg" width="75" height="75">

**Database migrations for [DuckDB](https://duckdb.org/) written in Python. Use as [CLI](#cli-usage) tool or import as [library](#use-in-your-python-project).**

- No dependencies other than DuckDB.
- Simple to use.

## Installation

```sh
pip install duckup
```

## CLI usage

Duckup provides a command line tool (named... `duckup`) that allows you to `create`, `list`, `upgrade` and `downgrade` migrations.

If you worked with Django you can think of duckup CLI as an analogue of `makemigrations` and `migrate` commands.

Using CLI is not mandatory, you can use duckup as a library in your Python project, create migration files manually and use the duckup library to programmatically apply your migrations.

**Directory**: default migrations directory name is `migrations`, relative to the current working directory. Custom migrations directory can be specified using `--dir` flag. If directory does not exist, it will be created.

**Database**: database file must be specified as a positional argument. It can be a relative or absolute path to the DuckDB database file you want to migrate.

**Versioning**: migrations are versioned using a number. The first migration is version 1, the second is version 2, etc. Actual version is stored in the `migrations` table in the database. You can change the name of the table using `--table` flag.

### Some examples:

**Create a new migration in default or custom migrations directory**

```sh
duckup create my_migration
duckup create my_migration --dir db_migrations
```

**Upgrade database to latest or to specific version with default or custom version table name**

```sh
duckup upgrade mydatabase.duckdb
duckup upgrade mydatabase.duckdb --table migrations_table
duckup upgrade mydatabase.duckdb 42
```

**Downgrade database to initial state or to specific version with default or custom version table name**

```sh
duckup downgrade mydatabase.duckdb 0
duckup downgrade mydatabase.duckdb 0 --table migrations_table
duckup downgrade mydatabase.duckdb 42
```

**List all migrations in default or custom migrations directory**

```sh
duckup list
duckup list --dir db_migrations
```

Refer to `duckup --help` for full usage information.

## Use in your Python project

Very basic example:

```python
import duckdb
import duckup

conn = duckdb.connect("my_database.duckdb")

try:
    duckup.upgrade(conn, "migrations_dir")

except duckup.MigrationError as e:
    print(f"Migration error: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
else:
    print("Migration successful")

```

For more detailed example see [examples/example_migration.py](examples/example_migration.py).

## Migration files

Naming convention:

```
<version>_<name>.py
```

Example:

```
001_create_users.py
```

`Version` is a number that is incremented for each migration. `Name` is a human readable name for the migration (use underscores to separate words, CLI does not take care of that automatically). When creating a migration using CLI, version is automatically generated. If you are creating a migration manually, you need to take care of naming it properly.

Each migration file contains `upgrade` and `downgrade` handlers which are a functions that take a DB connection instance as an argument and perform the necessary operations to upgrade or downgrade the database. It's up to you to decide what to put in there.

```python
"""
create_users migration.
"""

from duckdb import DuckDBPyConnection


def upgrade(conn: DuckDBPyConnection) -> None:
    conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR)")


def downgrade(conn: DuckDBPyConnection) -> None:
    conn.execute("DROP TABLE users")
```

Migrations are executed in the order of the version number.

Migrations are applied inside a transaction. If any instruction fails, the transaction is rolled back and the migration is considered as not applied.

## Logging

Duckup library utilises the standard Python logging interface. By default, log messages are at the INFO level and include:

- Migration files found
- Current database version
- Target version for upgrade/downgrade
- Each migration being applied (upgrade or downgrade)
- Successful completion of migrations

### Debug logging

When debug logging is enabled, you'll get much more detailed information:

- Full details of each migration including names and versions
- Source code for each migration being executed
- Execution time measurements for each migration
- Transaction details
- Explanations when migrations are skipped
- Detailed error information when migrations fail

### CLI verbosity options

When using the CLI, you can control the verbosity level:

```
duckup --verbose upgrade mydatabase.db     # More detailed logs (DEBUG level)
duckup --quiet upgrade mydatabase.db       # Only show errors (ERROR level)
```

Default verbosity level for CLI is INFO.

### Customizing logging in your code

When using Duckup as a library, you can customize logging behavior as usual with Python's logging module.

```python
logging.getLogger("duckup").setLevel(logging.DEBUG)

```

## Contribution

### Development setup

As 3.9 is the minimum supported by the library version of Python, it is recommended to use it during development to avoid backward compatibility issues introduced by newer versions.

`Poetry` is used for dependency management and virtual environment creation. It is recommended to use it for library development.

```sh
poetry env use 3.9
poetry install
poetry run ./duckup/cli.py
```

`Pytest` is used to run unittests.

```sh
poetry run pytest
```

`MyPy` and `Flake8` are used for linting.

```sh
poetry run mypy duckup tests
poetry run flake8 duckup tests
```

`Black` is used for code formatting.\
`isort` is used for sorting imports.

## License

This project is licensed under the terms of the MIT license.
