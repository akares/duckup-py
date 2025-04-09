from contextlib import contextmanager
from typing import Generator

from duckdb import DuckDBPyConnection as DBConn


def initialise_migrations_table(conn: DBConn, migrations_table: str) -> None:
    q1 = f"create table if not exists {migrations_table} (version UBIGINT)"
    q2 = f"insert into {migrations_table} values (0)"
    with transaction(conn):
        conn.execute(q1)
        conn.execute(q2)


def migrations_table_initialised(conn: DBConn, migrations_table: str) -> bool:
    q = "select * from duckdb_tables where table_name = ?"
    return bool(conn.execute(q, [migrations_table]).fetchall())


def get_version(conn: DBConn, migrations_table: str) -> int:
    q = f"select version from {migrations_table}"
    row = conn.execute(q).fetchone()
    return int(row[0]) if row else 0


def get_or_create_version(conn: DBConn, migrations_table: str) -> int:
    if not migrations_table_initialised(conn, migrations_table):
        initialise_migrations_table(conn, migrations_table)
    return get_version(conn, migrations_table)


def update_version(conn: DBConn, migrations_table: str, version: int) -> None:
    q = f"update {migrations_table} set version = ?"
    conn.execute(q, [version])


@contextmanager
def transaction(conn: DBConn) -> Generator[None, None, None]:
    try:
        conn.begin()
        yield
        conn.commit()
    except Exception:
        conn.rollback()
        raise
