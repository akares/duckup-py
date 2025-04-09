"""
create_users migration.
"""

from duckdb import DuckDBPyConnection


def upgrade(conn: DuckDBPyConnection) -> None:
    conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR)")
    conn.execute("INSERT INTO users (id, name) VALUES (1, 'John Doe')")


def downgrade(conn: DuckDBPyConnection) -> None:
    conn.execute("DROP TABLE users")
