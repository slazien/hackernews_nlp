from typing import Any
from src.db.connection import DBConnection

from psycopg2 import sql


def is_table_exists(conn: DBConnection, table_name: str) -> bool:
    try:
        cursor = conn.get().cursor()
        query = sql.SQL(
            """
            SELECT EXISTS (
            SELECT FROM pg_tables
            WHERE  schemaname = 'public'
            AND    tablename  = '{}'
           );
        """.format(table_name)
        )

        cursor.execute(query)
        res = cursor.fetchall()

        return res[0][0]

    except Exception as e:
        raise Exception("Exception: {}".format(e))


def is_value_exists(conn: DBConnection, table_name: str, column_name: str, value: Any):
    query_all = """
    SELECT {} FROM {}
    """.format(column_name, table_name)
