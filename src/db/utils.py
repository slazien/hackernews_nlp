from typing import Any

from psycopg2 import sql

from src.db.connection import DBConnection


def is_table_exists(conn: DBConnection, table_name: str) -> bool:
    try:
        cursor = conn.get_cursor()
        query = sql.SQL(
            """
            SELECT EXISTS (
            SELECT FROM pg_tables
            WHERE  schemaname = 'public'
            AND    tablename  = '{}'
           );
        """.format(
                table_name
            )
        )

        cursor.execute(query)
        res = cursor.fetchall()

        return res[0][0]

    except Exception as e:
        raise Exception("Exception: {}".format(e))


def is_value_exists(conn: DBConnection, table_name: str, column_name: str, value: Any):
    query_all = """
    SELECT {} FROM {}
    """.format(
        column_name, table_name
    )

    cursor = conn.get_cursor()

    cursor.execute(sql.SQL(query_all))

    query_all_res = cursor.fetchall()

    ids = [item[0] for item in query_all_res]

    return value in ids
