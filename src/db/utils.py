from typing import Any

from psycopg2 import sql


def is_table_exists(conn, table_name: str) -> bool:
    try:
        cursor = conn.cursor()
        query = sql.SQL(
            """
            SELECT EXISTS (
            SELECT FROM pg_tables
            WHERE  schemaname = 'public'
            AND    tablename  = '{}'
           );
        """
        ).format(sql.Identifier(table_name))

        cursor.execute(query)
        res = cursor.fetchall()

        return res[0][0]

    except Exception as e:
        raise Exception("Exception: {}".format(e))


def is_value_exists(value: Any, column_name: str):
    pass
