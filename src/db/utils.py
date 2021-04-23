import logging
from typing import Any

from psycopg2 import sql

from src.db.connection import DBConnection


def is_table_exists(conn: DBConnection, table_name: str) -> bool:
    logging.info("checking if table {} exists".format(table_name))
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

        # table exists
        if res[0][0]:
            logging.info("table {} exists, skipping".format(table_name))
        else:
            logging.info("table {} does not exist, creating".format(table_name))

        return res[0][0]

    except Exception as e:
        logging.error("exception: {]".format(e))
        raise Exception("Exception: {}".format(e))


def is_value_exists(conn: DBConnection, table_name: str, column_name: str, value: Any):
    logging.info(
        "checking if value {} exists in column {} in table {}".format(
            value, column_name, table_name
        )
    )

    query_exists = """
    SELECT EXISTS(
    SELECT 1 FROM {} WHERE {}={}
    );
    """.format(
        table_name, column_name, value
    )
    cursor = conn.get_cursor()
    cursor.execute(sql.SQL(query_exists))
    res = cursor.fetchall()[0][0]

    if res:
        logging.info("value {} exists, skipping".format(value))
    else:
        logging.info("value {} does not exist, inserting".format(value))

    return res
