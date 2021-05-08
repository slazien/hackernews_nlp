import logging
from typing import Any, List, Set, Tuple, Union

from psycopg2 import sql

from src.db.connection import DBConnection


def is_table_exists(conn: DBConnection, table_name: str) -> bool:
    """
    Check if a given table exists in the DB
    :param conn: DBConnection object
    :param table_name: name of the table to check
    :return: True if table exists, False otherwise
    """
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


def is_value_exists(
    conn: DBConnection, table_name: str, column_name: str, value: Any
) -> bool:
    """
    Check if a given value exists in a given column in a given table
    :param conn: DBConnection object
    :param table_name: name of the table
    :param column_name: name of the column
    :param value: value to check existence of
    :return: True if the value exists, False otherwise
    """
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


def all_values_exist(
    conn: DBConnection, table_name: str, column_name: str, values: tuple
) -> bool:
    """
    Check if all provided values exist in a given table in a given column
    :param conn: DBConnection object
    :param table_name: name of the table
    :param column_name: name of the column
    :param values: tuple of all values to check existence of
    :return: True if all values exist, False otherwise
    """
    logging.info(
        "checking if range of values [{}, {}] exists in column {} in table {}".format(
            min(values), max(values), column_name, table_name
        )
    )

    query_exist = """
    SELECT COUNT({}) FROM {} WHERE {} IN {}
    """.format(
        column_name, table_name, column_name, values
    )

    cursor = conn.get_cursor()
    cursor.execute(sql.SQL(query_exist))
    res = cursor.fetchall()[0][0]

    # Check if the count of returned values is the same as the count of the input values
    return res == len(values)
