import logging
from typing import Any, Iterable, Optional

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

    query_exists = sql.SQL(
        "SELECT EXISTS(SELECT 1 FROM {table} WHERE {column} = %s);"
    ).format(
        table=sql.Identifier(table_name),
        column=sql.Identifier(column_name),
    )

    cursor = conn.get_cursor()
    cursor.execute(query_exists, (value,))
    res = cursor.fetchall()[0][0]

    if res:
        logging.info("check: value {} exists".format(value))
    else:
        logging.info("check: value {} does not exist".format(value))

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

    query = "SELECT COUNT({column}) FROM {table} WHERE {column} IN %s;"

    query_sql = sql.SQL(query).format(
        column=sql.Identifier(column_name), table=sql.Identifier(table_name)
    )

    cursor = conn.get_cursor()
    cursor.execute(query_sql, values)
    res = cursor.fetchall()[0][0]

    # Check if the count of returned values is the same as the count of the input values
    return res == len(values)


def get_column_values(
    conn: DBConnection,
    table_name: str,
    column_name: str,
    limit: Optional[int] = None,
    fetch_size: int = 10000,
) -> Optional[Iterable[Any]]:
    """
    Get all values in a given column in a given table
    :param conn: DBConnection object
    :param table_name: name of the table
    :param column_name: name of the column
    :param limit: maximum number of values to return
    :param fetch_size: number of rows to fetch in one batch
    :return: generator of values (if any) in the column
    """
    logging.info("getting all values for column: {}".format(column_name))

    cursor = conn.get_named_cursor("cursor_get_column_values")
    cursor.itersize = 10000

    if limit:
        query = "SELECT {column} FROM {table} LIMIT %s;"
    else:
        query = "SELECT {column} FROM {table};"

    query_sql = sql.SQL(query).format(
        column=sql.Identifier(column_name), table=sql.Identifier(table_name)
    )
    cursor.execute(query_sql, [limit] if limit else None)

    while True:
        rows = cursor.fetchmany(fetch_size)
        if not rows:
            break
        for row in rows:
            yield row[0]


def get_value_count_in_column(
    conn: DBConnection, table_name: str, column_name: str
) -> int:
    """
    Count the number of values in a column
    :param conn: DBConnection object
    :param table_name: name of the table
    :param column_name: name of the column
    :return: count of values
    """
    logging.info(
        "getting value count in column: {}, table: {}".format(column_name, table_name)
    )

    query = "SELECT COUNT({column}) FROM {table}"
    query_sql = sql.SQL(query).format(
        column=sql.Identifier(column_name), table=sql.Identifier(table_name)
    )
    cursor = conn.get_cursor()
    cursor.execute(query_sql)
    res = cursor.fetchone()

    return res[0]
