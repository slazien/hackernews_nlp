import logging
from typing import Optional

from psycopg2 import sql

from src.db.connection import DBConnection
from src.db.utils import is_table_exists


class TableCreator:
    def __init__(self, conn: DBConnection, table_name: str):
        self.table_name = table_name
        self.conn_object = conn
        self.conn = conn.get_conn()
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def get_name(self) -> str:
        """
        Get table's name
        :return: string with table's name
        """
        return self.table_name

    def create_table(self, query: str, query_index: Optional[str]) -> bool:
        """
        Create a table
        :param query: string with SQL query to create a table
        :param query_index: optional SQL query to create an index on a column
        :return: True if a table was created, False otherwise
        """
        logging.info("creating table: %s", self.table_name)
        query_table = sql.SQL(query)

        if not is_table_exists(self.conn_object, self.table_name):
            # Create table
            self.cursor.execute(query_table)
            # Create index
            if query_index is not None:
                self.cursor.execute(sql.SQL(query_index))
            self.cursor.close()
            self.conn.close()
            return True

        logging.info("table %s already exists, skipping", self.table_name)
        return False
