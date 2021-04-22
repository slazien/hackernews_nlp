import logging

from psycopg2 import sql

from src.db.connection import DBConnection
from src.db.constants import TABLE_NAME_ITEMS
from src.db.utils import is_table_exists


class CreateTableItems:
    def __init__(self, conn: DBConnection, table_name: str = TABLE_NAME_ITEMS):
        self.table_name = table_name
        self.conn_object = conn
        self.conn = conn.get_conn()
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def create_table(self):
        logging.info("creating table: {}".format(self.table_name))
        query = sql.SQL(
            """
        CREATE TABLE items (
            id integer PRIMARY KEY,
            deleted bool,
            type varchar,
            by varchar,
            time bigint,
            text varchar,
            dead bool,
            parent integer,
            poll integer,
            kids integer[],
            url varchar,
            score integer,
            title varchar,
            parts integer[],
            descendants integer
            );
        """
        )

        if not is_table_exists(self.conn_object, self.table_name):
            self.cursor.execute(query)
            self.cursor.close()
            self.conn.close()
        else:
            logging.info("table {} already exists, skipping".format(self.table_name))
