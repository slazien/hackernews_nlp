from psycopg2 import sql

from src.db.connection import DBConnection
from src.db.utils import is_table_exists


class CreateTableItems:
    def __init__(self, conn: DBConnection, table_name: str = "items"):
        self.table_name = table_name
        self.conn = conn.get_conn()
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def create_table(self):
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

        if not is_table_exists(self.conn, self.table_name):
            self.cursor.execute(query)
            self.cursor.close_conn()
            self.conn.close_conn()
