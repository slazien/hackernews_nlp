from src.db.connection import DBConnection
from src.db.utils import is_table_exists


class CreateTableItems:
    def __init__(self, conn: DBConnection, db_name: str, table_name: str = "items"):
        self.db_name = db_name
        self.table_name = table_name
        self.conn = conn.get()
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def create_table(self):
        sql = """
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

        if not is_table_exists(self.conn, "items"):
            self.cursor.execute(sql)
            self.cursor.close()
            self.conn.close()
