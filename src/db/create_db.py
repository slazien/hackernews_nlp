from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from src.db.connection import DBConnection


class DBCreate:
    def __init__(self, conn: DBConnection, db_name: str):
        self.db_name = db_name
        self.cursor = conn.get_cursor()
        self.conn = conn.get_conn()

    def create_db(self):
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.conn.autocommit = True

        # Check if DB exists
        query = sql.SQL("SELECT datname FROM pg_database")
        self.cursor.execute(query)
        dbs = self.cursor.fetchall()

        if (self.db_name,) not in dbs:
            self.cursor.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.db_name))
            )

        self.cursor.close()
        self.conn.close()
