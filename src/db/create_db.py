from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from src.db.connection import DBConnection


class DBCreate:
    def __init__(self, conn: DBConnection, db_name: str):
        self.db_name = db_name
        self.conn = conn.get_conn()

    def create_db(self):
        self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = self.conn.cursor()
        self.conn.autocommit = True

        # Check if DB exists
        query = sql.SQL("SELECT datname FROM pg_database")
        cur.execute(query)
        dbs = cur.fetchall()

        if (self.db_name,) not in dbs:
            cur.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.db_name))
            )

        cur.close_conn()
        self.conn.close_conn()
