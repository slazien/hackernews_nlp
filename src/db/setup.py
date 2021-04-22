from src.db.connection import DBConnection
from src.db.constants import DB_NAME_HACKERNEWS, DB_NAME_INITIAL, DB_PASSWORD
from src.db.create_db import DBCreate
from src.db.create_tables import CreateTableItems


class Setup:
    def __init__(self):
        self.conn_initial = DBConnection(
            user="postgres", password=DB_PASSWORD, db_name=DB_NAME_INITIAL
        )
        self.dbcreate = DBCreate(conn=self.conn_initial, db_name=DB_NAME_HACKERNEWS)

    def run(self):
        self.dbcreate.create_db()
        # self.conn_initial.close()

        conn_hackernews = DBConnection(
            user="postgres", password=DB_PASSWORD, db_name=DB_NAME_HACKERNEWS
        )

        table_items = CreateTableItems(conn=conn_hackernews)

        tables = [table_items]

        for table in tables:
            table.create_table()

        conn_hackernews.close_conn()
