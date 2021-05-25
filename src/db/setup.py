import logging

from src.db.connection import DBConnection
from src.db.constants import (
    DB_NAME_HACKERNEWS,
    DB_NAME_INITIAL,
    DB_PASSWORD,
    TABLE_NAME_ITEMS,
    TABLE_NAME_TEXTS,
    TABLE_NAME_USERS,
    TABLES,
)
from src.db.create_db import DBCreator
from src.db.create_tables import TableCreator


class Setup:
    def __init__(self):
        self.conn_initial = DBConnection(
            user="postgres", password=DB_PASSWORD, db_name=DB_NAME_INITIAL
        )
        self.dbcreate = DBCreator(conn=self.conn_initial, db_name=DB_NAME_HACKERNEWS)

    def run(self):
        """
        Create the DB and tables
        :return:
        """
        self.dbcreate.create_db()
        # self.conn_initial.close()

        conn_hackernews = DBConnection(
            user="postgres", password=DB_PASSWORD, db_name=DB_NAME_HACKERNEWS
        )

        table_items = TableCreator(conn=conn_hackernews, table_name=TABLE_NAME_ITEMS)
        table_users = TableCreator(conn=conn_hackernews, table_name=TABLE_NAME_USERS)
        table_texts = TableCreator(conn=conn_hackernews, table_name=TABLE_NAME_TEXTS)

        tables = [table_items, table_users, table_texts]

        logging.info("creating all tables")
        for table in tables:
            table_name = table.get_name()
            query_create_table = TABLES[table_name]["QUERY_CREATE_TABLE"]
            query_create_index = TABLES[table_name]["QUERY_CREATE_INDEX"]
            was_created = table.create_table(query_create_table, query_create_index)
            if was_created:
                logging.info("table created: %s", table.get_name())
            else:
                logging.info("table not created: %s", table.get_name())

        conn_hackernews.close_conn()
