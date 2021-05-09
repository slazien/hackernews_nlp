import logging

from src.db.connection import DBConnection
from src.db.constants import *
from src.db.create_db import DBCreate
from src.db.create_tables import CreateTable


class Setup:
    def __init__(self):
        self.conn_initial = DBConnection(
            user="postgres", password=DB_PASSWORD, db_name=DB_NAME_INITIAL
        )
        self.dbcreate = DBCreate(conn=self.conn_initial, db_name=DB_NAME_HACKERNEWS)

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

        table_items = CreateTable(conn=conn_hackernews, table_name=TABLE_NAME_ITEMS)
        table_users = CreateTable(conn=conn_hackernews, table_name=TABLE_NAME_USERS)

        tables = [table_items, table_users]

        logging.info("creating all tables")
        for table in tables:
            table_name = table.get_name()
            query_create_table = TABLES[table_name]["QUERY_CREATE_TABLE"]
            query_create_index = TABLES[table_name]["QUERY_CREATE_INDEX"]
            was_created = table.create_table(query_create_table, query_create_index)
            if was_created:
                logging.info("table created: {}".format(table.get_name()))
            else:
                logging.info("table not created: {}".format(table.get_name()))

        conn_hackernews.close_conn()
