import logging

import psycopg2


class DBConnection:
    def __init__(self, user: str, password: str, db_name: str):
        logging.info(
            "creating db connection for user: {}, db_name: {}".format(user, db_name)
        )
        self.conn = psycopg2.connect(
            database=db_name,
            user=user,
            password=password,
            host="127.0.0.1",
            port="5432",
        )
        logging.info("creating cursor")
        self.cursor = self.conn.cursor()

    def get_conn(self):
        return self.conn

    def close_conn(self):
        logging.info("closing connection")
        return self.conn.close()

    def get_cursor(self):
        return self.cursor

    def close_cursor(self):
        logging.info("closing cursor")
        self.cursor.close()
