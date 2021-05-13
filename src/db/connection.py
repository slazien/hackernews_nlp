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
        """
        Return psycopg2's connection object
        :return: psycopg2 connection object
        """
        return self.conn

    def close_conn(self):
        """
        Close DB connection
        :return:
        """
        logging.info("closing connection")
        self.conn.close()

    def get_cursor(self):
        """
        Get a cursor object
        :return: cursor object
        """
        return self.cursor

    def get_named_cursor(self, name: str):
        """
        Return a named cursor ("server side cursor") which can transfer only a certain amount of data. Useful for
        creating a "generator-like" cursor
        :param name: name of the cursor
        :return: named cursor
        """
        return self.conn.cursor(name=name)

    def close_cursor(self):
        """
        Close a cursor
        :return:
        """
        logging.info("closing cursor")
        self.cursor.close()
