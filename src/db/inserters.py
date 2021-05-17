import logging
from typing import NamedTuple

from psycopg2 import sql

from src.db.connection import DBConnection
from src.db.constants import *
from src.db.utils import is_value_exists
from src.entities.item import Item
from src.entities.text import Text
from src.entities.user import User


class ItemInserter:
    def __init__(
        self,
        conn: DBConnection,
        table_name: str = TABLE_NAME_ITEMS,
        primary_key_name: str = PRIMARY_KEY_NAME_ITEMS,
    ):
        self.conn_obj = conn
        self.conn = conn.get_conn()
        self.cursor = conn.get_cursor()
        self.conn.autocommit = True
        self.table_name = table_name
        self.primary_key_name = primary_key_name

    def insert_item(self, item: Item) -> bool:
        """
        Insert an Item object into the DB if it doesn't exist, skip otherwise
        :param item: Item object to insert
        :return: True if item was inserted, False otherwise
        """
        logging.info("inserting item: {}".format(item))
        _id = item.get_property("id")
        deleted = item.get_property("deleted")
        _type = item.get_property("type")
        by = item.get_property("by")
        time = item.get_property("time")
        text = item.get_property("text")
        dead = item.get_property("dead")
        parent = item.get_property("parent")
        poll = item.get_property("poll")
        kids = item.get_property("kids")
        url = item.get_property("url")
        score = item.get_property("score")
        title = item.get_property("title")
        parts = item.get_property("parts")
        descendants = item.get_property("descendants")

        query = """
        INSERT INTO {table} 
        (id, deleted, type, by, time, text, dead, parent, poll, kids, url, score, title, parts, descendants)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        query_sql = sql.SQL(query).format(table=sql.Identifier(self.table_name))

        # Insert only if id doesn't already exist
        if not is_value_exists(
            self.conn_obj, self.table_name, self.primary_key_name, _id
        ):
            self.cursor.execute(
                query_sql,
                (
                    _id,
                    deleted,
                    _type,
                    by,
                    time,
                    text,
                    dead,
                    parent,
                    poll,
                    kids,
                    url,
                    score,
                    title,
                    parts,
                    descendants,
                ),
            )
            logging.info("item inserted: {}".format(item))
            return True
        else:
            logging.info("item already exists: {}".format(item))
            return False


class UserInserter:
    def __init__(
        self,
        conn: DBConnection,
        table_name: str = TABLE_NAME_USERS,
        primary_key_name: str = PRIMARY_KEY_NAME_USERS,
    ):
        self.conn_obj = conn
        self.conn = conn.get_conn()
        self.cursor = conn.get_cursor()
        self.conn.autocommit = True
        self.table_name = table_name
        self.primary_key_name = primary_key_name

    def insert_user(self, user: User) -> bool:
        """
        Insert a User object into the DB if it doesn't exist, skip otherwise
        :param user: User object to insert
        :return: True if user was inserted, False otherwise
        """
        logging.info("inserting user: {}".format(user))
        _id = user.get_property("id")
        created = user.get_property("created")
        karma = user.get_property("karma")
        about = user.get_property("about")
        submitted = user.get_property("submitted")

        query = "INSERT INTO {} (id, created, karma, about, submitted) VALUES (%s, %s, %s, %s, %s);"

        query_sql = sql.SQL(query).format(sql.Identifier(self.table_name))

        if not is_value_exists(
            self.conn_obj, self.table_name, self.primary_key_name, _id
        ):
            self.cursor.execute(query_sql, (_id, created, karma, about, submitted))
            logging.info("user inserted: {}".format(user))
            return True
        else:
            logging.info("user already exists: {}".format(user))
            return False


class TextInserter:
    def __init__(self, conn: DBConnection, table_name: str, primary_key_name: str):
        self.conn_obj = conn
        self.conn = conn.get_conn()
        self.cursor = conn.get_cursor()
        self.conn.autocommit = True
        self.table_name = table_name
        self.primary_key_name = primary_key_name

    def insert_text(self, text: Text):
        """
        Insert a Text object into the DB if it doesn't exist, skip otherwise
        :param text: text to insert
        :return:
        """
        logging.info("inserting text: {}".format(text))

        id_item = text.get_id_item()
        text_str = text.get_text()

        query = """
        INSERT INTO {} (id_item, text) VALUES (%s, %s)
        ON CONFLICT (id_item) DO UPDATE SET text = %s;
        """
        query_sql = sql.SQL(query).format(sql.Identifier(self.table_name))

        self.cursor.execute(query_sql, (id_item, text_str, text_str))

    def insert_sentiment(self, sentiment: NamedTuple, item_id: int):
        """
        Inserts a NamedTuple containing (polarity, subjectivity) scores for a given item_id
        :param sentiment: a NamedTuple containing subjectivity and polarity scores in float
        :param item_id: ID of the item to insert sentiment for
        :return:
        """
        logging.info(
            "inserting sentiment: {} for item id: {}".format(sentiment, item_id)
        )

        query = """
        INSERT INTO {} (id_item, polarity, subjectivity) VALUES (%s, %s, %s) 
        ON CONFLICT (id_item) DO UPDATE SET polarity = %s, subjectivity = %s;
        """
        query_sql = sql.SQL(query).format(sql.Identifier(TABLE_NAME_TEXTS))

        polarity = sentiment[0] if sentiment is not None else None
        subjectivity = sentiment[1] if sentiment is not None else None
        self.cursor.execute(
            query_sql, (item_id, polarity, subjectivity, polarity, subjectivity)
        )
