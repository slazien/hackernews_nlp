import logging
from typing import List, NamedTuple

from psycopg2 import sql

from src.db.connection import DBConnection
from src.db.constants import (
    PRIMARY_KEY_NAME_ITEMS,
    PRIMARY_KEY_NAME_USERS,
    TABLE_NAME_ITEMS,
    TABLE_NAME_TEXTS,
    TABLE_NAME_USERS,
)
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

    def insert_item(self, item: Item):
        """
        Insert an Item object into the DB if it doesn't exist, skip otherwise
        :param item: Item object to insert
        :return: True if item was inserted, False otherwise
        """
        logging.info("inserting item: %s", item)
        id = item.get_property("id")
        deleted = item.get_property("deleted")
        type = item.get_property("type")
        by = item.get_property("by")
        time = item.get_property("time")
        text = item.get_property("text")
        text = text.replace("\x00", "") if text is not None else text
        dead = item.get_property("dead")
        parent = item.get_property("parent")
        poll = item.get_property("poll")
        kids = item.get_property("kids")
        url = item.get_property("url")
        url = url.replace("\x00", "") if url is not None else url
        score = item.get_property("score")
        title = item.get_property("title")
        title = title.replace("\x00", "") if title is not None else title
        parts = item.get_property("parts")
        descendants = item.get_property("descendants")

        query = """
        INSERT INTO {table} 
        (id, deleted, type, by, time, text, dead, parent, poll, kids, url, score, title, parts, descendants)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """

        query_sql = sql.SQL(query).format(table=sql.Identifier(self.table_name))

        # Insert only if id doesn't already exist
        # if not is_value_exists(
        #     self.conn_obj, self.table_name, self.primary_key_name, id
        # ):
        self.cursor.execute(
            query_sql,
            (
                id,
                deleted,
                type,
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
        # logging.info("item inserted: %s", item)
        # return True

        # logging.info("item already exists: %s", item)
        # return False

    def insert_items_batch(self, item_list: List[Item]):
        logging.info("inserting item batch")
        query = """
                    INSERT INTO {table} 
                    (id, deleted, type, by, time, text, dead, parent, poll, kids, url, score, title, parts, descendants)
                    VALUES 
                    """
        query_values = ",".join(
            self.cursor.mogrify(
                "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                item.to_tuple(),
            ).decode("utf-8")
            for item in item_list
        )

        query = query + query_values + "ON CONFLICT (id) DO NOTHING;"

        query_sql = sql.SQL(query).format(table=sql.Identifier(self.table_name))
        self.cursor.execute(query_sql)


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

    def insert_user(self, user: User):
        """
        Insert a User object into the DB if it doesn't exist, skip otherwise
        :param user: User object to insert
        :return: True if user was inserted, False otherwise
        """
        logging.info("inserting user: %s", user)
        id = user.get_property("id")
        created = user.get_property("created")
        karma = user.get_property("karma")
        about = user.get_property("about")
        about = about.replace("\x00", "") if about is not None else about
        submitted = user.get_property("submitted")

        query = """
        INSERT INTO {} (id, created, karma, about, submitted) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """

        query_sql = sql.SQL(query).format(sql.Identifier(self.table_name))

        # if not is_value_exists(
        #     self.conn_obj, self.table_name, self.primary_key_name, id
        # ):
        self.cursor.execute(query_sql, (id, created, karma, about, submitted))
        # logging.info("user inserted: %s", user)
        # return True

        # logging.info("user already exists: %s", user)
        # return False


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
        logging.info("inserting text: %s", text)

        id_item = text.get_id_item()
        text_str = text.get_text()
        text_str = text_str.replace("\x00", "") if text_str is not None else text_str

        # query = """
        # INSERT INTO {} (id_item, text) VALUES (%s, %s)
        # ON CONFLICT (id_item) DO UPDATE SET text = %s;
        # """

        query = """
        INSERT INTO {} (id_item, text) VALUES (%s, %s)
        ON CONFLICT (id_item) DO NOTHING;
        """

        query_sql = sql.SQL(query).format(sql.Identifier(self.table_name))

        self.cursor.execute(query_sql, (id_item, text_str))

    def insert_sentiment(self, sentiment: NamedTuple, item_id: int):
        """
        Inserts a NamedTuple containing (polarity, subjectivity) scores for a given item_id
        :param sentiment: a NamedTuple containing subjectivity and polarity scores in float
        :param item_id: ID of the item to insert sentiment for
        :return:
        """
        logging.info("inserting sentiment: %s for item id: %s", sentiment, item_id)

        # query = """
        # INSERT INTO {} (id_item, polarity, subjectivity) VALUES (%s, %s, %s)
        # ON CONFLICT (id_item) DO UPDATE SET polarity = %s, subjectivity = %s;
        # """

        query = """
                INSERT INTO {} (id_item, polarity, subjectivity) VALUES (%s, %s, %s) 
                ON CONFLICT (id_item) DO NOTHING;
                """

        query_sql = sql.SQL(query).format(sql.Identifier(TABLE_NAME_TEXTS))

        polarity = sentiment[0] if sentiment is not None else None
        subjectivity = sentiment[1] if sentiment is not None else None
        self.cursor.execute(query_sql, (item_id, polarity, subjectivity))
