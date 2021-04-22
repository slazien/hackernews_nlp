import logging

from src.db.connection import DBConnection
from src.db.constants import TABLE_NAME_ITEMS
from src.db.utils import is_value_exists
from src.entities.item import Item


class ItemInserter:
    def __init__(self, conn: DBConnection, table_name: str = TABLE_NAME_ITEMS):
        self.conn_obj = conn
        self.conn = conn.get_conn()
        self.cursor = conn.get_cursor()
        # self.conn.autocommit = True
        self.table_name = table_name
        self.primary_key_name = "id"

    def insert_item(self, item: Item):
        logging.info("inserting item: {}".format(item))
        id = item.get_property("id")
        deleted = item.get_property("deleted")
        type = item.get_property("type")
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

        sql = """
        INSERT INTO {} (id, deleted, type, by, time, text, dead, parent, poll, kids, url, score, title, parts, descendants)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """.format(
            self.table_name
        )

        # Insert only if id doesn't already exist
        if not is_value_exists(
            self.conn_obj, self.table_name, self.primary_key_name, id
        ):
            self.cursor.execute(
                sql,
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
            self.conn.commit()
            logging.info("item inserted: {}".format(item))
        else:
            logging.info("item already exists: {}".format(item))
