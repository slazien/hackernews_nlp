import logging
from typing import List, Optional

from psycopg2 import sql

from src.db.connection import DBConnection
from src.db.constants import PRIMARY_KEY_NAME_ITEMS, TABLE_NAME_ITEMS
from src.db.utils import all_values_exist, is_value_exists
from src.entities.item import Item


class ItemGetter:
    def __init__(
        self,
        conn: DBConnection,
        table_name: str = TABLE_NAME_ITEMS,
        column_name: str = PRIMARY_KEY_NAME_ITEMS,
    ):
        self.conn_obj = conn
        self.conn = conn.get_conn()
        self.cursor = conn.get_cursor()
        self.table_name = table_name
        self.col_name_id = column_name

    def _is_item_exists(self, item_id: int) -> bool:
        return is_value_exists(
            self.conn_obj, self.table_name, self.col_name_id, item_id
        )

    def _are_items_exist(self, item_ids: tuple) -> bool:
        return all_values_exist(
            self.conn_obj, self.table_name, self.col_name_id, item_ids
        )

    def get_item(self, item_id: int) -> Optional[Item]:
        logging.info("getting item with id: {}".format(item_id))
        if self._is_item_exists(item_id):
            query = """
            SELECT * FROM {} WHERE {} = {};
            """.format(
                self.table_name, self.col_name_id, item_id
            )
            print(query)
            self.cursor.execute(query)
            item = Item().from_db_call(self.cursor.fetchall())
            return item
        else:
            logging.info("item with id: {} does not exist".format(item_id))
            return None

    def get_item_range(
        self, item_id_start: int, item_id_end: int
    ) -> Optional[List[Item]]:
        logging.info(
            "getting items with ids in range: [{}, {}]".format(
                item_id_start, item_id_end
            )
        )
        id_range = tuple(list(range(item_id_start, item_id_end + 1)))
        if self._are_items_exist(id_range):
            query = """
            SELECT * FROM {} WHERE {} IN {}
            """.format(
                self.table_name, self.col_name_id, id_range
            )
            self.cursor.execute(sql.SQL(query))
            res = self.cursor.fetchall()
            return [Item().from_tuple(item) for item in res]
        else:
            return None
