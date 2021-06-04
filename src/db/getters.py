import logging
from typing import List, Optional

from psycopg2 import sql

from src.db.connection import DBConnection
from src.db.constants import (
    COLUMN_NAME_USER_ID_ITEMS,
    COLUMN_NAME_USER_ID_USERS,
    PRIMARY_KEY_NAME_ITEMS,
    PRIMARY_KEY_NAME_USERS,
    TABLE_NAME_ITEMS,
    TABLE_NAME_USERS,
)
from src.db.utils import all_values_exist, is_value_exists
from src.entities.item import Item
from src.entities.user import User


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
        """
        Check if an item with a given ID exists in the DB
        :param item_id: Id of the item to check existence of
        :return: True if item exists, False otherwise
        """
        return is_value_exists(
            self.conn_obj, self.table_name, self.col_name_id, item_id
        )

    def _are_items_exist(self, item_ids: tuple) -> bool:
        """
        Check if all item IDs in a given list exist in the DB
        :param item_ids: list of item IDs to check existence of
        :return: True if all items exist in DB, False otherwise
        """
        return all_values_exist(
            self.conn_obj, self.table_name, self.col_name_id, item_ids
        )

    def get_item(self, item_id: int) -> Optional[Item]:
        """
        Get an Item object with a given ID from the DB
        :param item_id: ID of the item to get
        :return: Item object if exists, None otherwise
        """
        logging.info("getting item with id: %s", item_id)

        if self._is_item_exists(item_id):
            query = "SELECT * FROM {table} WHERE {column} = %s;"
            query_sql = sql.SQL(query).format(
                table=sql.Identifier(self.table_name),
                column=sql.Identifier(self.col_name_id),
            )
            self.cursor.execute(query_sql, item_id)

            return Item().from_db_call(self.cursor.fetchall())

        logging.info("item with id: %s does not exist", item_id)
        return None

    def get_item_range(
        self, item_id_start: int, item_id_end: int
    ) -> Optional[List[Item]]:
        """
        Get all items for a given range of item IDs
        :param item_id_start: beginning ID for the range
        :param item_id_end: ending ID for the range
        :return: a list of Item objects, None if not all items exist
        """
        logging.info(
            "getting items with ids in range: [%s, %s]", item_id_start, item_id_end
        )

        id_range = tuple(list(range(item_id_start, item_id_end + 1)))

        if self._are_items_exist(id_range):
            query = "SELECT * FROM {table} WHERE {column} IN %s;"
            query_sql = sql.SQL(query).format(
                table=sql.Identifier(self.table_name),
                column=sql.Identifier(self.col_name_id),
            )
            self.cursor.execute(query_sql, id_range)
            res = self.cursor.fetchall()

            return [Item().from_tuple(item) for item in res]

        return None


class UserGetter:
    def __init__(
        self,
        conn: DBConnection,
        table_name: str = TABLE_NAME_USERS,
        column_name: str = PRIMARY_KEY_NAME_USERS,
    ):
        self.conn_obj = conn
        self.conn = conn.get_conn()
        self.cursor = conn.get_cursor()
        self.table_name = table_name
        self.col_name_id = column_name

    def _is_user_exists(self, user_id: str) -> bool:
        """
        Check if a user with a given ID exists in the DB
        :param user_id: Id of the user to check existence of
        :return: True if user exists, False otherwise
        """
        return is_value_exists(
            self.conn_obj, self.table_name, self.col_name_id, user_id
        )

    def _are_users_exist(self, user_ids: tuple) -> bool:
        """
        Check if all user IDs in a given list exist in the DB
        :param user_ids: list of user IDs to check existence of
        :return: True if all users exist in DB, False otherwise
        """
        return all_values_exist(
            self.conn_obj, self.table_name, self.col_name_id, user_ids
        )

    def get_user(self, user_id: str) -> Optional[User]:
        """
        Get a User object with a given ID from the DB
        :param user_id: ID of the user to get
        :return: User object if exists, None otherwise
        """
        logging.info("getting user with id: %s", user_id)

        if self._is_user_exists(user_id):
            query = "SELECT * FROM {table} WHERE {column} = %s;"
            query_sql = sql.SQL(query).format(
                table=sql.Identifier(self.table_name),
                column=sql.Identifier(self.col_name_id),
            )
            self.cursor.execute(query_sql, user_id)
            user = User().from_db_call(self.cursor.fetchall())

            return user

        logging.info("user with id: %s does not exist", user_id)
        return None

    def get_all_user_ids(self, table_name: str) -> Optional[List[str]]:
        """
        Get all user IDs existing in the DB
        :param table_name: name of the table from which to get user IDs
        :return: a list of user IDs (if any)
        """
        logging.info("getting all user ids")

        query = "SELECT DISTINCT {column} FROM {table};"
        query_sql = sql.SQL(query).format(
            column=sql.Identifier(
                COLUMN_NAME_USER_ID_ITEMS
                if table_name == TABLE_NAME_ITEMS
                else COLUMN_NAME_USER_ID_USERS
            ),
            table=sql.Identifier(table_name),
        )
        self.cursor.execute(query_sql)
        res = self.cursor.fetchall()

        return [user_id[0] for user_id in res if user_id[0] is not None]
