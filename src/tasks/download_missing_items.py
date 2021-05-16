import logging

import luigi
from psycopg2 import sql
from tqdm import tqdm

from src.api_helper.item import ItemAPI
from src.db.connection import DBConnection
from src.db.constants import (
    DB_NAME_HACKERNEWS,
    DB_PASSWORD,
    PRIMARY_KEY_NAME_ITEMS,
    TABLE_NAME_ITEMS,
)
from src.db.inserters import ItemInserter


class TaskDownloadMissingItems(luigi.Task):
    start_id = luigi.IntParameter()
    end_id = luigi.IntParameter()

    def run(self):
        """
        Run the download task using Luigi for all missing IDs in the range [start_id, end_id]
        :return:
        """
        logging.info("starting luigi task: {}".format(self.__class__))
        conn = DBConnection(
            user="postgres", password=DB_PASSWORD, db_name=DB_NAME_HACKERNEWS
        )
        cursor = conn.get_cursor()

        item_api = ItemAPI()
        item_inserter = ItemInserter(conn, TABLE_NAME_ITEMS, PRIMARY_KEY_NAME_ITEMS)

        desired_ids = set(list(range(self.start_id, self.end_id + 1)))
        query = "SELECT DISTINCT {} FROM {};"
        query_sql = sql.SQL(query).format(
            sql.Identifier(PRIMARY_KEY_NAME_ITEMS), sql.Identifier(TABLE_NAME_ITEMS)
        )
        cursor.execute(query_sql)
        ids_in_db = set([row[0] for row in cursor.fetchall()])
        items_ids_to_download = desired_ids - ids_in_db

        for item_id in tqdm(items_ids_to_download):
            current_item = item_api.get_item(item_id=item_id)
            if current_item is not None:
                item_inserter.insert_item(current_item)

        logging.info("finished task: {}".format(self.__class__))
