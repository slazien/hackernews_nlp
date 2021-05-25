import logging

import luigi
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


class TaskDownloadItems(luigi.Task):
    ids_to_download = luigi.ListParameter()

    def run(self):
        """
        Run the download task using Luigi for a given range of item IDs
        :return:
        """
        logging.info("starting luigi task: %s", self.__class__)
        conn = DBConnection(
            user="postgres", password=DB_PASSWORD, db_name=DB_NAME_HACKERNEWS
        )
        item_api = ItemAPI()
        item_inserter = ItemInserter(conn, TABLE_NAME_ITEMS, PRIMARY_KEY_NAME_ITEMS)

        for item_id in tqdm(self.ids_to_download):
            current_item = item_api.get_item(item_id=item_id)
            if current_item is not None:
                item_inserter.insert_item(current_item)

        logging.info("finished task: %s", self.__class__)
