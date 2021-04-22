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
from src.db.utils import is_value_exists

PATH = "../../data/items"


class DownloadItems(luigi.Task):
    def run(self):
        conn = DBConnection(
            user="postgres", password=DB_PASSWORD, db_name=DB_NAME_HACKERNEWS
        )
        item_api = ItemAPI()
        item_inserter = ItemInserter(conn, TABLE_NAME_ITEMS)

        max_item_id = item_api.get_maxitem_id()
        for item_id in tqdm(range(1, max_item_id)):
            current_item = item_api.get_item(item_id=item_id)
            if not is_value_exists(
                conn, TABLE_NAME_ITEMS, PRIMARY_KEY_NAME_ITEMS, current_item.get_id()
            ):
                item_inserter.insert_item(current_item)
