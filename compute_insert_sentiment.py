import argparse
import logging
from time import time

from src.db.connection import DBConnection
from src.db.constants import DB_NAME_HACKERNEWS, DB_PASSWORD, TABLE_NAME_ITEMS
from src.db.utils import get_column_values
from src.tasks.compute_insert_sentiment import run

parser = argparse.ArgumentParser()
parser.add_argument(
    "--insert-preprocessed-texts",
    help="whether to preprocess and insert texts into DB while computing sentiment [Y/N]",
    type=str,
)
parser.add_argument(
    "--logging-enabled",
    help="whether to enable logging for this script [Y/N]",
    type=str,
)
parser.add_argument(
    "--batch-size", help="size of each batch when querying the DB for data", type=int
)
args = parser.parse_args()

WORKERS = 1
LOG_FILENAME = "logs/compute_insert_sentiment_{}.log".format(str(int(time())))

# DISABLE LOGGING?
if args.logging_enabled.lower() != "y":
    logging.disable(level=logging.CRITICAL)
else:
    logging.basicConfig(
        filename=LOG_FILENAME,
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.DEBUG,
    )


def main():
    conn = DBConnection(
        user="postgres", password=DB_PASSWORD, db_name=DB_NAME_HACKERNEWS
    )

    item_ids = get_column_values(
        conn, TABLE_NAME_ITEMS, "id", fetch_size=100000, cursor_name="item_id_cursor"
    )
    titles = get_column_values(
        conn, TABLE_NAME_ITEMS, "title", fetch_size=100000, cursor_name="titles_cursor"
    )
    texts = get_column_values(
        conn, TABLE_NAME_ITEMS, "text", fetch_size=100000, cursor_name="texts_cursor"
    )

    run(
        item_ids=item_ids,
        titles=titles,
        texts=texts,
        process_text=True,
        batch_size=args.batch_size,
    )


if __name__ == "__main__":
    main()
