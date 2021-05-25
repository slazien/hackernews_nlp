import argparse
import logging
from math import ceil
from time import time

import luigi
from psycopg2 import sql

from src.db.connection import DBConnection
from src.db.constants import *
from src.db.getters import UserGetter
from src.db.setup import Setup
from src.tasks.download_items import TaskDownloadItems
from src.tasks.download_users import TaskDownloadUsers
from src.utils.list import chunk_for_size

# Use the current epoch time as log file name
LOG_FILENAME = "logs/download_{}.log".format(str(int(time())))
parser = argparse.ArgumentParser()
parser.add_argument("--startid", help="id of the first item to download", type=int)
parser.add_argument("--endid", help="id of the last item to download", type=int)
parser.add_argument(
    "--download-users",
    help="[Y/N] whether to download users for user ids existing in the DB",
    type=str,
)
parser.add_argument("--workers", help="number of workers to run the job", type=int)
parser.add_argument(
    "--logging-enabled", help="[Y/N] Should logging be enabled?", type=str
)
args = parser.parse_args()

# DISABLE LOGGING?
if args.logging_enabled.lower() != "y":
    logging.disable(level=logging.CRITICAL)
else:
    logging.basicConfig(
        filename=LOG_FILENAME,
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )


def main():
    """
    Set up the DB and tables, download items for a given item ID range, insert them into the DB
    :return:
    """

    # Set up DB
    setup = Setup()
    setup.run()
    conn = DBConnection("postgres", DB_PASSWORD, DB_NAME_HACKERNEWS)

    # Check which (if any) IDs exist in the DB already
    cursor = conn.get_cursor()
    desired_ids = set(list(range(args.startid, args.endid + 1)))

    # Get all distinct IDs (if any) from the DB
    query = "SELECT DISTINCT {} FROM {};"
    query_sql = sql.SQL(query).format(
        sql.Identifier(PRIMARY_KEY_NAME_ITEMS), sql.Identifier(TABLE_NAME_ITEMS)
    )
    cursor.execute(query_sql)
    res_ids = cursor.fetchall()

    # If no IDs exist in DB
    if len(res_ids) == 0:
        ids_in_db = set()
    else:
        ids_in_db = set([row[0] for row in res_ids])

    item_ids_to_download = sorted(list(desired_ids - ids_in_db))

    # If no items to download, exit
    if len(item_ids_to_download) == 0:
        exit(0)

    # Split item id list into chunks for each worker
    chunk_size_items = int(ceil(len(item_ids_to_download) / args.workers))
    item_ids_to_download_chunks = chunk_for_size(item_ids_to_download, chunk_size_items)
    logging.info("item ranges for jobs: {}".format(item_ids_to_download_chunks))

    # For each chunk, create a new Luigi task
    task_list = []
    num_workers = 0
    for chunk in item_ids_to_download_chunks:
        task_list.append(TaskDownloadItems(ids_to_download=chunk))
        num_workers += 1

    # If asked to download users, add a task
    if args.download_users.lower() == "y":
        # Get all user IDs currently in the "items" table
        user_getter = UserGetter(conn, TABLE_NAME_USERS, PRIMARY_KEY_NAME_USERS)
        user_ids_all = user_getter.get_all_user_ids()

        # Build user ranges to download for each task
        chunk_size_users = int(len(user_ids_all) / args.workers)
        ranges_users = chunk_for_size(user_ids_all, chunk_size_users)
        for range_users in ranges_users:
            task_list.append(TaskDownloadUsers(user_ids=range_users))

    luigi.build(
        task_list,
        workers=num_workers,
        local_scheduler=True,
    )


if __name__ == "__main__":
    main()
