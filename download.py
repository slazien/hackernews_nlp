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
LOG_FILENAME = "logs/{}.log".format(str(int(time())))
parser = argparse.ArgumentParser()
parser.add_argument("--startid", help="id of the first item to download", type=int)
parser.add_argument("--endid", help="id of the last item to download", type=int)
parser.add_argument(
    "--downloadusers",
    help="[Y/N] whether to download users for user ids existing in the DB",
    type=str,
)
parser.add_argument("--workers", help="number of workers to run the job", type=int)
parser.add_argument(
    "--loggingenabled", help="[Y/N] Should logging be enabled?", type=str
)
args = parser.parse_args()

# DISABLE LOGGING?
if args.loggingenabled.lower() != "y":
    logging.disable(level=logging.CRITICAL)


def main():
    """
    Set up the DB and tables, download items for a given item ID range, insert them into the DB
    :return:
    """
    logging.basicConfig(
        filename=LOG_FILENAME,
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.DEBUG,
    )

    # Set up DB
    logging.info("setting up database")
    setup = Setup()
    setup.run()
    logging.info("finished setting up database")
    conn = DBConnection("postgres", DB_PASSWORD, DB_NAME_HACKERNEWS)

    # Check which (if any) IDs exist in the DB already
    cursor = conn.get_cursor()

    desired_ids = set(list(range(args.startid, args.endid + 1)))

    query = "SELECT DISTINCT {} FROM {};"
    query_sql = sql.SQL(query).format(
        sql.Identifier(PRIMARY_KEY_NAME_ITEMS), sql.Identifier(TABLE_NAME_ITEMS)
    )
    cursor.execute(query_sql)

    res = cursor.fetchall()

    if len(res) == 0:
        ids_in_db = set()
    else:
        ids_in_db = set([row[0] for row in res])

    item_ids_to_download = sorted(list(desired_ids - ids_in_db))

    # If no items to download
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

    # Get all user IDs currently in the "items" table
    conn = DBConnection("postgres", DB_PASSWORD, DB_NAME_HACKERNEWS)
    user_getter = UserGetter(conn, TABLE_NAME_USERS, PRIMARY_KEY_NAME_USERS)
    user_ids_all = user_getter.get_all_user_ids()

    # If asked to download users, add a task
    if args.downloadusers.lower() == "y":
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
