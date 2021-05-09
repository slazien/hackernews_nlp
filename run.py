import argparse
import logging
from math import ceil
from time import time

import luigi

from src.db.connection import DBConnection
from src.db.constants import *
from src.db.getters import UserGetter
from src.db.setup import Setup
from src.tasks.download_items import DownloadItemsTask
from src.tasks.download_users import DownloadUsersTask
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

    # Split item id list into chunks for each worker
    chunk_size_items = int(ceil((args.endid - args.startid) / args.workers))

    ranges_items = [
        (sublist[0], sublist[-1])
        for sublist in chunk_for_size(
            list(range(args.startid, args.endid)), chunk_size_items
        )
    ]

    num_workers = len(ranges_items)

    logging.info("item ranges for jobs: {}".format(ranges_items))

    # For each range, create a new Luigi task
    task_list = []

    for range_ids in ranges_items:
        start_id = range_ids[0]
        end_id = range_ids[1]
        task_list.append(DownloadItemsTask(start_id=start_id, end_id=end_id))

    conn = DBConnection("postgres", DB_PASSWORD, DB_NAME_HACKERNEWS)
    user_getter = UserGetter(conn, TABLE_NAME_USERS, PRIMARY_KEY_NAME_USERS)
    user_ids_all = user_getter.get_all_user_ids()

    # If asked to download users, add a task
    if args.downloadusers.lower() == "y":
        chunk_size_users = int(len(user_ids_all) / args.workers)
        ranges_users = chunk_for_size(user_ids_all, chunk_size_users)
        for range_users in ranges_users:
            task_list.append(DownloadUsersTask(user_ids=range_users))

    luigi.build(
        task_list,
        workers=num_workers,
        local_scheduler=True,
    )


if __name__ == "__main__":
    main()
