import argparse
import logging
from math import ceil
from time import time

import luigi

from src.db.setup import Setup
from src.tasks.download_items import DownloadItemsTask

# Use the current epoch time as log file name
LOG_FILENAME = "logs/{}.log".format(str(int(time())))
parser = argparse.ArgumentParser()
parser.add_argument("--startid", help="id of the first item to download", type=int)
parser.add_argument("--endid", help="id of the last item to download", type=int)
parser.add_argument("--workers", help="number of workers to run the job", type=int)
parser.add_argument(
    "--loggingenabled", help="should logging be enabled (1 = yes, 0 = no)", type=int
)
args = parser.parse_args()

# DISABLE LOGGING?
if args.loggingenabled == 0:
    logging.disable(level=logging.CRITICAL)


def main():
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

    ranges = []
    chunk_size = int(ceil((args.endid - args.startid) / args.workers))

    for n in range(1, args.workers + 1):
        start = args.startid + (n - 1) * chunk_size
        end = args.startid + n * chunk_size
        ranges.append((start, end))

    # If any items left over, add them to a final range
    if ranges[-1][1] != args.endid:
        ranges.append((ranges[-1][1], args.endid))

    logging.info("item ranges for jobs: {}".format(ranges))

    # For each range, create a new Luigi task
    print("ranges:", ranges)
    task_list = []

    for range_ids in ranges:
        start_id = range_ids[0]
        end_id = range_ids[1]
        task_list.append(DownloadItemsTask(start_id=start_id, end_id=end_id))

    luigi.build(
        task_list,
        workers=len(ranges),
        local_scheduler=True,
    )


if __name__ == "__main__":
    main()
