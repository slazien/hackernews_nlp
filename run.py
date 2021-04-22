import argparse
import logging
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
args = parser.parse_args()


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

    luigi.build(
        [DownloadItemsTask(start_id=args.startid, end_id=args.endid)],
        workers=args.workers,
        local_scheduler=True,
    )


if __name__ == "__main__":
    main()
