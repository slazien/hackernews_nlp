import argparse
import logging
from time import time

import luigi

from src.tasks.compute_insert_sentiment import TaskComputeInsertSentiment

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
    task_compute_insert_sentiment = TaskComputeInsertSentiment(
        process_text=True if args.insert_preprocessed_texts.lower() == "y" else False
    )
    luigi.build([task_compute_insert_sentiment], workers=WORKERS, local_scheduler=True)


if __name__ == "__main__":
    main()
