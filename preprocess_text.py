import argparse
import logging
from time import time

from src.db.connection import DBConnection
from src.db.constants import *
from src.db.utils import get_column_values, get_value_count_in_column
from src.preprocessing.pipeline import TextPreprocessor

parser = argparse.ArgumentParser()
parser.add_argument(
    "--filename", help="path to the file in which to save preprocessed texts", type=str
)
parser.add_argument(
    "--batchsize",
    help="size of batch to use for parallel processing of texts",
    type=int,
)
parser.add_argument(
    "--logging-enabled",
    help="whether to enable logging for this script [Y/N]",
    type=str,
)
args = parser.parse_args()

LOG_FILENAME = "logs/preprocess_text_{}.log".format(str(int(time())))

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
    # Get all texts from DB
    conn = DBConnection("postgres", DB_PASSWORD, DB_NAME_HACKERNEWS)

    text_generator = get_column_values(
        conn, TABLE_NAME_ITEMS, TABLE_ITEMS["COLUMN_NAME_TEXT"]
    )
    total_text_count = get_value_count_in_column(
        conn, TABLE_NAME_ITEMS, TABLE_ITEMS["COLUMN_NAME_TEXT"]
    )

    text_preprocessor = TextPreprocessor()

    text_preprocessor.process_multiprocessing(
        text_generator, args.filename, total_text_count, args.batchsize
    )


if __name__ == "__main__":
    main()
