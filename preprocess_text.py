import argparse
import zipfile

from src.db.connection import DBConnection
from src.db.constants import *
from src.db.utils import get_column_values, get_value_count_in_column
from src.models.word2vec import Word2VecTrainer
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
args = parser.parse_args()


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
