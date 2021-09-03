import argparse
import logging
from time import time

from psycopg2 import sql

from src.db.connection import DBConnection
from src.db.constants import (
    DB_NAME_HACKERNEWS,
    DB_PASSWORD,
    TABLE_NAME_ITEMS,
    TABLE_NAME_TEXTS,
)
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

    def data_generator(conn: DBConnection):
        cursor = conn.get_named_cursor("compute_sentiment_cursor")
        cursor.itersize = args.batch_size

        # Get existing id_item from texts to avoid checking them again
        query = """
        SELECT id, title, text 
        FROM {table_items}
        WHERE NOT EXISTS (
        SELECT 
        FROM {table_texts}
        WHERE id = {table_texts}.id_item
        )
        ORDER BY id;
        """

        # query = "SELECT id, title, text FROM {table} ORDER BY id ASC;"
        query_sql = sql.SQL(query).format(
            table_items=sql.Identifier(TABLE_NAME_ITEMS),
            table_texts=sql.Identifier(TABLE_NAME_TEXTS),
        )
        cursor.execute(query_sql)

        while True:
            rows = cursor.fetchmany(args.batch_size)
            if not rows:
                break
            for row in rows:
                yield row

    generator = data_generator(conn)

    run(generator, True, args.batch_size)

    # NCORE = 3
    #
    # q = mp.Queue(maxsize=NCORE)
    # iolock = mp.Lock()
    #
    # pool = mp.Pool(
    #     NCORE,
    #     initializer=run,
    #     initargs=(
    #         q,
    #         True,
    #         iolock,
    #         args.batch_size,
    #     ),
    # )
    #
    # for row in generator:
    #     q.put(row)
    #
    # for _ in range(NCORE):
    #     q.put(None)
    #
    # pool.close()
    # pool.join()
    #
    # run(
    #     data_generator=generator,
    #     process_text=True,
    #     batch_size=args.batch_size,
    # )


if __name__ == "__main__":
    main()
