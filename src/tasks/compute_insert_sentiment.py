import logging
import multiprocessing as mp
from typing import Iterable

from tqdm import tqdm

from src.db.connection import DBConnection
from src.db.constants import (
    DB_NAME_HACKERNEWS,
    DB_PASSWORD,
    PRIMARY_KEY_NAME_TEXTS,
    TABLE_NAME_TEXTS,
)
from src.db.inserters import TextInserter
from src.entities.text import Text
from src.models.sentiment_analysis import SentimentClassifier
from src.preprocessing.pipeline import TextPreprocessor
from src.utils.text import is_string_empty


def run(
    data_generator,
    process_text: bool,
    batch_size: int = 100000,
):
    """
    Compute and insert sentiment scores (polarity, subjectivity) into DB for all existing items
    :param data_generator: an iterable where each element is a single row from the DB
    :param process_text: whether to process the text or not before computing sentiment and inserting into DB
    :param batch_size: size of the batch to use for the named cursor when querying DB for data
    :return:
    """
    logging.info("starting task %s", __name__)
    conn = DBConnection(
        user="postgres", password=DB_PASSWORD, db_name=DB_NAME_HACKERNEWS
    )
    text_inserter = TextInserter(conn, TABLE_NAME_TEXTS, PRIMARY_KEY_NAME_TEXTS)
    sentiment_classifier = SentimentClassifier()
    text_preprocessor = TextPreprocessor()
    is_generator_exhausted = False

    if process_text:
        while not is_generator_exhausted:
            current_batch = []
            for _ in range(batch_size):
                try:
                    current_batch.append(next(data_generator))
                except StopIteration:
                    logging.info("generator %s exhausted, finishing", data_generator)
                    is_generator_exhausted = True
                    break

            if len(current_batch) == 0:
                break

            for item_id, title, text in tqdm(current_batch):
                # Preprocess "text" field if not empty, otherwise preprocess title (stories don't have text)
                if is_string_empty(text):
                    raw_text = title
                else:
                    raw_text = text

                text_preprocessed = text_preprocessor.process(raw_text)

                text_obj = Text(item_id, text_preprocessed)

                # Insert preprocessed text
                text_inserter.insert_text(text_obj)

                # Use unprocessed text for sentiment computation
                sentiment = sentiment_classifier.get_sentiment(raw_text)
                text_inserter.insert_sentiment(sentiment, item_id)
    else:
        while True:
            current_batch = []
            for _ in range(batch_size):
                current_batch.append(next(data_generator))

            if len(current_batch) == 0:
                break

            for item_id, title, text in current_batch:
                if is_string_empty(text):
                    raw_text = title
                else:
                    raw_text = text
                sentiment = sentiment_classifier.get_sentiment(raw_text)
                text_inserter.insert_sentiment(sentiment, item_id)

    logging.info("finished task: %s", __name__)
