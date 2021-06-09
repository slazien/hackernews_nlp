import logging
from typing import Iterable

import luigi
from tqdm import tqdm

from src.db.connection import DBConnection
from src.db.constants import (
    DB_NAME_HACKERNEWS,
    DB_PASSWORD,
    PRIMARY_KEY_NAME_TEXTS,
    TABLE_NAME_ITEMS,
    TABLE_NAME_TEXTS,
)
from src.db.inserters import TextInserter
from src.db.utils import get_column_values
from src.entities.text import Text
from src.models.sentiment_analysis import SentimentClassifier
from src.preprocessing.pipeline import TextPreprocessor
from src.utils.text import is_string_empty


def run(
    item_ids: Iterable,
    titles: Iterable,
    texts: Iterable,
    process_text: bool,
    batch_size: int = 10000,
):
    """
    :param item_ids: iterable of item IDs
    :param titles: iterable of titles
    :param texts: iterable of texts
    :param process_text: whether to process the text or not before computing sentiment and inserting into DB
    Compute and insert sentiment scores (polarity, subjectivity) into DB for all existing items
    :return:
    """
    logging.info("starting task %s", __name__)
    conn = DBConnection(
        user="postgres", password=DB_PASSWORD, db_name=DB_NAME_HACKERNEWS
    )
    text_inserter = TextInserter(conn, TABLE_NAME_TEXTS, PRIMARY_KEY_NAME_TEXTS)
    sentiment_classifier = SentimentClassifier()
    text_preprocessor = TextPreprocessor()

    if process_text:
        while True:
            current_batch = []
            for _ in range(batch_size):
                current_batch.append((next(item_ids), next(titles), next(texts)))

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
                current_batch.append((next(item_ids), next(titles), next(texts)))

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
