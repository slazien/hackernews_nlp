import logging

import luigi
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


class TaskComputeInsertSentiment(luigi.Task):
    process_text = luigi.BoolParameter()

    def run(self):
        """
        Compute and insert sentiment scores (polarity, subjectivity) into DB for all existing items
        :return:
        """
        logging.info("starting luigi task: %s", self.__class__)
        conn = DBConnection(
            user="postgres", password=DB_PASSWORD, db_name=DB_NAME_HACKERNEWS
        )
        text_inserter = TextInserter(conn, TABLE_NAME_TEXTS, PRIMARY_KEY_NAME_TEXTS)
        sentiment_classifier = SentimentClassifier()

        cursor = conn.get_cursor()
        cursor.execute("SELECT id, text, title FROM items;")
        res = cursor.fetchall()

        if self.process_text:
            text_preprocessor = TextPreprocessor()
            for item_id, text, title in tqdm(res):
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
            for item_id, text, title in tqdm(res):
                if is_string_empty(text):
                    raw_text = title
                else:
                    raw_text = text
                sentiment = sentiment_classifier.get_sentiment(raw_text)
                text_inserter.insert_sentiment(sentiment, item_id)

        logging.info("finished task: %s", self.__class__)
