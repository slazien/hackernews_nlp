from typing import NamedTuple, Optional

from textblob import TextBlob

from src.utils.text import is_string_empty


class SentimentClassifier:
    """
    A simple sentiment analysis class based on TextBlob
    """

    def __init__(self):
        pass

    @staticmethod
    def get_polarity(text: str) -> Optional[float]:
        """
        Returns the polarity of the input text
        :param text: string containing text to analyze
        :return: a float [-1, 1] indicating the polarity of the supplied text if nonempty, else None
        """
        if is_string_empty(text):
            return None
        else:
            return TextBlob(text).polarity

    @staticmethod
    def get_subjectivity(text: str) -> Optional[float]:
        """
        Returns the subjectivity of the input text
        :param text: string containing text to analyze
        :return: a float [0 ,1] indicating the subjectivity of the supplied text if nonempty else None
        """
        if is_string_empty(text):
            return None
        else:
            return TextBlob(text).subjectivity

    @staticmethod
    def get_sentiment(text: str) -> Optional[NamedTuple]:
        """
        Returns a named tuple Sentiment containing polarity and subjectivity of the input text
        :param text: string containing text to analyze
        :return: if input text non-empty, a named tuple Sentiment, None otherwise
        """
        if is_string_empty(text):
            return None
        else:
            return TextBlob(text).sentiment
