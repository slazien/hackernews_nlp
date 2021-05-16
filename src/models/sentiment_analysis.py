from typing import NamedTuple

from textblob import TextBlob


class SentimentClassifier:
    """
    A simple sentiment analysis class based on TextBlob
    """

    def __init__(self):
        pass

    @staticmethod
    def get_polarity(text: str) -> float:
        """
        Returns the polarity of the input text
        :param text: string containing text to analyze
        :return: a float [-1, 1] indicating the polarity of the supplied text
        """
        return TextBlob(text).polarity

    @staticmethod
    def get_subjectivity(text: str) -> float:
        """
        Returns the subjectivity of the input text
        :param text: string containing text to analyze
        :return: a float [0 ,1] indicating the subjectivity of the supplied text
        """
        return TextBlob(text).subjectivity

    @staticmethod
    def get_sentiment(text: str) -> NamedTuple:
        """
        Returns a named tuple Sentiment containing polarity and subjectivity of the input text
        :param text: string containing text to analyze
        :return: a named tuple Sentiment
        """
        return TextBlob(text).sentiment
