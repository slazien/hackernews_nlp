from html.parser import HTMLParser
from io import StringIO
from re import sub
from typing import List

from spacy import load
from spacy.lang.en import English
from unidecode import unidecode

NLP = English()
TOKENIZER = NLP.tokenizer

LM = load("en_core_web_sm")
STOPWORDS_SET = LM.Defaults.stop_words


class HTMLStripper(HTMLParser):
    """
    Class for removing HTML tags from the supplied string
    """

    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs = True
        self.text = StringIO()

    def handle_data(self, data):
        self.text.write(data)

    def get_data(self):
        return self.text.getvalue()


def strip_html(text: str) -> str:
    """
    Remove HTML tags from supplied string
    :param text: string to process
    :return: processed string
    """
    stripper = HTMLStripper()
    stripper.feed(text)
    return stripper.get_data()


def tokenize(text: str) -> List:
    """
    Tokenize an input string
    :param text: string to tokenize
    :return: tokenized string
    """
    return [token.text for token in TOKENIZER(text)]


def remove_stopwords(text: str) -> str:
    """
    Remove stopwords from input string
    :param text: input string
    :return: string with removed stopwords
    """
    return " ".join([token for token in tokenize(text) if token not in STOPWORDS_SET])


def lowercase(text: str) -> str:
    """
    Convert all characters in the input string to lowercase
    :param text: input string
    :return: string with all lowercase characters
    """
    return text.lower()


def remove_nonalphanumeric(text: str) -> str:
    """
    Remove non-alphanumeric characters (excluding spaces) from the input string
    :param text: input string
    :return: string with all non-alphanumeric characters removed
    """
    return sub("[^a-zA-Z0-9 ]+", "", text)


def transform_accented_chars(text):
    """
    Transform accented characters to their non-accented counterparts
    :param text: input string
    :return: string with accented characters transformed to non-accented versions
    """
    return unidecode(text)
