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
    stripper = HTMLStripper()
    stripper.feed(text)
    return stripper.get_data()


def tokenize(text: str) -> List:
    return [token.text for token in TOKENIZER(text)]


def remove_stopwords(text: str) -> str:
    return " ".join([token for token in tokenize(text) if token not in STOPWORDS_SET])


def lowercase(text: str) -> str:
    return text.lower()


def remove_nonalphanumeric(text: str) -> str:
    return sub("[^a-zA-Z0-9 ]+", "", text)


def transform_accented_chars(text):
    return unidecode(text)
