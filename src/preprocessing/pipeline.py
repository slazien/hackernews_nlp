from multiprocessing import Pool, cpu_count
from typing import Optional

from tqdm import tqdm

from src.preprocessing.text import *

NUM_CORES = cpu_count() - 1


class TextPreprocessor:
    def __init__(self, text_list: List[str], to_lowercase: bool = True):
        self.text_list = text_list
        self.to_lowercase = to_lowercase

    def process(self, text: str) -> Optional[str]:
        """
        Process a single string by applying cleaning methods defined in text.py
        :param text: string to process
        :return: processed string, None if supplied string was None
        """
        if text is None:
            return None
        text = strip_html(text)
        text = remove_stopwords(text)
        text = transform_accented_chars(text)
        if self.to_lowercase:
            text = lowercase(text)
        text = remove_nonalphanumeric(text)

        return text

    def process_multiprocessing(self) -> List[Optional[str]]:
        """
        Parallelize the process method using n - 1 CPU cores
        :return: list of processed strings
        """
        with Pool(NUM_CORES) as pool:
            text_processed = list(
                tqdm(pool.imap(self.process, self.text_list), total=len(self.text_list))
            )
        return text_processed
