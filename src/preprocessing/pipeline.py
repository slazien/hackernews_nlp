import re
from multiprocessing import Pool, cpu_count
from typing import Iterable, Optional

from tqdm import tqdm

from src.preprocessing.text import *
from src.utils.text import is_string_empty

NUM_CORES = cpu_count() - 1 or 1


class TextPreprocessor:
    def __init__(self, to_lowercase: bool = True):
        self.to_lowercase = to_lowercase

    def process(self, text: str) -> Optional[str]:
        """
        Process a single string by applying cleaning methods defined in text.py
        :param text: string to process
        :return: processed string, None if supplied or output string is None or if returned text would be empty
        """
        if is_string_empty(text):
            return None

        text = strip_html(text)
        text = remove_stopwords(text)
        text = transform_accented_chars(text)
        if self.to_lowercase:
            text = lowercase(text)
        text = remove_nonalphanumeric(text)
        text = re.sub(" +", " ", text)
        text = text.strip()

        if is_string_empty(text):
            return None

        return text

    def process_multiprocessing(
        self,
        texts: Iterable[str],
        file_name: str,
        total_count: int,
        batch_size: int = 100000,
    ):
        """
        Parallelize the process method using n - 1 CPU cores and save results in batches to a file
        :param texts: an iterable with string objects
        :param file_name: path to the file to append preprocessed texts to
        :param total_count: total number of records to iterate over
        :param batch_size: the size of a single text list to use for parallel preprocessing
        :return:
        """

        num_processed = 0

        with tqdm(total=total_count) as pbar:
            with open(file_name, "a") as f:
                with Pool(NUM_CORES) as pool:
                    while True:
                        # Create a batch of texts as a list for use in pool.imap
                        current_batch = []
                        for i in range(batch_size):
                            current_batch.append(next(texts))

                        # Break the loop if no more texts to process
                        if len(current_batch) == 0:
                            break

                        # Process texts in parallel
                        text_processed_batch = list(
                            tqdm(
                                pool.imap(self.process, current_batch),
                                total=total_count,
                            )
                        )

                        # Save processed texts to a file, with each text in a new line
                        for text in text_processed_batch:
                            if not is_string_empty(text):
                                f.write(text + "\n")

                            num_processed += 1
                            pbar.update(num_processed)
