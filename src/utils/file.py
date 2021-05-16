from tqdm import tqdm

from src.preprocessing.text import tokenize


class Word2VecIterator:
    """
    Class for iteration over files. This is to circumvent the limitation of generators which can be exhausted.
    Since Word2Vec needs several passed over the data, a generator would not enable correct training.
    """

    def __init__(self, file_name: str):
        self.file_name = file_name
        self.read_count = 0

    def __iter__(self):
        for line in tqdm(open(self.file_name, "r")):
            self.read_count += 1
            if self.read_count % 1000000 == 0:
                print("Lines read: {}".format(self.read_count))

            yield tokenize(line)
