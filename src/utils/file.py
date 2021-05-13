from src.preprocessing.text import tokenize


class Word2VecIterator:
    def __init__(self, file_name: str):
        self.file_name = file_name
        self.read_count = 0

    def __iter__(self):
        for line in open(self.file_name, "r"):
            self.read_count += 1

            yield tokenize(line)
