from multiprocessing import cpu_count
from typing import Iterable, List

from gensim.models import KeyedVectors, Word2Vec
from gensim.models.callbacks import CallbackAny2Vec

NUM_CORES = cpu_count() - 1


class EpochLogger(CallbackAny2Vec):
    def __init__(self):
        self.epoch = 1

    def on_epoch_begin(self, model):
        print("Epoch #{} start".format(self.epoch))

    def on_epoch_end(self, model):
        print("Epoch #{} end".format(self.epoch))
        self.epoch += 1


class Word2VecTrainer:
    """
    Class for training a Word2Vec model using Gensim
    """

    def __init__(
        self,
        texts: Iterable[List[str]],
        vector_size: int,
        window: int,
        min_count: int,
        negative: int,
        workers: int = NUM_CORES,
    ):
        self.text_list = texts
        self.model = Word2Vec(
            sentences=texts,
            vector_size=vector_size,
            window=window,
            min_count=min_count,
            negative=negative,
            workers=workers,
            callbacks=[self.epoch_logger],
        )
        self.was_trained = False
        self.epoch_logger = EpochLogger()

    def train(self, epochs: int = 5) -> Word2Vec:
        """
        Train the model for a given number of epochs
        :param epochs: number of epochs to use
        :return:
        """
        self.model.build_vocab(self.text_list, progress_per=10000)
        self.model.train(
            self.text_list, total_examples=self.model.corpus_count, epochs=epochs
        )
        self.was_trained = True

        return self.model

    def save(self, path: str):
        """
        Save a trained model to disk
        :param path: path to save the model in
        :return:
        """
        if not self.was_trained:
            raise Exception("Word2Vec model not trained, cannot save")

        self.model.wv.save(path)

    def load(self, path: str) -> object:
        """
        Load words <-> embeddings mapping from disk
        :param path: path to the mapping
        :return: object containing word <-> embedding mapping
        """

        return KeyedVectors.load(path, mmap="r")
