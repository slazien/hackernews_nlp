import logging
from multiprocessing import cpu_count
from typing import Iterable, List

from gensim.models import Word2Vec
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
        epochs: int,
        vector_size: int,
        window: int,
        min_count: int,
        negative: int,
        sg: int,
        workers: int = NUM_CORES,
    ):
        self.epoch_logger = EpochLogger()
        self.text_list = texts
        logging.info("training Word2Vec")
        self.model = Word2Vec(
            sentences=texts,
            epochs=epochs,
            vector_size=vector_size,
            window=window,
            min_count=min_count,
            negative=negative,
            sg=sg,
            workers=workers,
            callbacks=[self.epoch_logger],
        )
        logging.info("Word2Vec successfully trained")
        self.was_trained = False

    def train(self, epochs: int = 5) -> Word2Vec:
        """
        Train the model for a given number of epochs
        :param epochs: number of epochs to use
        :return: Word2Vec object
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

        self.model.save(path)
        logging.info("Word2Vec model saved to: {}".format(path))

    @staticmethod
    def load(path: str) -> object:
        """
        Load words <-> embeddings mapping from disk
        :param path: path to the mapping
        :return: object containing word <-> embedding mapping
        """

        logging.info("loading Word2Vec model from: {}".format(path))
        return Word2Vec.load(path)
