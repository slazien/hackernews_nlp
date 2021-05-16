import argparse
import os.path

from src.models.word2vec import Word2VecTrainer
from src.utils.file import Word2VecIterator

parser = argparse.ArgumentParser()
parser.add_argument(
    "--filename-texts",
    help="Path to the file containing preprocessed texts (one item per line)",
    type=str,
)
parser.add_argument(
    "--filename-out", help="Path to the file to save the model in", type=str
)
parser.add_argument("--vector-size", help="Size of the word vectors", type=int)
parser.add_argument("--window", help="Size of the rolling window", type=int)
parser.add_argument("--min-count", help="Minimum word count to consider")
parser.add_argument(
    "--sg",
    help="Whether to use skip-gram [Y] or not, in which case CBOW will be used",
    type=str,
)
parser.add_argument("--epochs", help="Number of epochs to use for training", type=int)
parser.add_argument("--workers", help="Number of cores to use for training", type=int)
parser.add_argument(
    "--negative", help="Number of noise words to use for negative sampling", type=int
)
args = parser.parse_args()


def main():
    if not os.path.isfile(args.filename_texts):
        raise FileExistsError("Specified file does not exist")

    iterator = Word2VecIterator(args.filename_texts)
    trainer = Word2VecTrainer(
        texts=iterator,
        epochs=args.epochs,
        vector_size=args.vector_size,
        window=args.window,
        min_count=args.min_count,
        negative=args.negative,
        sg=1 if args.sg.lower() == "y" else 0,
        workers=args.workers,
    )

    trainer.save(args.filename_out)


if __name__ == "__main__":
    main()
