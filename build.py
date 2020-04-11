import sys
import os
from scripts.user_histories import top_histories, to_spacy, spacy_to_disk
from utils import COLL, PRAW, PSAW, PROJ_DIR
import pandas as pd


def main():
    """Execute desired scripts."""

    # define constants
    subdir = "data"
    hist_fp = os.path.join(PROJ_DIR, subdir, "histories.csv")
    spacy_fp = os.path.join(PROJ_DIR, subdir, "histories.spacy")
    spacy_idx_fp = os.path.join(PROJ_DIR, subdir, "spacy_idx.csv")

    if "--hist" in sys.argv:
        history_df = top_histories(COLL, PRAW, PSAW, 50, 50)
        history_df.to_csv(hist_fp, index=False)

    if "--spacy" in sys.argv:
        hist_df = pd.read_csv(hist_fp)
        text = hist_df["text"].tolist()
        spacy_idx, spacy_docs = to_spacy(text)
        spacy_to_disk(spacy_docs, spacy_fp)
        pd.Series(spacy_idx).to_csv(spacy_idx_fp, header=False, index=False)


if __name__ == "__main__":
    main()
