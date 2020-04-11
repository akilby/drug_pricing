import sys
import os
from scripts.user_histories import top_histories
from utils import COLL, PRAW, PSAW, PROJ_DIR


def main():
    """Execute desired scripts."""

    # define constants
    subdir = "data"

    if "--hist" in sys.argv:
        hist_fp = os.path.join(PROJ_DIR, subdir, "histories.csv")
        history_df = top_histories(COLL, PRAW, PSAW, 50, 50)
        history_df.to_csv(hist_fp, index=False)


if __name__ == "__main__":
    main()
