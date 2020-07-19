"""Allows command line execution of programs."""
import argparse
import os
from typing import List

import pandas as pd
import spacy
from mongoengine import disconnect

from src.tasks.praw import read_praw, extract_praw
from src.tasks.csv import read_csv
from src.tasks.histories import get_users_histories
from src.tasks.spacy import add_spacy_to_mongo
from src.utils import (
    PROJ_DIR,
    SUBR_NAMES,
    connect_to_mongo,
    get_praw,
    get_psaw,
    last_date,
)


def gen_args() -> argparse.ArgumentParser:
    """Generate an argument parser."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--subr", help="The subreddit to use.", type=str)
    parser.add_argument("--startdate", help="The end date for Praw scraping", type=str)
    parser.add_argument("--enddate", help="The end date for Praw scraping", type=str)
    parser.add_argument("--limit", help="The number of Praw objects to limit querying", type=int)
    parser.add_argument("--csv", help="The csv filepath to parse from", type=str)
    parser.add_argument(
        "--posttype", help="If data to parse is" + "submissions (s) or comments (c)"
    )
    parser.add_argument(
        "--lastdate",
        help="Retrieve the last date stored in the mongo collection.",
        action="store_true",
    )
    parser.add_argument(
        "--update",
        help="Insert all posts for all subreddits from the last posted date",
        action="store_true",
    )
    parser.add_argument(
        "--histories", help="Retrieve full posting history for all users.", action="store_true"
    )
    parser.add_argument("--spacy", help="Run spacy on all new documents.", action="store_true")
    return parser


def main() -> None:
    """Execute programs from the command line."""
    # retrieve args
    args = gen_args().parse_args()

    # initialize data
    praw = get_praw()
    psaw = get_psaw(praw)
    nlp = spacy.load("en_core_web_sm")

    # retrieve the last date stored in mongo
    if args.lastdate and args.subr:
        print("Getting last date .....")
        date = last_date(args.subr)
        print("Most recent date in the database:", date)
        return

    # retrieve data from praw if valid fields given
    if args.subr:
        print("Retrieving limited posts from Reddit .....")
        data = read_praw(psaw, args.subr, args.startdate, args.enddate, args.limit)

    # retrieve data from csv if valid fields given
    if args.csv:
        print("Retrieving data from csv .....")
        data += read_csv(args.csv, args.posttype)

    # add all recent posts to database
    if args.update:
        print("Retrieving all new posts from Reddit .....")
        for subr_name in SUBR_NAMES:
            start_date = last_date(subr_name)
            posts = extract_praw(psaw, subr_name, start_date)
            for p in posts:
                p.save()
            print(f"{len(posts)} posts inserted")

    if args.histories:
        print("Retrieving user histories .....")
        users_fp = os.path.join(PROJ_DIR, "data", "users.csv")
        users = pd.read_csv(users_fp, squeeze=True, header=None).tolist()
        get_users_histories(users, psaw, users_fp)

    # add spacy to docs without spacy
    if args.spacy:
        print("Updating documents with spacy .....")
        add_spacy_to_mongo(nlp)
        return

    disconnect()
    print("Program completed.")


if __name__ == "__main__":
    main()
