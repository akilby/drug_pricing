"""Allows command line execution of programs."""
import argparse
import os
import re
from datetime import datetime
from typing import List

from psaw import PushshiftAPI

import spacy
from src.histories import all_user_hists
from src.pipeline import extract_csv, extract_praw, to_mongo
from src.spacy import add_spacy_to_mongo
from src.utils import (COLL_NAME, COMM_COLNAMES, DB_NAME, SUB_COLNAMES,
                       SUBR_NAMES, Post, get_mongo, get_praw, get_psaw,
                       last_date)


def gen_args() -> argparse.ArgumentParser:
    """Generate an argument parser."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--subr", help="The subreddit to use.", type=str)
    parser.add_argument("--startdate",
                        help="The end date for Praw scraping",
                        type=str)
    parser.add_argument("--enddate",
                        help="The end date for Praw scraping",
                        type=str)
    parser.add_argument("--limit",
                        help="The number of Praw objects to limit querying",
                        type=int)
    parser.add_argument("--csv",
                        help="The csv filepath to parse from",
                        type=str)
    parser.add_argument("--posttype",
                        help="If data to parse is" +
                        "submissions (s) or comments (c)")
    parser.add_argument("--lastdate",
                        help="Retrieve the last date stored in the mongo\
                                collection.",
                        action="store_true")
    parser.add_argument("--update",
                        help="Insert all posts for all subreddits from the\
                                last posted date",
                        action="store_true")
    parser.add_argument("--histories",
                        help="Retrieve full posting history for all users.",
                        action="store_true")
    parser.add_argument("--spacy",
                        help="Run spacy on all new documents.",
                        action="store_true")
    return parser


def read_praw(psaw: PushshiftAPI, subr: str, start_str: str, end_str: str,
              limit: int) -> List[Post]:
    """Read from praw starting from the given date."""
    # if date is formatted correctly, parse praw from the given date
    if re.match(r'\d{4}-\d{2}-\d{2}', start_str):
        print("Extracting posts from praw .....")
        start_date: datetime = datetime.strptime(start_str, "%Y-%m-%d")
        if not end_str:
            praw_data: List[Post] = extract_praw(psaw, subr, start_date, limit=limit)
            print(f"{len(praw_data)} posts from Reddit retrieved.")
            return praw_data
        if re.match(r'\d{4}-\d{2}-\d{2}', end_str):
            end_date: datetime = datetime.strptime(end_str, "%Y-%m-%d")
            praw_data = extract_praw(psaw,
                                     subr,
                                     start_date,
                                     limit=limit,
                                     end_time=end_date)
        print(f"{len(praw_data)} posts from Reddit retrieved.")
        return praw_data

    # raise exception if date incorrectly formatted
    raise ValueError("Invalid date provided.")


def read_csv(filepath: str, posttype: str, sub_labels: List[str],
             comm_labels: List[str]) -> List[Post]:
    """Read data from the given csv file using the given post type."""
    # check if the given post type is valid
    if posttype.lower() in sub_labels + comm_labels:
        # check if the given file exists
        if os.path.isfile(filepath):
            is_sub: bool = (posttype.lower() in sub_labels)
            colnames: List[str] = SUB_COLNAMES if is_sub else COMM_COLNAMES
            print("Reading Posts from csv file .....")
            file_data: List[Post] = extract_csv(filepath, colnames)
            print(f"{len(file_data)} posts from files retrieved.")
            return file_data

        # rase exception if file doesn't exist
        raise ValueError("The given file does not exist.")

    # raise an exception if an invalid post type is given
    raise ValueError("Invalid post type provided.")


def main() -> None:
    """Execute programs from the command line."""
    # initialize data
    data: List[Post] = []
    collection = get_mongo()[DB_NAME][COLL_NAME]
    praw = get_praw()
    psaw = get_psaw(praw)
    nlp = spacy.load("en_core_web_sm")

    # hardcode acceptable post type labels
    sub_labels: List[str] = ["s", "sub"]
    comm_labels: List[str] = ["c", "comm"]

    # retrieve args
    args = gen_args().parse_args()

    # retrieve the last date stored in mongo
    if args.lastdate and args.subr:
        print("Getting last date .....")
        date = last_date(collection, args.subr)
        print(date)

    # retrieve data from praw if valid fields given
    if args.subr:
        print("Retrieving limited posts from Reddit .....")
        data += read_praw(psaw, args.subr, args.startdate, args.enddate, args.limit)

    # retrieve data from csv if valid fields given
    if args.csv:
        print("Retrieving data from csv .....")
        data += read_csv(args.csv, args.posttype, sub_labels, comm_labels)

    # add all recent posts to database
    if args.update:
        print("Retrieving all new posts from Reddit .....")
        for subr_name in SUBR_NAMES:
            start_date = last_date(collection, subr_name)
            data += extract_praw(psaw, subr_name, start_date)

    if args.histories:
        print("Retrieving user histories .....")
        data += all_user_hists(praw, psaw, collection)

    # add spacy to docs without spacy
    if args.spacy:
        print("Updating documents with spacy .....")
        add_spacy_to_mongo(collection, nlp)

    # if data exists, write it to mongo
    resp = to_mongo(collection, data)
    print(resp)

    print("Program completed.")


if __name__ == "__main__":
    main()
