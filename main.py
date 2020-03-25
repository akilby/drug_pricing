"""Allows command line execution of programs."""
import argparse
import os
import re
from datetime import datetime
from typing import List

from constants import COLL
from constants import COMM_COLNAMES
from constants import SUB_COLNAMES
from utils.pipeline_funcs import extract_csv
from utils.pipeline_funcs import extract_praw
from utils.pipeline_funcs import to_mongo
from utils.post import Post

def gen_args(sub_labels: List[str],
             comm_labels: List[str]) -> argparse.ArgumentParser:
    """Generate an argument parser."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--subr",
                        help="The subreddit to use.",
                        type=str)
    parser.add_argument("--startdate",
                        help="The end date for Praw scraping",
                        type=str)
    parser.add_argument("--enddate",
                        help="The end date for Praw scraping",
                        type=str)
    parser.add_argument("--limit",
                        help="Limit number of praw or spacy objects",
                        type=int)
    parser.add_argument("--csv",
                        help="The csv filepath to parse from",
                        type=str)
    parser.add_argument("--posttype",
                        help=" ".join(["If data to parse is submissions",
                                       str(sub_labels),
                                       "or comments",
                                       str(comm_labels)]),
                        type=str)
    parser.add_argument("--tospacy",
                        help="Cache documents as spacy objects")
    parser.add_argument("--fromspacy",
                        help="Read cached spacy docs")
    parser.add_argument("--topn",
                        help="Retrieve reddit history for top n users",
                        type=int)
    return parser


def read_praw(subr: str, start_str: str, end_str: str,
              limit: int) -> List[Post]:
    """Read from praw starting from the given date."""
    # if date is formatted correctly, parse praw from the given date
    if re.match(r'\d{4}-\d{2}-\d{2}', start_str):
        print("Extracting posts from praw .....")
        start_date: datetime = datetime.strptime(start_str, "%Y-%m-%d")
        if not end_str:
            praw_data: List[Post] = extract_praw(subr, start_date, limit=limit)
            print(f"{len(praw_data)} posts from Reddit retrieved.")
            return praw_data
        if re.match(r'\d{4}-\d{2}-\d{2}', end_str):
            end_date: datetime = datetime.strptime(end_str, "%Y-%m-%d")
            praw_data = extract_praw(subr, start_date, limit=limit,
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


def write_data(posts: List[Post]) -> str:
    """Write data to mongodb or json and return the response."""
    print("Writing data to mongodb .....")
    resp: str = to_mongo(COLL, posts)
    return resp


def main() -> None:
    """Execute programs from the command line."""
    # initialize data
    data: List[Post] = []

    # hardcode acceptable post type labels
    sub_labels: List[str] = ["s", "sub"]
    comm_labels: List[str] = ["c", "comm"]

    # retrieve args
    args = gen_args(sub_labels, comm_labels).parse_args()

    # retrieve data from praw if valid fields given
    if args.subr:
        data += read_praw(args.subr, args.startdate, args.enddate, args.limit)

    # retrieve data from csv if valid fields given
    if args.csv:
        data += read_csv(args.csv, args.posttype, sub_labels, comm_labels)

    # if data exists, write it to either mongodb or a json file
    if len(data) > 0:
        response = write_data(data)
        print(response)

    print("Program completed.")


if __name__ == "__main__":
    main()
