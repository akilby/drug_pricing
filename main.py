"""Allows command line execution of programs."""
import argparse
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Union

from praw.models import Comment, Submission

from constants import (COLL, COMM_COLNAMES, CONN, OUT_JSON, SUB_COLNAMES,
                       SUB_LIMIT, SUBR)
from utils.functions import extract_csv, extract_files, extract_praw
from utils.post import Post


def gen_args(sub_labels: List[str],
             comm_labels: List[str],
             mongo_labels: List[str],
             json_labels: List[str]) -> argparse.ArgumentParser:
    """Generate an argument parser."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--files",
                        help="Enter the directory that should be read from.",
                        type=str)
    parser.add_argument("--posttype",
                        help=f"Enter if data to parse is submissions {sub_labels} or comments {comm_labels}",
                        type=str)
    parser.add_argument("--enddate",
                        help="Enter the end date for Praw scraping",
                        type=str)
    parser.add_argument("--praw",
                        help="Enter the date to scrape Praw from in YYYY-MM-DD format.",
                        type=str)
    parser.add_argument("--limit",
                        help="Enter the number of submissions to parse from praw.",
                        type=int,
                        default=SUB_LIMIT)
    parser.add_argument("--output",
                        help=f"Select whether to output data to mongo {mongo_labels} or a json file {json_labels}. Default is mongo.",
                        type=str,
                        default="m")
    parser.add_argument("--outpath",
                        help="Override the file location for output json.  Default is stored in constants.py.",
                        default=OUT_JSON)
    parser.add_argument("--csv",
                        help="Enter the csv filepath to parse from",
                        type=str)
    return parser


def read_files(path: str,
               posttype: str,
               sub_labels: List[str],
               comm_labels: List[str]) -> List[Post]:
    """Read data from the given filepath and using the given post type."""
    # if a valid label post type label is given, parse files for that type
    if posttype.lower() in sub_labels + comm_labels:
        is_sub: bool = (posttype.lower() in sub_labels)
        print("Reading Posts from files.....")
        file_data: List[Post] = extract_files(path, is_sub)
        print(f"{len(file_data)} posts from files retrieved.")
        return file_data

    # raise an exception if an invalid post type is given
    raise ValueError("Invalid post type provided.")


def read_praw(start_str: str, end_str: str, limit: int) -> List[Post]:
    """Read from praw starting from the given date."""
    # if date is formatted correctly, parse praw from the given date
    if re.match(r'\d{4}-\d{2}-\d{2}', start_str):
        print("Extracting posts from praw .....")
        if not end_str:
            start_date: datetime = datetime.strptime(start_str, "%Y-%m-%d")
            praw_data: List[Post] = extract_praw(SUBR, start_date, limit=limit)
        if re.match(r'\d{4}-\d{2}-\d{2}', end_str):
            end_date: datetime = datetime.strptime(end_str, "%Y-%m-%d")
            praw_data: List[Post] = extract_praw(
                SUBR, start_date, limit=limit, end_time=end_date)
        print(f"{len(praw_data)} posts from Reddit retrieved.")
        return praw_data

    # raise exception if date incorrectly formatted
    raise ValueError("Invalid date provided.")


def read_csv(filepath: str,
             posttype: str,
             sub_labels: List[str],
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


def write_data(posts: List[Post],
               output: str,
               outpath: str,
               mongo_labels: List[str],
               json_labels: List[str]) -> str:
    """Write data to mongodb or json and return the response."""
    # serialize data to json compatible
    print("Serializing data .....")
    serial_posts: List[Dict[str, Any]] = [post.to_dict() for post in posts]

    # check if mongodb should be written to
    if output in mongo_labels:
        print("Writing data to mongodb .....")
        response = COLL.insert_many(serial_posts)
        response_str: str = f"\t{len(response.inserted_ids)} documents inserted into mongo."
        return response_str

    # check if json should be written to
    if output in json_labels:
        print("Writing data to json .....")
        fobj = open(outpath, "w+")
        if len(fobj.readlines()) > 0:
            # if there is existing content in the file, append data
            data = json.load(fobj)
            data.append(serial_posts)
            json.dump(data, fobj)
        else:
            # else, just write data to file
            json.dump(serial_posts, fobj)
        fobj.close()
        response_str = f"\t{len(serial_posts)} documents inserted into json."
        return response_str

    # raise exception if neither mongo nor json requested as output
    raise ValueError("Invalid output type provided.")


def main() -> None:
    """Execute programs from the command line."""
    # initialize data
    data: List[Post] = []

    # hardcode acceptable post type labels
    sub_labels: List[str] = ["s", "sub"]
    comm_labels: List[str] = ["c", "comm"]
    mongo_labels: List[str] = ["m", "mongo"]
    json_labels: List[str] = ["j", "json"]

    # retrieve args
    args = gen_args(sub_labels,
                    comm_labels,
                    mongo_labels,
                    json_labels).parse_args()

    # retrieve data from files if valid fields given
    if args.files and args.posttype:
        data += read_files(args.files, args.posttype, sub_labels, comm_labels)

    # retrieve data from praw if valid fields given
    if args.praw:
        data += read_praw(args.praw, args.enddate, args.limit)

    # retrieve data from csv if valid fields given
    if args.csv:
        data += read_csv(args.csv, args.posttype, sub_labels, comm_labels)

    # if data exists, write it to either mongodb or a json file
    if len(data) > 0:
        response = write_data(
            data, args.output, args.outpath, mongo_labels, json_labels)
        print(f"Write response:\n\n{response}\n\nProgram completed.")
    else:
        print("Insufficient program arguments detected.")


if __name__ == "__main__":
    main()
