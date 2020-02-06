"""Define utility functions for the data pipeline."""
import csv
import functools as ft
import os
from datetime import datetime
from typing import List, Optional, Union

import pandas as pd
import progressbar
from praw.models import Comment, Submission, Subreddit
from praw.models.comment_forest import CommentForest
from praw.models.listing.generator import ListingGenerator

from constants import CONN, utc_to_dt

from .post import Post


def parse_comm_forest(root: CommentForest,
                      start_time: datetime,
                      end_time: datetime) -> List[Post]:
    """Parse all comments from the given comment forest as Post objects."""
    root.replace_more()  # replace any comments stored as a MoreComments object
    return ft.reduce(lambda acc, c: acc + parse_sc(c, False, start_time, end_time), root, [])


def parse_sc(sc: Union[Submission, Comment],
             is_sub: bool,
             start_time: datetime,
             end_time: datetime) -> List[Post]:
    """Parse either a submission or a comment."""
    # convert the time format in Praw (utc) to a datetime object
    sc_time: datetime = utc_to_dt(sc.created_utc)

    # if the given submission is in range, parse all of its comments
    if start_time < sc_time < end_time:
        post: Post = Post(sc)
        new_comms: CommentForest = sc.comments if is_sub else sc.replies
        return [post] + parse_comm_forest(new_comms, start_time, end_time)

    # return an empty list if the given submission/comment is out of the time range
    return []


def extract_praw(subr: Subreddit,
                 start_time: datetime,
                 limit: int,
                 end_time: datetime = datetime.utcnow()) -> List[Post]:
    """
    Extract all new submissions and comments in the subreddit after the given start time.

    :param subr: a Subreddit of a praw Reddit instance
    :param start_time: a datetime representing the time to start extracting comments
    :param limit: the maximium number of submissions to retrieve from the subreddit
    :param end_time: a datetime representing the time to end extracting comments

    :returns: a list of all submissions/comments posted between the given time range
    """
    def filter_by_time(losc: List[Union[Submission, Comment]]) -> List[Union[Submission, Comment]]:
        return [sc for sc in losc
                if start_time < utc_to_dt(sc.created_utc) < end_time]

    # retrieve submissions from Reddit via Praw
    lgen: ListingGenerator = subr.new(limit=limit)

    # filter submissions by the given time frame
    subs: List[Submission] = filter_by_time(lgen)

    # retrieve all comments from each submission and filter by time frame
    comms: List[Comment] = filter_by_time(ft.reduce(
        lambda s, acc: acc + s.comments.list(), subs, []))

    # combine submission and comments lists and convert all to post objects
    posts: List[Post] = [Post(sc) for sc in subs + comms]
    return posts


def read_line(line: List[str], is_sub: bool) -> Post:
    """Convert a line from a csv file to a Post object."""
    # select the proper id extractor method from the connection
    sb_getter = CONN.submission if is_sub else CONN.comment

    # extract the submission/comment using the id from the given line
    # NOTE: assumes that the id is the first item on the line
    subcomm: Union[Submission, Comment] = sb_getter(str(line[0]))

    # return a post object defined by the extracted submission/comment
    return Post(subcomm)


def read_file(path: str, is_sub: bool) -> List[Post]:
    """Convert each line of a given file to a Post object."""
    # get the filetype of the file for the given path
    filename, filetype = os.path.splitext(path)

    # if it is a csv file, parse data from it
    if filetype == ".csv":
        print(f"\tReading file: {filename} .....")
        fobj = open(path)
        reader = csv.reader(fobj)
        posts = [read_line(row, is_sub) for row in reader]
        fobj.close()
        return posts

    # otherwise, do not read and return an empty list
    return []


def extract_files(root: str, is_sub: bool) -> List[Post]:
    """
    Read all files from the given directory and parse all lines into Post objects.

    Note: only parses files from the first layer of the directory

    :param root: a filepath for the directory containing desired data
    :param is_sub: if the given files contain submission (T) or comment (F) data

    :returns: a list of Post objects derived from the files
    """
    # generate a list of full filepaths to read from
    paths = [os.path.join(root, fp) for fp in os.listdir(root)]

    # combine post objects extracted from each file to single list and return
    return ft.reduce(lambda acc, fp: acc + read_file(fp, is_sub), paths, [])


def extract_csv(filepath: str,
                colnames: List[str]) -> List[Post]:
    """
    Read all submissions or comments from the given csv file.

    :param filepath: the csv filepath
    :param colnames: the column names associated with the csv file

    :returns: a list of Post objects derived from the file
    """
    def add_row(row: pd.Series, is_sub: bool) -> Post:
        par_id: Optional[str] = None if is_sub else row["parent_id"]
        return Post(row["id"],
                    row["text"],
                    row["author"],
                    utc_to_dt(row["utc"]),
                    is_sub,
                    par_id)

    # parse the given file if it is a csv
    if os.path.splitext(filepath)[1] == ".csv":
        # open the file as a csv object
        df: pd.DataFrame = pd.read_csv(filepath, header=None)
        df.columns = colnames

        return [add_row(row, "parent_id" not in df.columns)
                for _, row in df.iterrows()]

    # else, return empty list
    return []
