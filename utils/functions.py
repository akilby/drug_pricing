"""Define utility functions for the data pipeline."""
import csv
import functools as ft
import os
from datetime import datetime
from typing import List, Union

from praw.models import Comment, Submission, Subreddit
from praw.models.comment_forest import CommentForest
from praw.models.listing.generator import ListingGenerator

from constants import CONN, utc_to_dt

from .post import Post


def parse_comment(comm: Comment, start_time: datetime) -> List[Post]:
    """Convert the given comment and all replies to a list of posts."""
    # print current state to command line
    print(
        f"\tParsing comment {comm.id} from submission {comm.submission.id} .....")

    # convert the time format in Praw (utc) to a datetime object
    comm_time: datetime = utc_to_dt(comm.created_utc)

    # if the comment was posted after the requested start time, parse it and subcomments
    if comm_time > start_time:
        return [Post(comm)] + parse_comm_forest(comm.replies, start_time)

    # return empty list if the given comment was posted too early
    return []


def parse_comm_forest(root: CommentForest, start_time: datetime) -> List[Post]:
    """Parse all comments from the given comment forest as Post objects."""
    root.replace_more()  # replace any comments stored as a MoreComments object
    return ft.reduce(lambda acc, c: acc + parse_comment(c, start_time), root, [])


def extract_praw(subr: Subreddit, start_time: datetime, limit: int) -> List[Post]:
    """
    Extract all new submissions and comments in the subreddit after the given start time.

    :param subr: a Subreddit of a praw Reddit instance
    :param start_time: a datetime representing the time to start extracting comments
    :param limit: the maximium number of submissions to retrieve from the subreddit

    :returns: a list of all submissions/comments posted after the given time
    """
    # retrieve submissions from Reddit via Praw
    lgen: ListingGenerator = subr.new(limit=limit)

    # parse each submission and all of the comments per submission
    return ft.reduce(lambda acc, s: acc +
                     [Post(s)] +
                     parse_comm_forest(s.comments, start_time),
                     lgen, [])


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
