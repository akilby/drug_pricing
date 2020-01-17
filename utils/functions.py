"""Define utility functions for the data pipeline."""
import csv
import functools as ft
import os
from datetime import datetime
from typing import Iterable, List, Union

from praw.models import Comment, Submission, Subreddit
from praw.models.comment_forest import CommentForest
from praw.models.listing.generator import ListingGenerator

from drug_pricing import CONN, utc_to_dt

from .post import Post


def parse_comment(comm: Comment, start_time: datetime) -> List[Post]:
    """
    Convert the given comment and all replies to a list of posts.

    Helper function for 'parse_comm_forest'.

    :param comm: the comment to parse
    :param start_time: the time that a comment must have been posted after

    :returns: posts derived from the given comment and all replies
    """
    comm_time: datetime = utc_to_dt(comm.created_utc)
    if comm_time > start_time:
        return [Post(comm)] + parse_comm_forest(comm.replies, start_time)
    return []  # return empty list if the given comment was posted too early


def parse_comm_forest(root: CommentForest, start_time: datetime) -> List[Post]:
    """
    Parse all comments from the given comment forest as Post objects.

    Helper function for 'extract_subcomms'.

    :param root: a praw CommentHelper instance
    :param start_time: a datetime representing the time to start extracting comments

    :returns: a list of all comments posted after the given time
    """
    root.replace_more()  # replace any comments stored as MoreComments
    return ft.reduce(lambda acc, c: acc + parse_comment(c, start_time), root, [])


def extract_posts(subr: Subreddit, start_time: datetime, limit: int) -> List[Post]:
    """
    Extract all new submissions and comments in the subreddit after the given start time.

    :param subr: a Subreddit of a praw Reddit instance
    :param start_time: a datetime representing the time to start extracting comments
    :param limit: the maximium number of submissions to retrieve from the subreddit

    :returns: a list of all submissions/comments posted after the given time
    """
    lgen: ListingGenerator = subr.new(limit=limit)
    return ft.reduce(lambda acc, s: acc +
                     [Post(s)] +
                     parse_comm_forest(s.comments, start_time),
                     lgen, [])


def read_line(line: List[str], is_sub: bool) -> Post:
    """
    Convert a line from a csv file to a Post object.

    :param line: a line from a csv file
    :param is_sub: if the given line is data for a submission (T) or comment (F)

    :returns: a Post object derived from the data on the line
    """
    # select the proper id extractor method from the connection
    sb_getter = CONN.submission if is_sub else CONN.comment

    # extract the submission/comment using the id from the given line
    # NOTE: assumes that the id is the first item on the line
    subcomm: Union[Submission, Comment] = sb_getter(str(line[0]))

    # return a post object defined by the extracted submission/comment
    return Post(subcomm)


def read_file(reader: Iterable[List[str]], is_sub: bool) -> List[Post]:
    """
    Convert each line of a given file to a Post object.

    :param reader: a csv file reader object
    :param is_sub: if the given file is data for a submission (T) or comment (F)

    :returns: a list of Post objects derived from the data in the file
    """
    return [read_line(row, is_sub) for row in reader]


def read_all_files(root: str, is_sub: bool) -> List[Post]:
    """
    Read all files from the given directory and parse all lines into Post objects.

    :param root: a filepath for the directory containing desired data
    :param is_sub: if the given files contain submission (T) or comment (F) data

    :returns: a list of Post objects derived from the files
    """
    # read all files in the given directory into a list of csv reader objects
    readers: List[csv.reader] = []
    for filename in os.listdir(root):
        fobj = open(os.path.join(root, filename))
        readers.append(csv.reader(fobj))
        fobj.close()

    # combine post objects extracted from each file to single list and return
    return ft.reduce(lambda acc, f: acc + read_file(f, is_sub), readers, [])
