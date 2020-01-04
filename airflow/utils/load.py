"""Define functions for data loading."""

from datetime import datetime
from functools import reduce
from typing import Any, Dict, List

from praw.models import Comment, Subreddit
from praw.models.comment_forest import CommentForest
from praw.models.listing.generator import ListingGenerator
from pymongo.collection import Collection

from utilities import utc_to_dt

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
    return reduce(lambda b, c: b + parse_comment(c, start_time), root, [])


def extract_subcomms(subr: Subreddit, start_time: datetime, limit: int) -> List[Post]:
    """
    Extract all new submissions and comments in the subreddit after the given start time.

    :param subr: a Subreddit of a praw Reddit instance
    :param start_time: a datetime representing the time to start extracting comments
    :param limit: the maximium number of submissions to retrieve from the subreddit

    :returns: a list of all submissions/comments posted after the given time
    """
    lgen: ListingGenerator = subr.new(limit=limit)
    return reduce(lambda b, s: b + [Post(s)] + parse_comm_forest(s.comments, start_time), lgen, [])


def add_to_mongo(coll: Collection, posts: List[Post]) -> None:
    """
    Insert the given posts to a mongo database.

    :param coll: the name of a mongo db connection
    """
    attrs: List[Dict[str, Any]] = [p.to_dict() for p in posts]
    coll.insert_many(attrs)
