"""Declare constants and define functions for data loading."""

import os
from datetime import datetime
from functools import reduce
from typing import List

from dotenv import load_dotenv
from praw import Reddit
from praw.models import Subreddit
from praw.models.listing.mixins.subreddit import CommentHelper

from .post import Post

# load local environment variables
ENV_PATH = "../.env"
load_dotenv(dotenv_path=ENV_PATH)

# establish reddit connection
CONN = Reddit(client_id=os.getenv("RCLIENT_ID"),
              client_secret=os.getenv("RSECRET_KEY"),
              password=os.getenv("RPASSWORD"),
              username=os.getenv("RUSERNAME"),
              user_agent=os.getenv("RUSER_AGENT"))

# define subreddit
SUBR = CONN.subreddit("opiates")

# define constants
SUBR_NAME = "opiates"


def utc_to_dt(utc: float) -> datetime:
    """Convert a unix time to a python datetime."""
    return datetime.utcfromtimestamp(int(utc))


def parse_comm_forest(root: CommentHelper, start_time: datetime) -> List[Post]:
    """
    Parse all comments from the given comment forest (contained in a CommentHelper instance) as Post objects.

    :param root: a praw CommentHelper instance
    :param start_time: a datetime representing the time to start extracting comments

    :returns posts: a list of all comments posted after the given time
    """
    posts: List[Post] = []
    for comment in root:
        comm_time: datetime = utc_to_dt(comment.created_utc)
        if comm_time > start_time:
            posts.append(Post(comment))
            if len(comment.replies) > 0:
                posts += parse_comm_forest(comment.replies, start_time)
    return posts


def extract_comments(subr: Subreddit, start_time: datetime) -> List[Post]:
    """
    Extract all submissions and comments in the subreddit that are after the given start time and store as Post objects.

    :param subr: a Subreddit of a praw Reddit instance
    :param start_time: a datetime representing the time to start extracting comments

    :returns comments: a list of all comments posted after the given time
    """
    posts: List[Post] = reduce(lambda base, sub: base + [Post(sub)] +
                               parse_comm_forest(sub.comments, start_time), subr, [])
    return posts
