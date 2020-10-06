"""Stores project constants and utility functions."""
import os
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Union

import mongoengine
import pymongo
import pytz
import spacy
from dotenv import load_dotenv
from mongoengine import connect
from praw import Reddit
from praw.models import Comment, Submission
from psaw import PushshiftAPI
from spacy.lang.en import English

from src.schema import CommentPost, Post, SubmissionPost, User

# --- Utility Constants ---
# project constants
PROJ_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)))
SUBR_NAMES = ["opiates", "heroin"]
SUB_LIMIT = 1000

# load local environment variables
load_dotenv(os.path.join(PROJ_DIR, "..", ".env"))

# --- Utility Functions ---


def get_mongo() -> pymongo.MongoClient:
    """Allows for lazy connection to Mongo."""
    return pymongo.MongoClient(
        os.getenv("HOST"),
        int(str(os.getenv("PORT"))),
        username=os.getenv("MUSERNAME"),
        password=os.getenv("MPASSWORD"),
        authSource=os.getenv("DB_NAME"),
    )


def connect_to_mongo():
    """Allows for lazy connection to Mongo."""
    connect(
        host=os.getenv("HOST"),
        port=int(str(os.getenv("PORT"))),
        username=os.getenv("MUSERNAME"),
        password=os.getenv("MPASSWORD"),
        db=os.getenv("DB_NAME"),
    )


def get_praw() -> Reddit:
    """Allows for lazy connection to Praw."""
    return Reddit(
        client_id=os.getenv("RCLIENT_ID"),
        client_secret=os.getenv("RSECRET_KEY"),
        password=os.getenv("RPASSWORD"),
        username=os.getenv("RUSERNAME"),
        user_agent=os.getenv("RUSER_AGENT"),
    )


def get_psaw(praw: Reddit) -> PushshiftAPI:
    """Allows for lazy connection to Psaw."""
    return PushshiftAPI(praw)

def get_nlp() -> English:
    """Allows lazy access of nlp module."""
    return spacy.load("en_core_web_sm")


def utc_to_dt(utc: float) -> datetime:
    """Convert a unix time to a python datetime."""
    return datetime.utcfromtimestamp(int(utc))


def dt_to_utc(dt_: Optional[datetime]) -> Optional[datetime]:
    """Converts a standard datetime representation to UTC."""
    return None if not dt_ else dt_.astimezone(pytz.UTC)


def last_date(subreddit: Optional[str] = None) -> datetime:
    """Gets the newest date from the mongo collection."""
    base_query = Post.objects(subreddit=subreddit) if subreddit else Post.objects
    return base_query.order_by("-datetime").first().datetime


def posts_to_mongo(posts: List[Post]) -> None:
    """Store the given posts in mongo."""
    n_posted = 0
    for post in posts:
        try:
            post.save()
            n_posted += 1
        except (mongoengine.errors.ValidationError or mongoengine.errors.NotUniqueError) as e:
            print(f"Error adding post {post.pid}: {e}")
    print(f"{n_posted} posts added to mongo.")


def user_from_username(username: Optional[str]) -> Optional[User]:
    """Return the user if it exists, else create it and return."""
    user = None
    if isinstance(username, str):
        query = User.objects(username=username)
        if query.count() == 0:
            user = User(username=username)
            user.save()
        else:
            user = query.first()

    return user


def sub_comm_to_post(sub_comm: Union[Submission, Comment], is_sub: bool) -> Post:
    """Convert a Praw Submission or Comment to a Post object."""
    # convert username to user
    username = None if not sub_comm.author else sub_comm.author.name
    user = user_from_username(username)

    # store attributes common to both submission and comments
    kwargs = {
        "pid": sub_comm.id,
        "user": user,
        "datetime": utc_to_dt(sub_comm.created_utc),
        "subreddit": sub_comm.subreddit.display_name,
    }

    # assign particular attributes and return the proper post type
    if is_sub:
        kwargs["url"] = sub_comm.url
        kwargs["text"] = sub_comm.selftext
        kwargs["title"] = sub_comm.title
        kwargs["num_comments"] = sub_comm.num_comments
        post = SubmissionPost
    else:
        kwargs["text"] = sub_comm.body
        kwargs["parent_id"] = sub_comm.parent_id
        post = CommentPost

    return post(**kwargs)
