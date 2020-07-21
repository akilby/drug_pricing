"""Stores project constants and utility functions."""
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pytz
from dotenv import load_dotenv
from mongoengine import connect
from praw import Reddit
from psaw import PushshiftAPI

from src.schema import Post

# --- Utility Constants ---
# project constants
PROJ_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)))
SUBR_NAMES = ["opiates", "heroin"]
SUB_LIMIT = 1000

# load local environment variables
load_dotenv(os.path.join(PROJ_DIR, "..", ".env"))

# database constants
DB_NAME = os.getenv("DB_NAME")
COLL_NAME = os.getenv("COLL_NAME")
TEST_COLL_NAME = os.getenv("TEST_COLL_NAME")

# --- Utility Functions ---


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


# --- Data Structures ---

@dataclass
class Location:
    neighborhood: Optional[str] = None
    city: Optional[str] = None
    county: Optional[str] = None
    state: Optional[str] = None
