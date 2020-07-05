"""Stores project constants and utility functions."""
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, cast

import pymongo
import pytz
from dotenv import load_dotenv
from praw import Reddit
from psaw import PushshiftAPI

# --- Utility Constants ---
# project constants
PROJ_DIR = os.path.dirname(os.path.abspath(__file__))
SUBR_NAMES = ["opiates", "heroin"]
SUB_LIMIT = 1000

# hardcoded file locations on the HPC cluster
BASE_DIR = "/work/akilby/drug_pricing_project"
SUB_DIR = os.path.join(BASE_DIR, "opiates/opiates/threads")
COMM_DIR = os.path.join(BASE_DIR, "opiates/opiates/comments/complete")
OUT_JSON = os.path.join(BASE_DIR, "all_posts.json")

# hardcoded column names for legacy comment and submission csv files
SUB_COLNAMES = ["id", "url", "num_comments", "shortlink", "author", "title",
                "text", "utc"]
COMM_COLNAMES = ["id", "sub_url", "parent_id", "text", "author", "utc"]

# database constants
DB_NAME = os.getenv("DB_NAME")
COLL_NAME = os.getenv("COLL_NAME")
TEST_COLL_NAME = os.getenv("TEST_COLL_NAME")

# load local environment variables
load_dotenv(dotenv_path=os.path.join(PROJ_DIR, ".env"))

# --- Utility Functions ---


def get_mongo() -> pymongo.MongoClient:
    """Allows for lazy connection to Mongo."""
    return pymongo.MongoClient(
        os.getenv("HOST"),
        int(str(os.getenv("PORT"))),
        username=os.getenv("MUSERNAME"),
        password=os.getenv("MPASSWORD"),
        authSource=os.getenv("DB_NAME")
    )


def get_praw() -> Reddit:
    """Allows for lazy connection to Praw."""
    return Reddit(
        client_id=os.getenv("RCLIENT_ID"),
        client_secret=os.getenv("RSECRET_KEY"),
        password=os.getenv("RPASSWORD"),
        username=os.getenv("RUSERNAME"),
        user_agent=os.getenv("RUSER_AGENT")
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


def last_date(coll: pymongo.collection.Collection, subr: str) -> datetime:
    """Gets the newest date from the mongo collection."""
    res = coll.aggregate([{
        "$match": {
            "subr": subr
        }
    }, {
        "$sort": {
            "time": -1
        }
    }, {
        "$limit": 1
    }])
    time = list(res)[0]["time"]
    return time


# --- Objects ---


@dataclass
class Post():
    """An abstract representation of Submission and Comment objects."""
    pid: Optional[str] = None
    text: Optional[str] = None
    username: Optional[str] = None
    time: Optional[datetime] = None
    subr: Optional[str] = None
    utc: Optional[datetime] = dt_to_utc(time)

    def to_dict(self) -> Dict:
        """Convert the attributes of this object to a dictionary."""
        return {
            "text": self.text,
            "username": self.username,
            "time": self.utc,
            "pid": self.pid,
            "hash": hash(self),
            "subr": self.subr
        }


@dataclass
class CustomSubmission(Post):
    """A custom representation of a Submission object."""
    url: Optional[str] = None
    title: Optional[str] = None
    num_comments: Optional[int] = None

    def to_dict(self) -> Dict:
        """Convert the attributes of this object to a dictionary."""
        base_dict = super().to_dict()
        base_dict.update({
            "title": self.title,
            "num_comments": self.num_comments,
            "is_sub": True
        })
        return base_dict

    def __eq__(self, obj: object) -> bool:
        """Determine if the given object equals this object."""
        if isinstance(obj, CustomSubmission):
            submission_obj = cast(CustomSubmission, obj)
            return submission_obj.pid == self.pid and submission_obj.text == self.text
        return False

    def __ne__(self, obj: Any) -> bool:
        """Determine if the given object does not equal this object."""
        return not obj == self

    def __hash__(self) -> int:
        """Establish a hash value for this object."""
        return 10 * hash(self.text) + hash(self.pid)


@dataclass
class CustomComment(Post):
    """A custom representation a Comment object."""
    parent_id: Optional[str] = None

    def to_dict(self) -> Dict:
        """Convert the attributes of this object to a dictionary."""
        base_dict = super().to_dict()
        base_dict.update({"parent_id": self.parent_id, "is_sub": False})
        return base_dict

    def __eq__(self, obj: object) -> bool:
        """Determine if the given object equals this object."""
        if isinstance(obj, CustomComment):
            comment_obj = cast(CustomComment, obj)
            return comment_obj.pid == self.pid and comment_obj.text == self.text
        return False

    def __ne__(self, obj: Any) -> bool:
        """Determine if the given object does not equal this object."""
        return not obj == self

    def __hash__(self) -> int:
        """Establish a hash value for this object."""
        return 10 * hash(self.text) + hash(self.pid)
