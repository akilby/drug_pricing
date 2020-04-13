"""Define utility functions for the data pipeline."""
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import pymongo
from praw.models import Comment, Submission
from pymongo.collection import Collection

from utils import PSAW, dt_to_utc, utc_to_dt


@dataclass
class Post():
    """An abstract representation of Submission and Comment objects."""

    def __init__(self, pid: Optional[str] = None,
                 text: Optional[str] = None,
                 username: Optional[str] = None,
                 time: Optional[datetime] = None,
                 subr: Optional[str] = None) -> None:
        """Initialize attributes of the Post."""
        self.pid = pid
        self.text = text
        self.username = username
        self.time = time
        self.subr = subr
        self.utc = dt_to_utc(self.time)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the attributes of this object to a dictionary."""
        return {"text": self.text,
                "username": self.username,
                "time": self.utc,
                "pid": self.pid,
                "hash": hash(self),
                "subr": self.subr}

    def __eq__(self, obj: object) -> bool:
        """Determine if the given object equals this object."""
        return isinstance(obj, self) and (obj.pid == self.pid) and \
            (obj.text == self.text)

    def __ne__(self, obj: Any) -> bool:
        """Determine if the given object does not equal this object."""
        return not obj == self

    def __hash__(self) -> int:
        """Establish a hash value for this object."""
        return 10 * hash(self.text) + hash(self.pid)


@dataclass(eq=False)
class Sub(Post):
    """Represents a Submission Object."""

    def __init__(self, pid: Optional[str] = None,
                 text: Optional[str] = None,
                 username: Optional[str] = None,
                 time: Optional[datetime] = None,
                 url: Optional[str] = None,
                 title: Optional[str] = None,
                 num_comments: Optional[int] = None,
                 subr: Optional[str] = None) -> None:
        """Initialize attributes of the Submission."""
        super().__init__(pid=pid, text=text, username=username, time=time,
                         subr=subr)
        self.url = url
        self.title = title
        self.num_comments = num_comments

    def to_dict(self) -> Dict[str, Any]:
        """Convert the attributes of this object to a dictionary."""
        base_dict = super().to_dict()
        base_dict.update({"title": self.title,
                          "num_comments": self.num_comments,
                          "is_sub": True})
        return base_dict


@dataclass(eq=False)
class Comm(Post):
    """Represents a Comment object."""

    def __init__(self, pid: Optional[str] = None,
                 text: Optional[str] = None,
                 username: Optional[str] = None,
                 time: Optional[datetime] = None,
                 parent_id: Optional[str] = None,
                 subr: Optional[str] = None) -> None:
        """Initialize attributes of the Comment."""
        super().__init__(pid=pid, text=text, username=username, time=time,
                         subr=subr)
        self.parent_id = parent_id

    def to_dict(self) -> Dict[str, Any]:
        """Convert the attributes of this object to a dictionary."""
        base_dict = super().to_dict()
        base_dict.update({"parent_id": self.parent_id,
                          "is_sub": False})
        return base_dict



def sc_to_post(sc: Union[Submission, Comment], is_sub: bool,
               subr: str) -> Post:
    """Convert a Praw Submission or Comment to a Post object."""
    # generic attributes
    pid = sc.id
    username = None if not sc.author else sc.author.name
    time = utc_to_dt(sc.created_utc)

    # submission attrs
    if is_sub:
        url = sc.url
        text = sc.selftext
        title = sc.title
        num_comments = sc.num_comments
        return Sub(pid=pid, username=username, time=time, text=text,
                   url=url, title=title, num_comments=num_comments,
                   subr=subr)

    # comment attrs
    text = sc.body
    parent_id = sc.parent_id
    return Comm(pid=pid, username=username, time=time, text=text,
                parent_id=parent_id, subr=subr)


def extract_praw(subr: str, start_time: datetime, limit: Optional[int] = None,
                 end_time: Optional[datetime] = None) -> List[Post]:
    """
    Extract all submissions/comments from reddit in the given time frame.

    :param subr: the name of a subreddit
    :param start_time: the time to start extracting comments
    :param limit: the max number of comments to obtain. 'None' if no limit.
    :param end_time: the time to end extracting comments

    :returns: a list of Post objects
    """
    # convert datetimes to ints for PSAW
    start_int = int(start_time.timestamp())
    end_int = int(end_time.timestamp()) if end_time else None

    # retrieve all submissions and comments
    subs = list(PSAW.search_submissions(after=start_int, subreddit=subr,
                                        limit=limit, before=end_int))
    comms = list(PSAW.search_comments(after=start_int, subreddit=subr,
                                      limit=limit, before=end_int))

    # convert Submission/Comment object to Sub/Comm objects
    sub_objs: List[Post] = [sc_to_post(s, True, subr) for s in subs]
    comm_objs: List[Post] = [sc_to_post(c, False, subr) for c in comms]

    # return list of combined post objects
    return sub_objs + comm_objs


def row_to_post(row: pd.Series, is_sub: bool) -> Post:
    """Convert a dataframe row to a post object."""
    # generic attributes
    pid = row["id"]
    username = row["author"]
    time = utc_to_dt(row["utc"])
    text = row["text"]

    # submission attrs
    if is_sub:
        url = row["url"]
        title = row["title"]
        num_comments = row["num_comments"]
        return Sub(pid=pid, username=username, time=time, text=text, url=url,
                   title=title, num_comments=num_comments, subr="opiates")

    # comment attrs
    parent_id = row["parent_id"]
    return Comm(pid=pid, username=username, time=time, text=text,
                parent_id=parent_id, subr="opiates")


def extract_csv(filepath: str, colnames: List[str]) -> List[Post]:
    """
    Read all submissions or comments from the given csv file.

    :param filepath: the csv filepath
    :param colnames: the column names associated with the csv file

    :returns: a list of Post objects derived from the file
    """
    # parse the given file if it is a csv
    if os.path.splitext(filepath)[1] == ".csv":
        # read data into csv file
        df: pd.DataFrame = pd.read_csv(filepath, header=None)
        df.columns = colnames

        return [row_to_post(row, "parent_id" not in df.columns)
                for _, row in df.iterrows()]

    # else, return empty list
    return []


def config_mongo(coll: Collection) -> None:
    """Conifgure the mongo collection."""
    coll.create_index([("text", "text")])
    coll.create_index([("time", pymongo.DESCENDING)])
    coll.create_index([("hash", pymongo.ASCENDING)], unique=True)


def to_mongo(coll: Collection, posts: List[Post]) -> str:
    """
    Add the given posts to mongo.

    :param coll: the pymongo collection to use
    :param posts: a list of posts to add to the collection

    :returns: a summary of the posts that were inserted
    """
    # configure the given collection if not yet configured
    config_mongo(coll)

    # serialize posts
    serial_posts: List[Dict[str, Any]] = [post.to_dict() for post in posts]

    # insert the given posts to the collection if they do not already exist
    n_inserts: int = 0
    for sp in serial_posts:
        try:
            coll.insert_one(sp)
            n_inserts += 1
        except pymongo.errors.DuplicateKeyError:
            pass

    # return response
    return f"{n_inserts}/{len(serial_posts)} posts inserted"


def last_date(coll: Collection) -> datetime:
    """Gets the newest date from the mongo collection."""
    res = coll.aggregate([{"$sort": {"time": -1}}, {"$limit": 1}])
    time = list(res)[0]["time"]
    return time


