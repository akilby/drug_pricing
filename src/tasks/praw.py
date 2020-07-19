"""Define utility functions for the data pipeline."""
from datetime import datetime
import re
from typing import Any, Dict, List, Optional, Union

import pymongo
from praw.models import Comment, Submission
from psaw import PushshiftAPI
from pymongo.collection import Collection

from src.schema import CommentPost, Post, SubmissionPost
from src.utils import utc_to_dt


def sub_comm_to_post(sub_comm: Union[Submission, Comment], is_sub: bool, subr: str) -> Post:
    """Convert a Praw Submission or Comment to a Post object."""
    # generic attributes
    pid = sub_comm.id
    username = None if not sub_comm.author else sub_comm.author.name
    datetime = utc_to_dt(sub_comm.created_utc)

    # submission attrs
    if is_sub:
        url = sub_comm.url
        text = sub_comm.selftext
        title = sub_comm.title
        num_comments = sub_comm.num_comments
        return SubmissionPost(
            pid=pid,
            username=username,
            datetime=datetime,
            text=text,
            url=url,
            title=title,
            num_comments=num_comments,
            subr=subr,
        )

    # comment attrs
    text = sub_comm.body
    parent_id = sub_comm.parent_id
    return CommentPost(
        pid=pid,
        username=username,
        time=datetime,
        text=text,
        parent_id=parent_id,
        subr=subr
    )


def extract_praw(
    psaw: PushshiftAPI,
    subr: str,
    start_time: datetime,
    limit: Optional[int] = None,
    end_time: Optional[datetime] = None,
) -> List[Post]:
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

    subs = list(
        psaw.search_submissions(after=start_int, subreddit=subr, limit=limit, before=end_int)
    )
    comms = list(psaw.search_comments(after=start_int, subreddit=subr, limit=limit, before=end_int))

    # convert Submission/Comment object to Sub/Comm objects
    sub_objs: List[Post] = [sub_comm_to_post(s, True, subr) for s in subs]
    comm_objs: List[Post] = [sub_comm_to_post(c, False, subr) for c in comms]

    # return list of combined post objects
    return sub_objs + comm_objs


def read_praw(
    psaw: PushshiftAPI,
    subr: str,
    start_str: str,
    end_str: str,
    limit: int
) -> List[Post]:
    """Read from praw starting from the given date."""
    if re.match(r"\d{4}-\d{2}-\d{2}", start_str):
        print("Extracting posts from praw .....")
        start_date = datetime.strptime(start_str, "%Y-%m-%d")

        if not end_str:
            praw_data = extract_praw(psaw, subr, start_date, limit=limit)
            print(f"{len(praw_data)} posts from Reddit retrieved.")
            return praw_data

        if re.match(r"\d{4}-\d{2}-\d{2}", end_str):
            end_date = datetime.strptime(end_str, "%Y-%m-%d")
            praw_data = extract_praw(psaw, subr, start_date, limit=limit, end_time=end_date)

        print(f"{len(praw_data)} posts from Reddit retrieved.")
        return praw_data

    # raise exception if date incorrectly formatted
    raise ValueError("Invalid date provided.")
