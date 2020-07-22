"""Define utility functions for the data pipeline."""
import re
from datetime import datetime
from typing import List, Optional

from psaw import PushshiftAPI

from src.schema import Post
from src.utils import sub_comm_to_post


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
    sub_objs: List[Post] = [sub_comm_to_post(s, True) for s in subs]
    comm_objs: List[Post] = [sub_comm_to_post(c, False) for c in comms]

    # return list of combined post objects
    return sub_objs + comm_objs


def validate_praw(
    psaw: PushshiftAPI, subr: str, start_str: str, end_str: str, limit: int
) -> List[Post]:
    """Read from praw starting from the given date."""
    if re.match(r"\d{4}-\d{2}-\d{2}", start_str):
        start_date = datetime.strptime(start_str, "%Y-%m-%d")

        if not end_str:
            praw_data = extract_praw(psaw, subr, start_date, limit=limit)
            return praw_data

        if re.match(r"\d{4}-\d{2}-\d{2}", end_str):
            end_date = datetime.strptime(end_str, "%Y-%m-%d")
            praw_data = extract_praw(psaw, subr, start_date, limit=limit, end_time=end_date)

        return praw_data

    # raise exception if date incorrectly formatted
    raise ValueError("Invalid date provided.")
