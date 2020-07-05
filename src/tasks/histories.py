"""Functions for extracting user histories."""
import random
from typing import List, Optional

import pandas as pd
import prawcore
import pymongo
import tqdm
from praw import Reddit
from praw.models import Redditor
from psaw import PushshiftAPI

from src.pipeline import to_mongo
from src.utils import Comm, Post, Sub, utc_to_dt


def get_users(coll: pymongo.collection.Collection,
              how: str = "top") -> List[str]:
    """
    Retrieve distinct usernames from the collection.

    Currently sorting by number of posts in collection and random sorting
    is supported.
    """
    if how == "top":
        query = [{
            "$group": {
                "_id": "$username",
                "count": {
                    "$sum": 1
                }
            }
        }, {
            "$sort": {
                "count": -1
            }
        }]
        res = coll.aggregate(query)
        users = [rec["_id"] for rec in res]
        filt_users = list(filter(lambda x: x and pd.notna(x), users))
        return filt_users
    if how == "rand":
        res = coll.distinct("username")
        users = [u for u in res if u is not None]
        rand_users = random.sample(users, len(users))
        return rand_users
    if how == "all":
        users = list(coll.distinct("username"))
        return users
    raise ValueError("Invalid 'how' type given.")


def user_posts(psaw: PushshiftAPI, user: str) -> pd.DataFrame:
    """Retrieve the full reddit posting history for the given user."""
    subs = list(psaw.search_submissions(author=user))
    comms = list(psaw.search_comments(author=user))
    username = [user] * (len(subs) + len(comms))
    text = [s.selftext for s in subs] + [c.body for c in comms]
    subr = [sc.subreddit.display_name for sc in subs + comms]
    times = [utc_to_dt(p.created_utc) for p in subs + comms]
    is_sub = [True] * len(subs) + [False] * len(comms)
    ids = [s.id for s in subs] + [c.id for c in comms]
    data = {
        "username": username,
        "text": text,
        "subreddit": subr,
        "is_sub": is_sub,
        "id": ids,
        "time": times
    }
    df = pd.DataFrame(data)
    return df


def hist_to_post(row) -> Post:
    """Convert posts from df form to Post form."""
    if row["is_sub"]:
        return Sub(username=row["username"],
                   text=row["text"],
                   pid=row["id"],
                   subr=row["subreddit"],
                   time=row["time"].to_pydatetime())
    return Comm(username=row["username"],
                text=row["text"],
                pid=row["id"],
                subr=row["subreddit"],
                time=row["time"].to_pydatetime())


def get_users_histories(users: List[str],
                        psaw: PushshiftAPI,
                        coll: pymongo.collection.Collection,
                        filt_mods: bool = False) -> None:
    """
    Retrieve the full reddit posting history for all given users.

    :param conn: a psaw connection object
    :param users: a list of usernames

    :return: a dataframe of post features
    """
    for user in tqdm.tqdm(users):
        df = user_posts(psaw, user)
        posts = [hist_to_post(row) for _, row in df.iterrows()]
        to_mongo(coll, posts)
