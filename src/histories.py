"""Functions for extracting user histories."""
import random
from typing import List, Optional

import pandas as pd
import prawcore
import pymongo
from praw import Reddit
from praw.models import Redditor
from psaw import PushshiftAPI

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


def user_posts(psaw: PushshiftAPI, user: str) -> Optional[pd.DataFrame]:
    """Retrieve the full reddit posting history for the given users."""
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


def get_non_mods(users: List[str],
                 praw: Reddit,
                 n: int,
                 acc: List[str] = []) -> List[str]:
    """Retrieve n users that are not moderators."""
    if n <= 0 or n >= len(users) or len(users) == 0:
        return acc
    user = users.pop(0)
    redditor = Redditor(praw, user)
    try:
        if hasattr(redditor, "is_mod") and (not redditor.is_mod):
            acc.append(user)
            return get_non_mods(users, praw, n - 1, acc=acc)
    except prawcore.exceptions.NotFound:
        pass
    return get_non_mods(users, praw, n, acc=acc)


def users_posts(users: List[str],
                praw: Reddit,
                psaw: PushshiftAPI,
                n: int,
                filt_mods: bool = False) -> pd.DataFrame:
    """
    Retrieve the full reddit posting history for all given users.

    :param conn: a psaw connection object
    :param users: a list of usernames

    :return: a dataframe of post features
    """
    # get n users that are not moderators if desired
    if filt_mods:
        n_users = get_non_mods(users, praw, n)
    # otheriwse just select first n users
    else:
        if n <= len(users):
            n_users = users[:n]
        else:
            raise ValueError("n is too big")
    dfs = [user_posts(psaw, user) for user in n_users]
    df = pd.concat([df for df in dfs if df is not None])
    return df


def all_user_hists(praw: Reddit, psaw: PushshiftAPI,
                   coll: pymongo.collection.Collection) -> List[Post]:
    """Retrieve full posting history for all users."""
    # retrieve all users
    print("Retrieving users .....")
    all_users = get_users(coll, how="all")
    pct_start = .4
    pct_end = .5
    i_start = int(len(all_users) * pct_start)
    i_end = int(len(all_users) * pct_end)
    sub_users = all_users[i_start:i_end]

    # retrieve all user's posts
    print("Retrieving user posts .....")
    posts_df = users_posts(sub_users, praw, psaw, len(sub_users))

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

    print("Converting histories to Posts .....")
    posts = [hist_to_post(row) for _, row in posts_df.iterrows()]
    return posts
