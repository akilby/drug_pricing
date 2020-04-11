from praw.models import Redditor
from praw import Reddit
from psaw import PushshiftAPI
import pymongo
from typing import Optional, List
import pandas as pd
import random
import prawcore


def get_users(coll: pymongo.collection.Collection,
              how: str = "top") -> List[str]:
    """
    Retrieve distinct usernames from the collection.

    Currently sorting by number of posts in collection and random sorting
    is supported.
    """
    if how == "top":
        query = [
            {"$group": {"_id": "$username",
                        "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}]
        res = coll.aggregate(query)
        users = [rec["_id"] for rec in res]
        filt_users = list(filter(lambda x: x and pd.notna(x), users))
        return filt_users
    elif how == "rand":
        res = coll.distinct("username")
        users = [u for u in res if u is not None]
        rand_users = random.sample(users, len(users))
        return rand_users
    else:
        raise ValueError("Invalid 'how' type given.")


def user_posts(praw: Reddit,
               psaw: PushshiftAPI,
               user: str,
               filt_mods: Optional[bool] = None) -> Optional[pd.DataFrame]:
    """Retrieve the full reddit posting history for the given users."""
    print("On user:", user)
    subs = list(psaw.search_submissions(author=user))
    comms = list(psaw.search_comments(author=user))
    username = [user] * (len(subs) + len(comms))
    text = [s.selftext for s in subs] + [c.body for c in comms]
    subr = [s.subreddit for s in subs] + [c.subreddit for c in comms]
    is_sub = [True] * len(subs) + [False] * len(comms)
    ids = [s.id for s in subs] + [c.id for c in comms]
    data = {"username": username, "text": text, "subreddit": subr,
            "is_sub": is_sub, "id": ids}
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
                filt_mods: bool = False) -> List[pd.DataFrame]:
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
    dfs = [user_posts(praw, psaw, user, filt_mods) for user in n_users]
    dfs = [df for df in dfs if df is not None]
    return dfs


def top_histories(coll: pymongo.collection.Collection,
                  praw: Reddit,
                  psaw: PushshiftAPI,
                  n_top: int,
                  n_rand: int) -> List[str]:
    # get top user histories
    top_users = get_users(coll, how="top")
    top_history_dfs = users_posts(top_users, praw, psaw,
                                  n_top, filt_mods=True)

    # get random user histories
    rand_users = get_users(coll, how="rand")
    rand_history_dfs = users_posts(rand_users, praw, psaw,
                                   n_rand, filt_mods=False)

    # get full user histories
    history_df = pd.concat(top_history_dfs + rand_history_dfs)

    return history_df