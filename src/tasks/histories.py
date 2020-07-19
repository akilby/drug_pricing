"""Functions for extracting user histories."""
import random
from typing import List, Optional

import pandas as pd
import pymongo
import tqdm
from psaw import PushshiftAPI

from src.pipeline import to_mongo
from src.utils import CustomComment, CustomSubmission, Post, utc_to_dt


def get_users(coll: pymongo.collection.Collection, how: str = "top") -> List[str]:
    """
    Retrieve distinct usernames from the collection.

    Currently sorting by number of posts in collection and random sorting
    is supported.
    """
    if how == "top":
        query = [{"$group": {"_id": "$username", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}]
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


def extract_user_posts(psaw: PushshiftAPI, user: str) -> List[Post]:
    """Retrieve the full reddit posting history for the given user."""
    submissions = list(psaw.search_submissions(author=user))
    comments = list(psaw.search_comments(author=user))
    posts = []

    for s in submissions:
        posts.append(
            CustomSubmission(
                username=user,
                text=s.selftext,
                pid=s.id,
                subr=s.subreddit.display_name,
                time=utc_to_dt(s.created_utc),
                title=s.title,
                url=s.url,
                num_comments=s.num_comments,
            )
        )

    for c in comments:
        posts.append(
            CustomComment(
                username=user,
                text=c.body,
                pid=c.id,
                subr=c.subreddit.display_name,
                time=utc_to_dt(c.created_utc),
                parent_id=c.parent_id,
            )
        )
    return posts


def get_users_histories(
    users: List[str],
    psaw: PushshiftAPI,
    coll: pymongo.collection.Collection,
    cache_fn: Optional[str] = None,
) -> None:
    """
    Retrieve the full reddit posting history for all given users.

    :param users: a list of usernames
    :param psaw: a psaw connection object
    :param coll: a pymongo collection
    :param cache_file: an optional cache file to write remaining users to
    """
    for i in tqdm.tqdm(range(len(users))):
        posts = extract_user_posts(psaw, users[i])
        to_mongo(coll, posts)

        if cache_fn:
            new_users = users[(i + 1) :]
            pd.Series(new_users).to_csv(cache_fn, index=False, header=False)
