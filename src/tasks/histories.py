"""Functions for extracting user histories."""
import random
from typing import List, Optional

import pandas as pd
import tqdm
from psaw import PushshiftAPI

from src.schema import Post, User
from src.utils import posts_to_mongo, sub_comm_to_post


def get_users(how: str = "all") -> List[str]:
    """
    Retrieve distinct usernames from the collection.

    Currently sorting by number of posts in collection and random sorting
    is supported.
    """
    all_users = [u.username for u in User.objects]
    if how == "rand":
        users = [u for u in all_users if u is not None]
        rand_users = random.sample(users, len(users))
        return rand_users
    if how == "all":
        return all_users
    raise ValueError("Invalid 'how' type given.")


def extract_user_posts(psaw: PushshiftAPI, user: str) -> List[Post]:
    """Retrieve the full reddit posting history for the given user."""
    try:
        submissions = list(psaw.search_submissions(author=user))
        comments = list(psaw.search_comments(author=user))

        posts = [sub_comm_to_post(s, True) for s in submissions] + \
                [sub_comm_to_post(c, False) for c in comments]
        return posts
    except Exception:
        print(f"User: {user} was not found with PushshiftAPI")
        return []


def get_users_histories(
    users: List[str], psaw: PushshiftAPI, cache_fn: Optional[str] = None,
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
        posts_to_mongo(posts)

        if cache_fn:
            new_users = users[(i + 1) :]
            pd.Series(new_users).to_csv(cache_fn, index=False, header=False)
