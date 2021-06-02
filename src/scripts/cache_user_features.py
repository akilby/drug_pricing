import os
import pickle
import requests
import itertools as it

from typing import List, Set

import tqdm
import pandas as pd

from src.utils import connect_to_mongo, get_nlp, ROOT_DIR
from src.schema import Post, User
from src.tasks.spacy import bytes_to_spacy
from src.models.__init__ import get_user_spacy, get_ents, DENYLIST, forward_geocode
from src.models.cluster_li import LocationClusterer, get_geocodes, map_state_abbrevs, ALIAS_MAP, LARGE_STATE_MAP
from src.models.filters import BaseFilter, DenylistFilter, LocationFilter


def get_user_post_count(user: User) -> int:
    '''Get the number of posts in a users history.'''
    post_count = Post.objects(user=user).count()
    return post_count


def get_user_post_timerange(user: User) -> int:
    '''Get the first, last datetime of a user's posting history.'''
    u_posts = Post.objects(user=user).only('datetime').all()
    u_dt = [p.datetime for p in u_posts]

    if len(u_dt) > 0:
        timerange = (max(u_dt) - min(u_dt)).days
    else:
        timerange = 0

    return timerange


def cache_users_features():
    '''Cache features of a user that will be used for confidence scoring.'''
    users = User.objects.all()
    post_count_fp = os.path.join(ROOT_DIR, 'cache', 'users_post_counts_cache.pk')
    post_timerange_fp = os.path.join(ROOT_DIR, 'cache', 'users_post_timerange_cache.pk')

    if os.path.exists(post_count_fp):
        post_count_cache = pickle.load(open(post_count_fp, 'rb'))
    else:
        post_count_cache = {}

    if os.path.exists(post_timerange_fp):
        post_timerange_cache = pickle.load(open(post_timerange_fp, 'rb'))
    else:
        post_timerange_cache = {}

    for user in tqdm.tqdm(users):
        if user not in post_count_cache:
            user_post_count = get_user_post_count(user)
            post_count_cache[user.username] = user_post_count
            pickle.dump(post_count_cache, open(post_count_fp, 'wb'))

        if user not in post_timerange_cache:
            timerange = get_user_post_timerange(user)
            post_timerange_cache[user.username] = timerange
            pickle.dump(post_timerange_count, open(post_timerange_fp, 'wb'))


if __name__ == '__main__':
    cache_users_features()
