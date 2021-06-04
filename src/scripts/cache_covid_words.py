import os
import pickle
import requests
import itertools as it

from datetime import datetime
from typing import List, Set

import tqdm
import pandas as pd

from src.utils import connect_to_mongo, get_nlp, ROOT_DIR
from src.schema import Post, User
from src.tasks.spacy import bytes_to_spacy
from src.models.__init__ import get_user_spacy, get_ents, DENYLIST, forward_geocode
from src.models.cluster_li import LocationClusterer, get_geocodes, map_state_abbrevs, ALIAS_MAP, LARGE_STATE_MAP
from src.models.filters import BaseFilter, DenylistFilter, LocationFilter


def cache_users_covid_words():
    '''Cache covid word counts pre/post lockdown.'''
    # establish timeframes
    lockdown_dt = datetime(2020, 3, 1)
    max_dt = datetime.now()
    min_dt = lockdown_dt - (max_dt - lockdown_dt)

    # establish keywords
    keywords = ['money', 'withdrawal', 'overdose']

    # get cache file
    cache_fp = os.path.join(ROOT_DIR, 'cache', 'covid_keyword_counts_cache.pk')
    if os.path.exists(cache_fp):
        cache = pickle.load(open(cache_fp, 'rb'))
    else:
        cache = {}

    all_users = User.objects.all()

    for user in tqdm.tqdm(all_users):
        if user.username not in cache:
            cache[user.username] = {}

        for keyword in keywords:
            if keyword not in cache[user.username]:
                pre_posts = Post.objects(user=user, datetime__gt=min_dt, datetime__lt=lockdown_dt, text__icontains=keyword).count()
                post_posts = Post.objects(user=user, datetime__lt=max_dt, datetime__gt=lockdown_dt, text__icontains=keyword).count()
                cache[user.username][keyword] = (pre_posts, post_posts)
                pickle.dump(cache, open(cache_fp, 'wb'))


if __name__ == '__main__':
    connect_to_mongo()
    cache_users_covid_words()
