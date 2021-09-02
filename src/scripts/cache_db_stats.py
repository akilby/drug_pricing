import os
import pickle
import itertools as it

from datetime import datetime
from typing import List, Set, Tuple, Dict

import tqdm
import pandas as pd

from src.utils import connect_to_mongo, get_nlp, ROOT_DIR
from src.schema import Post, User


def get_num_subreddits() -> int:
    pipeline = [
        {"$group": {"_id": "$subreddit"}},
        {"$group": {"_id": 1, "count": {"$sum": 1}}}
    ]
    scounts = Post.objects().aggregate(pipeline)
    nsubreddits = list(scounts)[0]['count']
    return nsubreddits


def get_dt_range() -> Tuple[datetime, datetime]:
    pipeline_min = [
        {"$group": {"_id": {}, "mindt": {"$min": "$datetime"}}}
    ]
    pipeline_max = [
        {"$group": {"_id": {}, "maxdt": {"$max": "$datetime"}}}
    ]
    dmin_out = Post.objects().aggregate(pipeline_min)
    dmax_out = Post.objects().aggregate(pipeline_max)

    dmin = list(dmin_out)[0]['mindt']
    dmax = list(dmax_out)[0]['maxdt']

    return (dmin, dmax)


def get_year_counts() -> Dict[int, int]:
    pipeline = [
        {"$group": {"_id": {"$year": "$datetime"}, "count": {"$sum": 1}}}
    ]
    year_counts = Post.objects().aggregate(pipeline)
    year_counts_dict = {r['_id']: r['count'] for r in list(year_counts)}
    return year_counts_dict

def get_month_counts() -> pd.DataFrame:
    pipeline = [
        {
            "$group": { 
                "_id": { "year": {"$year": "$datetime"}, "month": {"$month": "$datetime"}}, 
                "num_datetimes": {"$sum": {"$cond": { "if": { "$ne": ["$datetime","true"] },"then": 1,"else": 0 }}},
                "num_spacy": {"$sum": {"$cond": { "if": { "$ne": ["$spacy","true"] },"then": 1,"else": 0 }}}
            }
        }
    ]
    month_counts = Post.objects().limit(100000).aggregate(pipeline)
    res = list(month_counts)
    rows = [{'datetime': datetime(r['_id']['year'], r['_id']['month'], 1), 
            'num_datetimes': r['num_datetimes'],
            'num_spacy': r['num_spacy'],
            'num_posts': r['num_posts']
            } for r in res]
    df = pd.DataFrame(rows)
    return df

def main():
    connect_to_mongo()
    cache_fp = os.path.join(ROOT_DIR, 'cache', 'db_stats.pk')
    if not os.path.exists(cache_fp):
        cache = {}
    else:
        cache = pickle.load(open(cache_fp, 'rb'))

    print('Loading subreddit counts .....')
    subr_counts = get_num_subreddits()
    cache['subreddit_counts'] = subr_counts
    pickle.dump(cache, open(cache_fp, 'wb'))

    print('Getting dt range .....')
    dt_range = get_dt_range()
    cache['dt_range'] = dt_range
    pickle.dump(cache, open(cache_fp, 'wb'))

    print('Getting year counts .....')
    year_counts = get_year_counts()
    cache['year_counts'] = year_counts
    pickle.dump(cache, open(cache_fp, 'wb'))


if __name__ == '__main__':
    # main()
    connect_to_mongo()
    month_counts_df = get_month_counts()
    month_counts_df.to_csv('cache/month_counts.csv', index=False)
