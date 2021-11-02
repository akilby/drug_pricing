import os
import pickle
import itertools as it

from datetime import datetime
from typing import List, Set, Tuple, Dict

import tqdm
import pandas as pd

from src.utils import connect_to_mongo, get_nlp, ROOT_DIR
from src.schema import Post, User, SubmissionPost, CommentPost


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


def opiate_month_counts() -> Dict[int, int]:
	pipeline = [
		{"$match": {"subreddit": "opiates"}},
		{"$group": {"_id": {"year": {"$year": "$datetime"}, "month": {"$month": "$datetime"}}, "count": {"$sum": 1}}}
	]
	month_counts = list(Post.objects().aggregate(pipeline))
	# month_counts_dict = {r['_id']: r['count'] for r in list(month_counts)}
	return month_counts



def get_month_counts() -> pd.DataFrame:
	pipeline = [
		{
			"$group": { 
				"_id": { "year": {"$year": "$datetime"}, "month": {"$month": "$datetime"}}, 
				"num_datetimes": {"$sum": {"$cond": { "if": { "$ne": ["$datetime","true"] },"then": 1,"else": 0 }}},
				"num_spacy": {"$sum": {"$cond": { "if": { "$ne": ["$spacy","true"] },"then": 1,"else": 0 }}},
				"num_posts": {"$sum": 1}
			}
		}
	]
	month_counts = Post.objects(subreddit='opiates').aggregate(pipeline)
	res = list(month_counts)
	rows = [{'datetime': datetime(r['_id']['year'], r['_id']['month'], 1),
			'num_datetimes': r['num_datetimes'],
			'num_spacy': r['num_spacy'],
			'num_posts': r['num_posts']
			} for r in res if (r['_id']['year'] and r['_id']['month'])]
	df = pd.DataFrame(rows)
	return df


def get_user_month_counts() -> pd.DataFrame:
	pipeline = [
		{
			"$group": {
				"_id": { "year": {"$year": "$datetime"}, "month": {"$month": "$datetime"}, "user": "$user"},
				"num_datetimes": {"$sum": {"$cond": { "if": { "$ne": ["$datetime","true"] },"then": 1,"else": 0 }}},
				"num_spacy": {"$sum": {"$cond": { "if": { "$ne": ["$spacy","true"] },"then": 1,"else": 0 }}},
				"num_posts": {"$sum": 1},
			}
		}
	]
	month_counts = Post.objects(subreddit='opiates').aggregate(pipeline, allowDiskUse=True)
	res = list(month_counts)
	ids = [str(r['_id']['user']) if 'user' in r['_id'] else None for r in res]
	usernames = [User.objects(pk=_id).only('username').first().username if _id else None for _id in ids]
	full_data = list(zip(usernames, res))
	'''
	rows = [{'datetime': datetime(r['_id']['year'], r['_id']['month'], 1),
			'num_datetimes': r['num_datetimes'],
			'num_spacy': r['num_spacy'],
			'num_posts': r['num_posts']
			} for r in res if (r['_id']['year'] and r['_id']['month'])]
	df = pd.DataFrame(rows)
	return df
	'''
	return full_data


def policy_counts():
	rop_count = Post.objects(subreddit='opiates').count()
	print('opiates post count:', rop_count)

	rops_count = SubmissionPost.objects(subreddit='opiates').count()
	print('opiates sub count:', rops_count)

	ropc_count = CommentPost.objects(subreddit='opiates').count()
	print('opiates comm count:', ropc_count)

	her_count = Post.objects(subreddit='heroin').count()
	print('heroin post count:', her_count)

	hers_count = SubmissionPost.objects(subreddit='opiates').count()
	print('heroin sub count:', hers_count)

	herc_count = CommentPost.objects(subreddit='opiates').count()
	print('heroin comm count:', herc_count)

	sub_count = SubmissionPost.objects.count()
	print('submission count:', sub_count)

	comm_count = CommentPost.objects.count()
	print('comm count:', comm_count)

	min_rop = Post.objects(subreddit='opiates', datetime__exists=True).order_by('datetime').limit(1)
	rop_start = min_rop[0]
	print('opiates start', rop_start)

	min_rhe = Post.objects(subreddit='heroin', datetime__exists=True).order_by('datetime').limit(1)
	her_start = min_rhe[0]
	print('heroin start', her_start)

	data = {
		'op_count': rop_count,
		'op_sub_count': rops_count,
		'op_comm_count': ropc_count,
		'her_count': her_count,
		'her_sub_count': hers_count,
		'her_comm_count': herc_count,
		'sub_count': sub_count,
		'comm_count': comm_count,
		'op_start': rop_start,
		'her_start': her_start
	}

	with open('/work/akilby/drug_pricing_project/denhart_cache/db_stats_10_21', 'wb') as f:
		pickle.dump(data, f)


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
	# month_counts_df = get_month_counts()
	# month_counts = get_user_month_counts()
	policy_counts()
	ts = datetime.now()
	# with open(f'/work/akilby/drug_pricing_project/denhart_cache/opiates_month_user_counts_{ts}.pk', 'wb') as f:
	# 	pickle.dump(month_counts, f)
	# month_counts_df.to_csv('cache/month_counts_opiates.csv', index=False)
