import os
import pickle
import itertools as it
import re

from datetime import datetime
from typing import List, Set, Tuple, Dict

import tqdm
import pandas as pd

from src.utils import connect_to_mongo, get_nlp, ROOT_DIR
from src.schema import Post, User, SubmissionPost, CommentPost


def cache_tree():
	print('Querying for submissions')
	all_submissions = SubmissionPost.objects(subreddit='opiates').only('pid').all()
	all_sub_pids = [s.pid for s in all_submissions]

	print('Querying for comments')
	res = CommentPost.objects(subreddit='opiates', parent_id__exists=True)\
		.only('pid', 'parent_id')\
		.all()

	print('Grouping parent ids')
	par_id_map = pd.DataFrame([(r.pid, r.parent_id.split('_')[1]) for r in res], columns=['pid', 'parent_id'])\
		.groupby('parent_id')\
		.agg(lambda x: list(x))['pid']\
        .to_dict()

	matches = set(par_id_map.keys()) & set(all_sub_pids)
	matched_par_id_map = {k: v for k, v in par_id_map.items() if k in matches}

	print('Writing to file')
	pickle.dump(matched_par_id_map, open('cache/parent_id_tree.pk', 'wb'))


def combine_tree_text():
	connect_to_mongo()

	par_id_map = pickle.load(open('cache/parent_id_tree.pk', 'rb'))

	cache = {}

	for pid in tqdm.tqdm(par_id_map):
		all_ids = [pid] + par_id_map[pid]
		subset_posts = SubmissionPost.objects(subreddit='opiates', pid__in=all_ids).only('text').all()
		texts = [p.text for p in subset_posts]
		cache[pid] = texts
		pickle.dump(open('cache/sub_tree_texts.pk', 'wb'))

	
def build_tree():
	connect_to_mongo()

	cache_fp = os.path.join(ROOT_DIR, 'cache', 'sub_comm_tree_path.pk')
	if os.path.exists(cache_fp):
		cache = pickle.load(open(cache_fp, 'rb'))
	else:
		cache = {}

	previous_comm_pids = []

	batch_size = 100
	all_submissions = SubmissionPost.objects(subreddit='opiates').limit(1000).all()
	all_sub_pids = [s.pid for s in all_submissions]
	for sub in all_submissions:
		if sub.pid not in cache:
			cache[sub.pid] = {'title': sub.title, 'body': sub.text, 'comms': []}

	all_comm_pids_objs = CommentPost.objects(subreddit='opiates', parent_id__exists=True)\
		.only('pid', 'parent_id')\
		.limit(10)\
		.all()
	par_id_map = pd.DataFrame([(r.pid, r.parent_id) for r in all_comm_pids_objs], columns=['pid', 'parent_id'])\
		.groupby('parent_id')\
		.agg(lambda x: list(x))['pid']\
		.to_dict()

	all_comm_pids = [c.pid for c in all_comm_pids_objs]
	start_idx = 0
	comm_pid_batches = []
	n_batches = (len(all_comm_pids) // batch_size) + 1
	for i in range(n_batches):
		end_idx = start_idx + batch_size
		comm_pid_batches.append(all_comm_pids[start_idx:end_idx])
		start_idx = end_idx
		
	for comm_batch_pids in tqdm.tqdm(comm_pid_batches):
		subset_comms = CommentPost.objects(subreddit='opiates', pid__in=comm_batch_pids).all()
		subset_comm_par_ids = [c.parent_id for c in subset_comms]
		for i, par_id in enumerate(subset_comm_par_ids):
			match_idxs = [pid for pid in all_sub_pids if pid in par_id]
			if len(match_idxs) > 0:
				cache[sub.pid]['comms'].append(subset_comms[i])

		pickle.dump(cache, open(cache_fp, 'wb'))


def main():
	connect_to_mongo()

	cache_fp = os.path.join(ROOT_DIR, 'cache', 'sub_comm_tree_path.pk')
	if os.path.exists(cache_fp):
		cache = pickle.load(open(cache_fp, 'rb'))
	else:
		cache = {}

	limit = 10000
	skipping = False
	target_subs = SubmissionPost.objects.limit(limit).all()

	for sub in tqdm.tqdm(target_subs):
		pid = sub.pid
		sub_texts = [sub.text]
		comment_ids = []
		if pid not in cache and skipping:
			sub_comms = CommentPost.objects(parent_id=pid).all()
			for comm in sub_comms:
				sub_texts.append(comm.text)
			cache[pid] = {'texts': sub_texts, 'title': sub.title, 'comment_ids': comment_ids}
			pickle.dump(cache, open(cache_fp, 'wb'))


if __name__ == '__main__':
	cache_tree()	
