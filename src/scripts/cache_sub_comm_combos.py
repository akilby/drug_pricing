import os
import pickle
import itertools as it

from datetime import datetime
from typing import List, Set, Tuple, Dict

import tqdm
import pandas as pd

from src.utils import connect_to_mongo, get_nlp, ROOT_DIR
from src.schema import Post, User, SubmissionPost, CommentPost


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
	main()