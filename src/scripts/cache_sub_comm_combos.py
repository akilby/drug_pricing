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
	target_subs = SubmissionPost.objects.limit(limit).all()

	for sub in tqdm.tqdm(target_subs):
		pid = sub.pid
		sub_text = [sub.text]
		if pid not in cache:
			sub_comms = CommentPost.objects(parent_id=pid).all()
			for comm in sub_comms:
				sub_text.append(comm.text)
			cache[pid] = '. '.join(sub_text)
			pickle.dump(cache, open(cache_fp, 'wb'))


if __name__ == '__main__':
	main()