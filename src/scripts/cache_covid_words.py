import os
import pickle
import requests
import itertools as it
import re
import math

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

KEYWORDS = [
	'money', 
	'withdraw', 
	'overdose', 
	'fent', 
	'heroin', 
	'addict', 
	'pain', 
	'tolerance', 
	'oxy', 
	'covid', 
	'virus', 
	'corona', 
	'quarantine',
	'unemployment',
	'pandemic', 
	'vaccinations',
	'testing center',
	'stimulus',
	'stimmies',
	'death',
	'died',
	'withdrawal',
 ]

def cache_users_covid_words():
	'''Cache covid word counts pre/post lockdown.'''
	# establish timeframes
	lockdown_dt = datetime(2020, 3, 1)
	max_dt = datetime.now()
	min_dt = lockdown_dt - (max_dt - lockdown_dt)

	# establish keywords

	# get cache file
	ts = str(int(datetime.now().timestamp()))
	cache_read_fp = os.path.join(ROOT_DIR, 'cache', 'covid_keyword_counts_cache_7.pk')
	cache_write_fp = os.path.join(ROOT_DIR, 'cache', 'covid_keyword_counts_cache_7.pk')
	if os.path.exists(cache_read_fp):
		cache = pickle.load(open(cache_read_fp, 'rb'))
	else:
		cache = {}

	print('Loading all post ids .....')
	all_pid_objs = Post.objects(datetime__gt=min_dt).only('pid').all()
	all_pids = [obj.pid for obj in all_pid_objs]

	batch_size = 100000
	n_batches = math.ceil(len(all_pids) / batch_size)

	print('Iterating through batches .....')
	for batch_num in tqdm.tqdm(range(n_batches)):
		n_start, n_end = (batch_num * batch_size, (batch_num + 1) * batch_size)
		current_pids = all_pids[n_start:n_end]
		current_posts = Post.objects(pid__in=current_pids).only('pid', 'text', 'user', 'datetime')
		for post in current_posts:
			if post.user:
				username = post.user.username

				if username not in cache:
					cache[username] = {}
					for keyword in KEYWORDS:
						cache[username][keyword] = (0, 0)

				use_pre = post.datetime < lockdown_dt

				for keyword in KEYWORDS:
					pre_count, post_count = cache[username][keyword]
					try:
						keyword_count = len(re.findall(keyword, post.text)) 
						new_pre_count = pre_count + keyword_count if use_pre else pre_count
						new_post_count = post_count + keyword_count if not use_pre else post_count
						cache[username][keyword] = (new_pre_count, new_post_count)
					except:
						pass
		
		pickle.dump(cache, open(cache_write_fp, 'wb'))


if __name__ == '__main__':
	connect_to_mongo()
	cache_users_covid_words()
