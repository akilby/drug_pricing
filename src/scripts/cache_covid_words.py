import os
import pickle
import requests
import itertools as it
import re
import math

from datetime import datetime
from dateutil import rrule
from typing import List, Set

import tqdm
import pandas as pd
import spacy

from nltk.corpus import wordnet
from itertools import chain
from spacy.lang.en import English

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
	'check',
	'payment',
	'paid',
	'broke',
	'finance',
	'dying',
	'trust',
	'dry',
	'empty',
	'bill',
	'health',
	'mental',
	'spend',
	'bank',
	'narc',
	'desparate',
	'funds',
    'irs'
 ]


def build_new_keywords(og_keywords: List[str], nlp: English) -> Set[str]:
	 '''Synonymize and lemmatize all keywords.'''
	 lemmas = set([t.lemma_ for t in nlp(' '.join(og_keywords))])
	 synonyms = []
	 for key_lemma in lemmas:
		 for synset in wordnet.synsets(key_lemma):
			 for syn_lemma in synset.lemma_names():
				 synonyms.append(syn_lemma.replace('_', ' ').replace('-', ' ').lower())
	 return set(synonyms)


def cache_users_covid_words(nlp: English):
	'''Cache covid word counts pre/post lockdown.'''
	# establish timeframes
	lockdown_dt = datetime(2020, 3, 15)
	max_dt = datetime.now()
	min_dt = lockdown_dt - (max_dt - lockdown_dt)
	dtrange = rrule.rrule(rrule.MONTHLY, dtstart=min_dt, until=max_dt)

	# establish keywords
	new_keywords = build_new_keywords(KEYWORDS, nlp)

	# get cache file
	ts = str(int(datetime.now().timestamp()))
	cache_read_fp = os.path.join('/work/akilby/drug_pricing_project', 'cache', 'covid_keyword_counts_cache.pk')
	cache_write_fp = os.path.join('/work/akilby/drug_pricing_project', 'cache', 'covid_keyword_counts_cache.pk')
	if os.path.exists(cache_read_fp):
		cache = pickle.load(open(cache_read_fp, 'rb'))
	else:
		cache = {}

	print('Loading all post ids .....')
	all_pid_objs = Post.objects(datetime__gt=min_dt).only('pid').all()
	all_pids = [obj.pid for obj in all_pid_objs]

	batch_size = 10000
	n_batches = math.ceil(len(all_pids) / batch_size)

	print('Iterating through batches .....')
	for batch_num in tqdm.tqdm(range(n_batches)):
		n_start, n_end = (batch_num * batch_size, (batch_num + 1) * batch_size)
		current_pids = all_pids[n_start:n_end]
		current_posts = Post.objects(pid__in=current_pids).only('pid', 'spacy', 'user', 'datetime')
		for post in current_posts:
			if post.user and post.datetime and post.datetime.year and post.datetime.month and post.spacy:
				username = post.user.username

				lemmas = [t.lemma_.lower() for t in bytes_to_spacy(post.spacy, nlp)]

				if username not in cache:
					cache[username] = {k: {(d.month, d.year): 0 for d in dtrange} for k in new_keywords}

				for keyword in new_keywords:
					date_key = (post.datetime.month, post.datetime.year)
					keyword_count = lemmas.count(keyword)
					try:
						cache[username][keyword][date_key] += keyword_count
					except:
						breakpoint()
		
		pickle.dump(cache, open(cache_write_fp, 'wb'))


if __name__ == '__main__':
	nlp = spacy.load("en_core_web_sm")
	connect_to_mongo()
	cache_users_covid_words(nlp)
