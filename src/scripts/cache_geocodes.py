import os
import pickle
import requests

from typing import List

import tqdm
import pandas as pd

from spacy.lang.en import English

from src.utils import connect_to_mongo, get_nlp, ROOT_DIR
from src.schema import Post
from src.tasks.spacy import bytes_to_spacy
from src.models.__init__ import get_user_spacy, get_ents, DENYLIST, forward_geocode
from src.models.filters import BaseFilter, DenylistFilter, LocationFilter


def cache_geocodes(docs: List[English], session: requests.sessions.Session, filepath: str):
	'''Memoize a map from entities from the spacy docs to geocodes in a file.'''
	if os.path.exists(filepath):
		ent_geocode_map = pickle.load(open(filepath, 'rb'))
	else:
		ent_geocode_map = {}

	ents = get_ents(docs, 'GPE')

	for e in ents:
		if e not in ent_geocode_map:
			ent_geocode_map[e] = forward_geocode(e, session=session)

	pickle.dump(ent_geocode_map, open(filepath, 'wb'))


def cache_ents(docs: List[English], filepath: str):
    '''Memoize a set of entities in the database.'''
    if os.path.exists(filepath):
        ent_list = pd.read_csv(filepath, header=None).squeeze().tolist()
    else:
        ent_list = []

    ents = get_ents(docs, 'GPE')

    for e in ents:
        if e not in ent_list:
            ent_list.append(e)

    pd.Series(ent_list).to_csv(filepath, index=False, header=None)


def cache_user_ents(user: User, filepath: str, lc: LocationClusterer):
    '''Cache all entities for a given user.'''
    if os.path.exists(filepath):
        user_ents_cache = pickle.load(open(filepath, 'rb'))
    else:
        user_ents_cache = {}

    user_ents = lc.extract_entities(user)

    if user.username not in user_ents_cache:
        user_ents_cache[user.username] = user_ents

        pickle.dump(user_ents_cache, open(filepath, 'wb'))


def cache_all_user_ents():
    print('Initializing .....')
    cache_filepath = os.path.join(ROOT_DIR, 'data', 'user_ents_cache.pk')
    nlp = get_nlp()
    connect_to_mongo()
    gazetteer = pd.read_csv("data/locations/grouped-locations.csv")
    filters = [DenylistFilter(DENYLIST), LocationFilter(gazetteer)]
    model = LocationClusterer(filters, nlp)
    all_users = Users.objects.all()

    print('Caching user entities .....')
    for user in tqdm.tqdm(all_users):
        cache_user_ents(user, cache_filepath, model)

    print('Done.')


def main():
    # utility variables
    session = requests.Session()
    cache_filename = os.path.join(ROOT_DIR, 'data', 'geocodes_cache.pk')
    ent_cache_filename = os.path.join(ROOT_DIR, 'data', 'ents_cache.csv')
    nlp = get_nlp()
    connect_to_mongo()

    # retrieve all post ids
    print('Retrieving all pids .....')
    post_subsets = Post.objects(spacy__exists=True).only('pid').all()
    pids = [p.pid for p in post_subsets]

    # cache geocodes in chunks
    print('Iterating over chunks .....')
    chunk_size = 100000
    pid_chunks = (pids[idx:(idx + chunk_size)]
                    for idx in range(0, len(pids), chunk_size))
    for pid_chunk in tqdm.tqdm(pid_chunks):
        posts = Post.objects(pid__in=pid_chunk).all()
        docs = [bytes_to_spacy(p.spacy, nlp) for p in posts]
        # cache_geocodes(docs, session, cache_filename)
        cache_ents(docs, ent_cache_filename)

    print('Done.')


if __name__ == '__main__':
	# main()
    cache_all_user_ents()
