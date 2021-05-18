import os
import pickle
import requests

from typing import List

from spacy.lang.en import English
from tqdm import tqdm

from src.utils import connect_to_mongo, get_nlp, ROOT_DIR
from src.schema import Post
from src.tasks.spacy import bytes_to_spacy
from src.models.__init__ import get_ents, forward_geocode


def cache_geocodes(docs: List[English], session: requests.sessions.Session, filepath: str):
	'''Memoize a map from entities from the spacy docs to geocodes in a file.'''
	if os.path.exists(filepath):
		ent_geocode_map = pickle.load(open(filepath, 'rb'))
	else:
		ent_geocode_map = {}

	ents = get_ents(docs, 'GPE')

	for e in ents:
		if e not in ent_geocode_map:
			ent_geocode_map[e] = forward_geocode(e, session=session, service='mapbox')

	pickle.dump(ent_geocode_map, open(filepath, 'wb'))


def main():
	# utility variables
	session = requests.Session()
	cache_filename = os.path.join(ROOT_DIR, 'data', 'geocodes_cache.pk')
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
	for pid_chunk in tqdm(pid_chunks):
		posts = Post.objects(pid__in=pid_chunk).all()
		docs = [bytes_to_spacy(p.spacy, nlp) for p in posts]
		cache_geocodes(docs, session, cache_filename)

	print('Done.')


if __name__ == '__main__':
	main()
