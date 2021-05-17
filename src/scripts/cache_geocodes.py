import os
import pickle
import requests

import pandas as pd

from spacy.lang.en import English

from src.utils import connect_to_mongo, get_nlp, ROOT_DIR
from src.models.cluster_li import get_geocodes
from src.models.__init__ import get_ents


def cache_geocodes(docs: List[English], session: requests.session.Session, filepath: str):
    '''Memoize a map from entities from the spacy docs to geocodes in a file.'''
    if os.path.exists(filename):
        ent_geocode_map = pickle.load(open(filepath, 'rb'))
    else:
        ent_geocode_map = {}

    ents = get_ents(docs, 'GPE')

    for e in ents:
        if e not in ent_geocode_map:
            ent_geocode_map[e] = get_geocodes(e, session)


def main():
    # utility variables
    session = requests.Session()
    cache_filename = os.path.join(ROOT_DIR, 'data', 'cache_geocodes.pk')

    # retrieve all post ids
    post_subsets = Post.objects(spacy__exists=True).only('pid').all()
    pids = [p.pid for p in post_subsets]

    # cache geocodes in chunks
    chunk_size = 100000
    pid_chunks = (pids[idx:(idx + chunk_size)]
                  for idx in range(0, len(pids), chunk_size))
    for pid_chunk in chunks:
        posts = Post.objects(pid__in=pid_chunk).all()
        cache_geocodes(posts, session, cache_filename)


if __name__ == '__main__':
    main()
