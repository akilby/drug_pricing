'''A location inference model that attempts to cluster geocoded entities.'''
import functools as ft
import itertools as it
import os
import pickle
import time
from collections import Counter
from typing import Any, Dict, Iterable, List, Tuple
import datetime

import geocoder
import numpy as np
import pandas as pd
import requests
import tqdm

from geocoder.geonames import GeonamesResult
from geocoder.mapbox import MapboxResult
from geopy import distance
from scipy.special import softmax
from sklearn.cluster import DBSCAN
from sklearn.metrics.pairwise import cosine_similarity
from spacy.lang.en import English

from src.models.__init__ import (DENYLIST, forward_geocode, get_ents,
                                 get_user_spacy, reverse_geocode)
from src.models.filters import BaseFilter, DenylistFilter, LocationFilter
from src.schema import Location, User, Post
from src.utils import ROOT_DIR, connect_to_mongo, get_nlp

# store the number of times geonames has been requested globally
NUM_GEONAMES_REQUESTS = 0
MIN_POPULATION = 30000

LARGE_STATE_MAP = {
    'california': ['california', 'southern california', 'northern california'],
    'texas': ['el paso, texas', 'houston, texas', 'dallas, texas'],
    'florida': ['florida', 'tallahassee, florida', 'miami, florida'],
    'alaska': ['alaska', 'juneau, alaska', 'anchorage, alaska', 'fairbanks, alaska']
}

ALIAS_MAP = {
    'vegas': 'las vegas',
    'nyc': 'new york city',
    'l.a.': 'los angeles'
}


def find_center(points: np.array) -> np.array:
    '''Find the point that is closest to the center of the points.'''
    center = np.mean(points, axis=0)
    max_point_idx = np.argmax(cosine_similarity(center.reshape(1, -1), points))
    central_point = points[max_point_idx]
    return central_point


def filter_entities(entities: List[str], filters: List[BaseFilter]) -> List[str]:
    '''Filter out entities based on filter criteria of the given filters.'''
    distinct_entities = set(entities)
    possible_entities = ft.reduce(lambda acc, f: f.filter(acc),
                                  filters,
                                  distinct_entities)
    filtered_entities = [entity for entity in entities if entity in possible_entities]
    return filtered_entities


def nearest_coords(
    target_coord: Tuple[float, float],
    possible_coords: List[Tuple[float, float]]
) -> int:
    '''
    Return the index of possible coords that contains coordinates
    closests to the target coord
    '''
    distances = [distance.distance(coord, target_coord) for coord in possible_coords]
    return np.argmin(distances)


def map_state_abbrevs(gazetteer: pd.DataFrame) -> Dict[str, str]:
    '''Create a mapping from state abbreviations to state names'''
    abbrevs = gazetteer['state'].tolist()
    state_full = gazetteer['state_full'].tolist()
    return dict(zip(abbrevs, state_full))


def split_states(entities: List[str]) -> List[str]:
    '''Split large states into subsections.'''
    new_ents = []
    for ent in entities:
        if ent in LARGE_STATE_MAP:
            new_ents += LARGE_STATE_MAP[ent]
        else:
            new_ents.append(ent)
    return new_ents


def geocode_to_location(geocode: geocoder.base.OneResult) -> Location:
    '''convert a geocode to a location.'''
    is_geonames = isinstance(geocode, GeonamesResult)
    # define base location params
    loc_params = {
        'country': geocode.country,
        'lat': float(geocode.lat),
        'lng': float(geocode.lng),
        'population': (geocode.population if is_geonames else -1)
    }

    # add more detail if geocode in US
    city_codes = ["RGNE"]
    if geocode.country == 'United States':
        geonames_cond = lambda geocode: geocode.feature_class == 'P' or geocode.code in city_codes
        mapbox_cond = lambda geocode: (isinstance(geocode.__dict__['raw']['place_name'], str) and (len(geocode.__dict__['raw']['place_name'].split(',')) == 3))
        city_cond = geonames_cond if is_geonames else mapbox_cond
        if city_cond(geocode):
            loc_params['city'] = geocode.address if is_geonames else geocode.__dict__['raw']['text']
        loc_params['state'] = geocode.state

    return Location(**loc_params)


def location_comparator(l1: Tuple[Location, int], l2: Tuple[Location, int]) -> int:
    '''
    Compare two frequency-locations, where a frequency-location is a location
    paired with a frequency weight.

    -1 if l1 should be before l2
    1 if l1 should be after l2
    0 if the two are equal
    '''
    # prioritize a location with a city over one without one
    if l1[0].city and not l2[0].city:
        return -1

    if l2[0].city and not l1[0].city:
        return 1

    # prioritize a location with a higher frequency count
    if l1[1] > l2[1]:
        return -1

    if l1[1] < l2[1]:
        return 1

    # prioritize a location wih a larger population
    if l1[0].population > l2[0].population:
        return -1

    if l2[0].population > l1[0].population:
        return 1

    return 0


def best_cluster_location(geocodes: geocoder.base.OneResult) -> Location:
    '''Determine the best location to represent the given cluster.'''
    # convert geocodes to locations
    locations = [geocode_to_location(g) for g in geocodes]

    # filter out cities with populations less than some threshold
    locations = [l for l, g in zip(locations, geocodes)
                 if l.population > MIN_POPULATION and isinstance(g, GeonamesResult)]

    # get frequency counts for locations
    location_frequencies = list(Counter(locations).items())

    # sort locations using the above comparator
    sorted_locations = sorted(location_frequencies, key=ft.cmp_to_key(location_comparator))

    if len(sorted_locations) > 0:
        return sorted_locations[0]
    else:
        return Location()


def get_geocodes(entity: str, session, cache: Dict = None, service='geonames') -> Iterable[geocoder.base.OneResult]:
    '''Convert an entity to a list of possible geocodes.'''
    if cache and entity in cache:
        return cache[entity]

    # if number of geonames requests 1000, pause for 1 hr
    if service == 'geonames':
        global NUM_GEONAMES_REQUESTS
        if NUM_GEONAMES_REQUESTS >= 1000:
            print('Pausing for 1 hr .....')
            time.sleep(3600)
            NUM_GEONAMES_REQUESTS = 0

        NUM_GEONAMES_REQUESTS += 1

    geocodes = forward_geocode(entity, service=service, session=session)
    return geocodes


class LocationClusterer:
    def __init__(self,
                 filters: List[BaseFilter],
                 nlp: English,
                 use_caches: bool = True):
        self.filters = filters
        self.nlp = nlp
        self.session = requests.Session()
        self.use_caches = use_caches
        self.load_caches()

    def load_caches(self):
        self.gazetteer = pd.read_csv(os.path.join(ROOT_DIR, 'resources', 'gazetteer.csv'))
        self.metro_state_coords_map = pickle.load(open(os.path.join(ROOT_DIR, 'resources', 'metro_state_coord_map.pk'), 'rb'))
        self.state_abbrev_map = map_state_abbrevs(self.gazetteer)
        self.subreddit_location_map = dict(pd.read_csv(os.path.join(ROOT_DIR, 'resources', 'subreddit_location_map.csv')).values)

        user_ents_filepath = os.path.join(ROOT_DIR, 'cache', 'user_ents_cache.pk')
        if os.path.exists(user_ents_filepath):
            self.user_ents_cache = pickle.load(open(user_ents_filepath, 'rb'))
        else:
            self.user_ents_cache = {}

        geocodes_filepath = os.path.join(ROOT_DIR, 'cache', 'geonames_geocodes_cache.pk')
        if os.path.exists(geocodes_filepath):
            self.geocodes_cache = pickle.load(open(geocodes_filepath, 'rb'))
        else:
            self.geocodes_cache = {}

        post_count_fp = os.path.join(ROOT_DIR, 'cache', 'users_post_counts_cache.pk')
        if os.path.exists(post_count_fp):
            post_count_cache = pickle.load(open(post_count_fp, 'rb'))
        else:
            post_count_cache = {}

        post_timerange_fp = os.path.join(ROOT_DIR, 'cache', 'users_post_timerange_cache.pk')
        if os.path.exists(post_timerange_fp):
            post_timerange_cache = pickle.load(open(post_timerange_fp, 'rb'))
        else:
            post_timerange_cache = {}


    def extract_entities(self, user: User) -> List[str]:
        '''Extract and filter all location entities for a user.'''
        # use cache if requests + exists
        if self.use_caches and user.username in self.user_ents_cache:
            return self.user_ents_cache[user.username]

        breakpoint()

        # extract entities from spacy
        user_spacy_docs = get_user_spacy(user, self.nlp)
        user_entities = get_ents(user_spacy_docs, 'GPE')
        filtered_user_entities = filter_entities(user_entities, self.filters)

        # add subreddit as entity if represents a location subreddit
        subreddit_entities = [self.subreddit_location_map[p.subreddit]
                              for p in Post.objects(user=user).only('subreddit')
                              if p.subreddit in self.subreddit_location_map]

        all_entities = filtered_user_entities + subreddit_entities
        return all_entities

    def predict(self, entities: List[str], user: User) -> Dict[Location, float]:
        '''Predicts the most likely locations for a user.'''
        # return empty map if there are no entities to utilize
        if len(entities) == 0:
            return {}

        # store initial entity names
        init_entities = entities

        # convert state abbreviations to full state names
        entities = [self.state_abbrev_map[e] if e in self.state_abbrev_map else e
                    for e in entities]

        # convert common location nicknames to full names
        entities = [ALIAS_MAP[e] if e in ALIAS_MAP else e for e in entities]

        # split large states into different geocodable regions
        entities = split_states(entities)

        # convert entities to possible coordinates
        geocodes = []
        for entity in entities:
            if self.use_caches and entity in self.geocodes_cache:
                geocodes += self.geocodes_cache[entity]
            else:
                breakpoint()
                geocodes += get_geocodes(entity, self.session, service='geonames')
        latlngs = [(float(g.lat), float(g.lng)) for g in geocodes]

        if len(latlngs) <= 1:
            return {}

        # cluster all possible coordinates
        clusters = DBSCAN(eps=2.5, min_samples=2).fit_predict(np.array(latlngs))

        # remove clusters that have only 1 item
        clusters_idx = [i for i, c in enumerate(clusters) if c >= 0]
        clusters = [clusters[i] for i in clusters_idx]
        geocodes = [geocodes[i] for i in clusters_idx]

        # return an empty map if there are no real clusters
        if len(clusters) == 0:
            return {}

        # extract the largest 10 clusters
        cluster_counts = Counter(clusters)
        top_cluster_counts = cluster_counts.most_common(10)
        top_clusters = [c[0] for c in top_cluster_counts]
        top_counts = [c[1] for c in top_cluster_counts]

        # guess the most representative location within each cluster
        location_guesses = []
        for cluster in top_clusters:
            cluster_idx = [i for i in range(len(clusters)) if clusters[i] == cluster]
            cluster_geocodes = [geocodes[i] for i in cluster_idx]
            location_guess = best_cluster_location(cluster_geocodes)
            location_guesses.append(location_guess)

        # normalize the scores
        score_features = [{
            'cluster_pct': tc / sum(top_counts),
            'num_entities': len(entities),
            'is_in_us': int(location_guesses[i].country == 'United States'),
            'num_posts': self.post_count_cache[user.username],
            'timerange': self.post_timerange_cache[user.username],
            'population': location_guesses[i].population,
        } for i, tc in enumerate(top_counts)]

        # map each location guess to a score
        location_score_map = {location_guesses[i]: score_features[i] for i in range(len(score_features))}

        return location_score_map


def run_all_users():
    '''Run location inference on all users.'''
    connect_to_mongo()
    nlp = get_nlp()

    users = User.objects.all()
    gazetteer = pd.read_csv(os.path.join(ROOT_DIR, 'resources', 'gazetteer.csv'))

    filters = [DenylistFilter(DENYLIST), LocationFilter(gazetteer)]
    model = LocationClusterer(filters, nlp)

    predictions = []
    for i, user in tqdm.tqdm(enumerate(users)):
        if user.username != 'autotldr':
            entities = model.extract_entities(user)
            preds = model.predict(entities, user)
            predictions.append(preds)

    usernames = [u.username for u in users]
    user_preds = dict(zip(usernames, predictions))
    ts = str(int(datetime.datetime.now().timestamp()))
    preds_fp = os.path.join(ROOT_DIR, 'results', f'user-predictions-{ts}.pk')
    pickle.dump(user_preds, open(preds_fp, 'wb'))


if __name__ == '__main__':
    print('Initializing .....')
    connect_to_mongo()
    nlp = get_nlp()

    usernames = pd.read_csv(os.path.join(ROOT_DIR, 'clutter', '600-users.csv'), squeeze=True, header=None).tolist()
    gazetteer = pd.read_csv(os.path.join(ROOT_DIR, 'resources', 'gazetteer.csv'))

    filters = [DenylistFilter(DENYLIST), LocationFilter(gazetteer)]
    model = LocationClusterer(filters, nlp)

    print('Making guesses for each user .....')
    users_entities = []
    users_locations = []
    for username in tqdm.tqdm(usernames):
        user = User.objects(username=username).first()

        entities = model.extract_entities(user)
        users_entities.append(entities)

        locations = model.predict(entities, user)
        users_locations.append(locations)

    print('Writing to pickle .....')
    labels_df = pd.DataFrame({
        'usernames': usernames,
        'entity_guesses': users_entities,
        'location_guesses': users_locations
    })
    ts = str(int(datetime.datetime.now().timestamp()))
    pickle.dump(labels_df, open(os.path.join(ROOT_DIR, 'clutter', f'location-guesses-all-{ts}.pk'), 'wb'))

    print('NUM_GEONAMES_REQUESTS:', NUM_GEONAMES_REQUESTS)
    print('Done.')
