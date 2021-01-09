'''A location inference model that attempts to cluster geocoded entities.'''
import functools as ft
import itertools as it
from collections import Counter
from typing import List, Dict, Tuple, Any, Iterable
import pickle
import time

from scipy.special import softmax
import geocoder
from geocoder.geonames import GeonamesResult
from geopy import distance
import numpy as np
import requests
from sklearn.cluster import DBSCAN
from sklearn.metrics.pairwise import cosine_similarity
from spacy.lang.en import English
import pandas as pd
from tqdm import tqdm

from src.models.__init__ import (forward_geocode, get_ents, get_user_spacy,
                                 reverse_geocode, DENYLIST)
from src.models.filters import BaseFilter, DenylistFilter, LocationFilter
from src.utils import connect_to_mongo, get_nlp
from src.schema import User, Location

# store the number of times geonames has been requested globally
NUM_GEONAMES_REQUESTS = 0


LARGE_STATE_MAP = {
    'california': ['southern california', 'northern california'],
    'texas': ['el paso, texas', 'houston, texas', 'dallas, texas'],
    'florida': ['florida', 'tallahassee', 'miami']
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


def geocode_to_location(geocode: GeonamesResult) -> Location:
    '''convert a geocode to a location.'''
    # define base location params
    loc_params = {
        'country': geocode.country,
        'lat': float(geocode.lat),
        'lng': float(geocode.lng),
        'population': geocode.population
    }

    # add more detail if geocode in US
    city_codes = ["RGNE"]
    if geocode.country == 'United States':
        if geocode.feature_class == 'P' or geocode.code in city_codes:
            loc_params['city'] = geocode.address
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


def best_cluster_location(geocodes: GeonamesResult) -> Location:
    '''Determine the best location to represent the given cluster.'''
    # convert geocodes to locations
    locations = [geocode_to_location(g) for g in geocodes]

    # get frequency counts for locations
    location_frequencies = list(Counter(locations).items())

    # sort locations using the above comparator
    sorted_locations = sorted(location_frequencies, key=ft.cmp_to_key(location_comparator))

    return sorted_locations[0]


def get_geocodes(entity: str, session) -> Iterable[GeonamesResult]:
    '''Convert an entity to a list of possible geocodes.'''
    # if number of geonames requests 1000, pause for 1 hr
    global NUM_GEONAMES_REQUESTS
    print('NUM_GEONAMES_REQUESTS:', NUM_GEONAMES_REQUESTS)
    if NUM_GEONAMES_REQUESTS >= 1000:
        print('Pausing for 1 hr .....')
        time.sleep(3600)
        NUM_GEONAMES_REQUESTS = 0

    NUM_GEONAMES_REQUESTS += 1
    geocodes = forward_geocode(entity, service='geonames', session=session)
    return geocodes


class LocationClusterer:
    def __init__(self,
                 filters: List[BaseFilter],
                 nlp: English):
        self.filters = filters
        self.nlp = nlp
        self.gazetteer = pd.read_csv('data/gazetteer.csv')
        self.metro_state_coords_map = pickle.load(open('data/metro_state_coord_map.pk', 'rb'))
        self.state_abbrev_map = map_state_abbrevs(self.gazetteer)
        self.session = requests.Session()

    def extract_entities(self, user: User) -> List[str]:
        '''Extract and filter all location entities for a user.'''
        user_spacy_docs = get_user_spacy(user, self.nlp)
        user_entities = get_ents(user_spacy_docs, 'GPE')
        filtered_user_entities = filter_entities(user_entities, self.filters)
        return filtered_user_entities

    def predict(self, entities: List[str]) -> Dict[Location, float]:
        '''Predicts the most likely locations for a user.'''
        # return empty map if there are no entities to utilize
        if len(entities) == 0:
            return {}

        # convert state abbrevitions to full state names
        init_entities = entities
        entities = [self.state_abbrev_map[e] if e in self.state_abbrev_map else e
                    for e in entities]
        entities = split_states(entities)

        # convert entities to possible coordinates
        geocodes = list(it.chain(*[get_geocodes(e, self.session) for e in entities]))
        latlngs = [(float(g.lat), float(g.lng)) for g in geocodes]

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
        scores = [tc / sum(top_counts) for tc in top_counts]

        # map each location guess to a score
        location_score_map = {location_guesses[i]: scores[i] for i in range(len(scores))}

        return location_score_map


def cache_user_geocodes(username: str):
    '''Store a dataframe with geocodes from a user's posting history.'''
    print("Configuring .....")
    connect_to_mongo()
    nlp = get_nlp()
    gazetteer = pd.read_csv('data/locations/grouped-locations.csv')
    filters = [DenylistFilter(DENYLIST), LocationFilter(gazetteer)]
    model = LocationClusterer(filters, nlp)
    user = User.objects(username=username).first()

    print("Extracting entities .....")
    user_entities = model.extract_entities(user)

    print("Extracting geocodes .....")
    geocodes = [geocoder.geonames(entity, key="cccdenhart", maxRows=5)
                for entity in user_entities]
    rows = []
    for e, geocode in zip(user_entities, geocodes):
        for g in geocode:
            rows.append({"entity": e,
                         "address": g.address,
                         "lat": float(g.lat),
                         "lng": float(g.lng),
                         "score": g.population})

    df = pd.DataFrame(rows)

    print("Saving ......")
    pickle.dump(df, open(f"data/user_geocodes/{username}_geocodes.pk", "wb"))


if __name__ == '__main__':
    print('Initializing .....')
    connect_to_mongo()
    nlp = get_nlp()

    usernames = pd.read_csv('data/all-rand-sample-users.csv', squeeze=True,
                            header=None).tolist()
    gazetteer = pd.read_csv('data/locations/grouped-locations.csv')

    filters = [DenylistFilter(DENYLIST), LocationFilter(gazetteer)]
    model = LocationClusterer(filters, nlp)

    print('Making guesses for each user .....')
    users_entities = []
    users_locations = []
    for username in tqdm(usernames):
        user = User.objects(username=username).first()

        entities = model.extract_entities(user)
        users_entities.append(entities)

        locations = model.predict(entities)
        users_locations.append(locations)

    print('Writing to pickle .....')
    labels_df = pd.DataFrame({
        'usernames': usernames,
        'entity_guesses': users_entities,
        'location_guesses': users_locations
    })
    pickle.dump(labels_df, open('data/location-guesses-all.pk', 'wb'))

    print('Done.')
