'''A location inference model that attempts to cluster geocoded entities.'''
import functools as ft
import itertools as it
from collections import Counter
from typing import List, Dict, Tuple, Any
import pickle

from scipy.special import softmax
import geocoder
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


def geonames_json_to_location(
    json_result: Dict[str, Any],
    metro_state_coords: List[Tuple[float, float]],
    metro_state_coords_list: List[Tuple[str, Tuple[float, float]]]
) -> Location:
    """Transform a geonames api result in JSON form to a Location."""
    print(json_result)
    loc_params = {}
    if 'countryName' in json_result:
        loc_params = {
            'country': json_result['countryName'],
            'lat': json_result['lat'],
            'lng': json_result['lng']
        }
        if loc_params['country'] == 'United States':
            if json_result['fcl'] not in ['A', 'P']:
                # assign to closest metro area
                nearest_metro_idx = nearest_coords((json_result['lat'], json_result['lng']),
                                                   metro_state_coords)
                nearest_metro = metro_state_coords_list[nearest_metro_idx][0]
                loc_params['metro'] = nearest_metro.split(',')[0]
                loc_params['state_full'] = nearest_metro.split(',')[1]
                # TODO: replace lat/lng w/ metro lat/lng
            else:
                loc_params['state_full'] = json_result['adminName1']
                loc_params['country'] = json_result['countryName']
                if json_result['fcl'] == 'P':
                    loc_params['city'] = json_result['toponymName']

    return Location(**loc_params)


class LocationClusterer:
    def __init__(self,
                 filters: List[BaseFilter],
                 nlp: English):
        self.filters = filters
        self.nlp = nlp
        self.gazetteer = pd.read_csv('data/gazetteer.csv')
        self.metro_state_coords_map = pickle.load(open('data/metro_state_coord_map.pk', 'rb'))

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

        # convert entities to possible coordinates
        session = requests.Session()
        geocodes = it.chain(*[forward_geocode(e, service='geonames', session=session)
                              for e in entities])
        latlngs = [(float(g.lat), float(g.lng)) for g in geocodes]

        # cluster all possible coordinates
        clusters = DBSCAN(eps=1.5, min_samples=2).fit_predict(np.array(latlngs))
        clusters_idx = [i for i, c in enumerate(clusters) if c >= 0]
        clusters = [clusters[i] for i in clusters_idx]
        latlngs = [latlngs[i] for i in clusters_idx]
        if len(clusters) == 0:
            return {}

        # extract the largest clusters
        cluster_counts = Counter(clusters)
        top_cluster_counts = cluster_counts.most_common(10)
        top_clusters = [c[0] for c in top_cluster_counts]
        top_counts = [c[1] for c in top_cluster_counts]

        # find the centers for each of the largest clusters
        centers = []
        for cluster in top_clusters:
            cluster_idx = [i for i in range(len(clusters)) if clusters[i] == cluster]
            cluster_latlngs = [latlngs[i] for i in cluster_idx]
            center = find_center(np.array(cluster_latlngs))
            centers.append(center)

        # convert center coordinates to a physical place
        guessed_locations = [reverse_geocode(c[0], c[1], service='geonames', session=session)
                             for c in centers]

        # normalize the scores
        scores = softmax(top_counts)

        metro_state_coords_list = list(self.metro_state_coords_map.items())
        metro_state_coords = [m[1] for m in metro_state_coords_list]

        # convert geonames responses to Locations
        locations = [geonames_json_to_location(guess, metro_state_coords, metro_state_coords_list)
                     for guess in guessed_locations]

        location_score_map = {locations[i]: scores[i] for i in range(len(scores))}

        breakpoint()
        return location_score_map


if __name__ == '__main__':
    print('Initializing .....')
    connect_to_mongo()
    nlp = get_nlp()

    labels_df = pd.read_csv('data/geocoded-location-labels.csv').iloc[0:10, :]
    gazetteer = pd.read_csv('data/locations/grouped-locations.csv')

    filters = [DenylistFilter(DENYLIST), LocationFilter(gazetteer)]
    model = LocationClusterer(filters, nlp)

    print('Making guesses for each user .....')
    users_entities = []
    users_locations = []
    for username in tqdm(labels_df['username'].tolist()):
        user = User.objects(username=username).first()

        entities = model.extract_entities(user)
        users_entities.append(entities)

        locations = model.predict(entities)
        users_locations.append(locations)

    print('Writing to pickle .....')
    labels_df['entity_guesses'] = users_entities
    labels_df['location_guesses'] = users_locations
    pickle.dump(labels_df, open('data/location-guesses-10.pk', 'wb'))

    print('Done.')
