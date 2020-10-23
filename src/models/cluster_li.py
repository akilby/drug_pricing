"""A location inference model that attempts to cluster geocoded entities."""
import functools as ft
import itertools as it
from collections import Counter
from typing import List, Dict

from scipy.special import softmax
import geocoder
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
from src.schema import User


def find_center(points: np.array) -> np.array:
    """Find the point that is closest to the center of the points."""
    center = np.mean(points, axis=0)
    max_point_idx = np.argmax(cosine_similarity(center.reshape(1, -1), points))
    central_point = points[max_point_idx]
    return central_point


def filter_entities(entities: List[str], filters: List[BaseFilter]) -> List[str]:
    """Filter out entities based on filter criteria of the given filters."""
    distinct_entities = set(entities)
    possible_entities = ft.reduce(lambda acc, f: f.filter(acc),
                                  filters,
                                  distinct_entities)
    filtered_entities = [entity for entity in entities if entity in possible_entities]
    return filtered_entities


class LocationClusterer:
    def __init__(self,
                 filters: List[BaseFilter],
                 nlp: English):
        self.filters = filters
        self.nlp = nlp

    def extract_entities(self, user: User) -> List[str]:
        """Extract and filter all location entities for a user."""
        user_spacy_docs = get_user_spacy(user, self.nlp)
        user_entities = get_ents(user_spacy_docs, "GPE")
        filtered_user_entities = filter_entities(user_entities, self.filters)
        return filtered_user_entities

    def predict(self, entities: List[str]) -> Dict[geocoder.base.OneResult, float]:
        """Predicts the most likely locations for a user."""
        session = requests.Session()
        geocodes = it.chain(*[forward_geocode(e, session=session) for e in entities])
        latlngs = [(float(g.lat), float(g.lng)) for g in geocodes]

        clusters = DBSCAN(eps=1.5, min_samples=2).fit_predict(np.array(latlngs))
        cluster_counts = Counter(clusters)
        top_cluster_counts = cluster_counts.most_common(10)
        top_clusters = [c[0] for c in top_cluster_counts]
        top_counts = [c[1] for c in top_cluster_counts]

        centers = []
        for cluster in top_clusters:
            cluster_idx = [i for i in range(len(clusters)) if clusters[i] == cluster]
            cluster_latlngs = [latlngs[i] for i in cluster_idx]
            center = find_center(np.array(cluster_latlngs))
            centers.append(center)

        # TODO: fix why this is breaking on lat/lng extraction
        guessed_locations = [reverse_geocode(c[0], c[1], session=session)
                             for c in centers]
        scores = softmax(top_counts)

        return dict(zip(guessed_locations, scores))


if __name__ == "__main__":
    print("Initializing .....")
    connect_to_mongo()
    nlp = get_nlp()

    labels_df = pd.read_csv("data/geocoded-location-labels.csv").drop("Unnamed: 0", axis=1)
    gazetteer = pd.read_csv("data/locations/grouped-locations.csv")

    filters = [DenylistFilter(DENYLIST), LocationFilter(gazetteer)]
    model = LocationClusterer(filters, nlp)

    print("Making guesses for each user .....")
    users_locations = []
    for username in tqdm(labels_df["username"].tolist()):
        user = User.objects(username=username).first()
        model_entities = model.extract_entities(user)
        if len(model_entities) > 0:
            locations = model.predict(model_entities)
        else:
            locations = {}
        users_locations.append(locations)

    print("Writing to csv .....")
    labels_df["location_guesses"] = users_locations
    labels_df.to_csv("location-guesses.csv")

    print("Done.")
