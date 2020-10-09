"""A location inference model that matches entities with a gazetteer."""
import functools as ft
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
from scipy.special import softmax
from spacy.lang.en import English
from tqdm import tqdm

from src.models.__init__ import get_ents, get_user_spacy
from src.schema import Location, Post, User
from src.utils import connect_to_mongo, get_nlp

from .filters import BaseFilter, DenylistFilter, LocationFilter
from .rankers import BaseRanker, FrequencyRanker

DENYLIST = {"china", "russia", "turkey", "op"}

# -- HELPER FUNCTIONS --


def rankings_averager(gpe: str, rankings: List[Dict[str, float]]) -> float:
    """Combine rankings for a given gpe by averaging each rankings respective score."""
    scores = [r[gpe] for r in rankings]
    return sum(scores) / len(scores)


def load_locations(fp: str) -> Set[Location]:
    """
    Retrieve US city/county/state locations from a csv filepath.
    """
    df = pd.read_csv(fp)

    def string_or_none(maybe_string: Any) -> Optional[str]:
        return maybe_string if type(maybe_string) == str else None

    def row_to_loc(row: pd.Series) -> Location:
        return Location(neighborhood=string_or_none(row["neighborhood"]),
                        city=string_or_none(row["city"]),
                        county=string_or_none(row["county"]),
                        state=string_or_none(row["state"]),
                        country=string_or_none(row["country"]),
                        metro=string_or_none(row["metro"]))

    return set([row_to_loc(row) for _, row in df.iterrows()])


def find_parent_location(record_1: Location,
                         record_2: Location) -> List[Location]:
    """
    If a parent exists, return a list just containing that parent.
    Else, return the two records.
    """
    parent = Location()
    if record_1["country"] == record_2["country"]:
        parent["country"] = record_1["country"]
        if record_1["state"] == record_2["state"]:
            parent["state"] = record_1["state"]
            if record_1["county"] == record_2["county"]:
                parent["county"] = record_1["county"]
                if record_1["city"] == record_2["city"]:
                    parent["city"] = record_1["city"]

    if parent.country:
        return [parent]
    return [record_1, record_2]


def group_locations(locations: Set[Location]) -> List[Location]:
    """Find a common parent for the records if possible."""

    for location in locations:
        location.neighborhood = None

    return list(set(locations))


def group_locations_by_score(
    records: List[Location], scores: List[float]
) -> Tuple[List[Location], List[float]]:
    """Attempt to find a common location parent for locations with the same score."""
    # group records with same score
    score_records_map = dict()
    for record, score in zip(records, scores):
        if score in score_records_map:
            score_records_map[score].append(record)
        else:
            score_records_map[score] = [record]

    # reduce records for each score that have a common ancestor
    new_records = []
    new_scores = []
    for score in score_records_map:
        records = score_records_map[score]
        grouped_records = group_locations(set(records))
        new_records = new_records + grouped_records
        new_scores = new_scores + [score] * len(grouped_records)

    return new_records, new_scores


def group_by_metro(location_scores: Dict[Location, float]) -> Dict[Location, float]:
    # sum scores for each metro area
    metro_scores = {l.metro: 0.0 for l in location_scores}
    for loc in location_scores:
        score = location_scores[loc]
        metro_scores[loc.metro] += score

    def metro_to_location(metro: str) -> Optional[Location]:
        """Helper func to generate full location for a given metro"""
        for location in location_scores:
            if location.metro == metro:
                return Location(metro=metro,
                                state=location.state,
                                country=location.country)
        return None

    # convert metro areas to locations
    new_location_scores: Dict[Location, float] = dict()
    for metro in metro_scores:
        if type(metro) == str:
            # if metro area exists, convert metro area to a location
            new_location_scores[metro_to_location(metro)] = metro_scores[metro]
        else:
            # otherwise, add each location that doesn't have a metro area with its score
            non_metro_locs = [l for l in location_scores if not l.metro]
            for l in non_metro_locs:
                new_location_scores[l] = location_scores[l]

    return new_location_scores


# -- MODEL DECLARATION --


class LocationMatcher:
    def __init__(self, filters: List[BaseFilter], rankers: List[BaseRanker],
                 real_locations: pd.DataFrame, nlp: English):
        self.filters = filters
        self.rankers = rankers
        self.real_locations = real_locations
        self.nlp = nlp

    def score_entities(self, user: User) -> Dict[str, float]:
        """Assign a likeliness score to each location entity in a user's posting history."""
        # extract user locations
        spacy_docs = get_user_spacy(user, self.nlp)
        gpes = get_ents(spacy_docs, "GPE")

        # apply filters
        possible_gpes = ft.reduce(lambda acc, f: f.filter(acc), self.filters,
                                  set(gpes))
        filtered_gpes = [gpe for gpe in gpes if gpe in possible_gpes]

        # rank gpes with different methods
        rankings = [r.rank(filtered_gpes) for r in self.rankers]

        # combine rankings
        scores = {
            gpe: rankings_averager(gpe, rankings)
            for gpe in possible_gpes
        }

        return scores

    def score_locations(
            self,
            entity_scores: Dict[str, float]
    ) -> List[Tuple[Location, float]]:
        """
        Convert each location entity in a user's posting history to a real location.
        Then, assign a likeliness score to each location.
        """
        records = self.real_locations.to_dict(orient="records")

        guessed_records = []
        scores = []
        for record in records:
            score = 0.0
            for field in record.values():
                if field in entity_scores:
                    score += entity_scores[field]
            if score > 0:
                guessed_records.append(record)
                scores.append(score)

        guessed_locations = [Location(**record) for record in guessed_records]

        metro_scores = group_by_metro({
            l: s for l, s in zip(guessed_locations, scores)
        })

        normalized_scores = softmax(list(metro_scores.values())) if len(metro_scores) > 0 else []

        location_scores = {
            location: score
            for location, score in zip(list(metro_scores.keys()), normalized_scores)
        }

        return sorted(location_scores.items(),
                      key=lambda _: _[1],
                      reverse=True)

    def _predict(self, location_scores: Dict[Location, float]) -> Optional[Location]:
        """
        Compute likeliness scores for each entity in a user's posting history and
        package them into a single most likely location.
        """
        if len(location_scores) > 0:
            max_loc = sorted(location_scores.items(),
                             key=lambda _: _[1],
                             reverse=True)[0]
            return max_loc
        return None


if __name__ == "__main__":

    print("Connecting to mongo .....")
    connect_to_mongo()

    # define filters/rankers
    fp = "data/locations/grouped-locations.csv"
    locations = pd.read_csv(fp)
    filters = [DenylistFilter(DENYLIST), LocationFilter(locations)]
    rankers = [FrequencyRanker()]
    nlp = get_nlp()

    # instantiate model
    model = LocationMatcher(filters, rankers, locations, nlp)

    use_top = True
    print("Getting users .....")

    if use_top:
        # get the users with the most posts
        pipeline = [{"$sortByCount": "$user"}, {"$limit": 5}]
        res = Post.objects().aggregate(pipeline)
        ids = [str(r["_id"]) for r in res if r["_id"]]
        users = User.objects(id__in=ids)
    else:
        usernames = pd.read_csv("data/rand_user_200.csv",
                                squeeze=True).tolist()
        users = User.objects(username__in=usernames).all()

    print("Making predictions .....")
    scores = [model.score_locations(user) for user in tqdm(users)]
    breakpoint()
