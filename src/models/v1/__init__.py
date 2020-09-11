import functools as ft
import itertools as it
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Set

import pandas as pd
from scipy.special import softmax
from spacy.lang.en import English
from tqdm import tqdm

from src.schema import Location, Post, User
from src.tasks.spacy import bytes_to_spacy
from src.utils import connect_to_mongo, get_nlp

from .filters import BaseFilter, DenylistFilter, LocationFilter
from .rankers import BaseRanker, FrequencyRanker

DENYLIST = {"china", "russia", "turkey", "op"}

# -- HELPER FUNCTIONS --


def get_user_spacy(user: User, nlp: English) -> List[English]:
    """Retrieves all of the spacy docs for a given user."""
    posts = Post.objects(user=user)
    return [bytes_to_spacy(p.spacy, nlp) for p in posts if p.spacy]


def get_ents(docs: List[English], entity_type: str) -> List[str]:
    """Return all entities in the given docs of the given type."""
    all_ents = it.chain(*[d.ents for d in docs])
    filt_ents = [e.text.lower() for e in all_ents if e.label_ == entity_type]
    return filt_ents

def rankings_averager(gpe: str, rankings: List[Dict[str, float]]) -> float:
    """Combine rankings for a given gpe by averaging each rankings respective score."""
    scores = [r[gpe] for r in rankings]
    return sum(scores) / len(scores)


def load_locations(fp: str) -> List[Dict[str, Optional[str]]]:
    """
    Retrieve US city/county/state locations from a csv filepath.
    """
    df = pd.read_csv(fp)

    return df.to_dict(orient="records")

    def string_or_none(maybe_string: Any) -> Optional[str]:
        return maybe_string if type(maybe_string) == str else None

    def row_to_loc(row: pd.Series) -> Location:
        return Location(
            neighborhood=string_or_none(row["neighborhood"]),
            city=string_or_none(row["city"]),
            county=string_or_none(row["county"]),
            state=string_or_none(row["state"]),
            country=string_or_none(row["country"])
        )

    return set([row_to_loc(row) for _, row in df.iterrows()])


def convert_entities_to_locations(entities: List[str], locations: pd.DataFrame) -> List[Location]:
    """
    Convert the given entities into locations.
    Then, combine them into as few locations as possible such that they are all real locations
    (as determined by the given set of locations)
    """

    columns = locations.to_dict(orient="list")
    records = locations.to_dict(orient="records")

    possible_locations = []

    for column in columns:


    for entity in entities:
        for column in columns:
            if entity in columns[column]:
                new_location = {column: entity}

                possible_locations.append(new_location)

    return []



# -- MODEL DECLARATION --


class V1:

    def __init__(
            self,
            filters: Set[BaseFilter],
            rankers: Set[BaseRanker],
            possible_locations: Set[Location],
            nlp: English
    ):
        self.filters = filters
        self.rankers = rankers
        self.possible_locations = possible_locations
        self.nlp = nlp

    def score_entities(self, user: User) -> Dict[str, float]:
        """Assign a likeliness score to each location entity in a user's posting history."""
        # extract user locations
        spacy_docs = get_user_spacy(user, self.nlp)
        gpes = get_ents(spacy_docs, "GPE")

        # apply filters
        possible_gpes = ft.reduce(lambda acc, f: f.filter(acc), self.filters, set(gpes))
        filtered_gpes = [gpe for gpe in gpes if gpe in possible_gpes]

        # rank gpes with different methods
        rankings = [r.rank(filtered_gpes) for r in self.rankers]

        # combine rankings
        scores = {gpe: rankings_averager(gpe, rankings) for gpe in possible_gpes}

        return scores

    def score_locations(self, entity_scores: Dict[str, float]) -> Dict[Location, float]:
        """
        Convert each location entity in a user's posting history to a real location.
        Then, assign a likeliness score to each location.
        """
        guessed_locations = []

        def locations_by_type(locations: Sequence[Location]):
            return {
                location_type: set([getattr(location, location_type)
                                    for location in locations
                                    if type(getattr(location, location_type)) == str])
                for location_type in Location._fields.keys()
            }


        # store each location type (e.g. city, state, etc.) mapped to the unique possibilities
        location_types = locations_by_type(self.possible_locations)

        # construct a new location for each entity, combining each with other plausible entities
        for i, location_type in enumerate(location_types.keys()):
            for entity in entity_scores.keys():

                # create a new location if the entity exists in the current type
                if entity in location_types[location_type]:
                    guessed_location = Location()
                    setattr(guessed_location, location_type, entity)

                    # iterate up through the hierarchy of location types,
                    # adding location elements if the exist
                    if i < (len(location_types) - 1):
                        types_to_check = list(location_types.keys())[i + 1:]
                        for new_type in types_to_check:
                            for new_entity in entity_scores.keys():
                                subset_locations = [l for l in self.possible_locations
                                                    if guessed_location.subset_of(l)]
                                subset_location_types = locations_by_type(subset_locations)
                                if new_entity in subset_location_types[new_type]:
                                    setattr(guessed_location, new_type, new_entity)

                    # store the constructed location
                    guessed_locations.append(guessed_location)

        # score each location
        location_scores = {
            location: sum(entity_scores[entity] for entity in list(location.to_mongo().values()))
            for location in guessed_locations
        }

        normalized_scores = softmax(list(location_scores.values())) if len(location_scores.values()) > 0 else []

        location_scores = {location: score for location, score in zip(location_scores.keys(), normalized_scores)}

        return location_scores

    def predict(self, user: User) -> Optional[Location]:
        """
        Compute likeliness scores for each entity in a user's posting history and
        package them into a single most likely location.
        """
        entity_scores = self.score_entities(user)

        max_entity = sorted(entity_scores.items(), key=lambda _: _[1], reverse=True)[0]

        # TODO: add step for forward/backward traversal to fill in other loc attrs of max entity
        max_loc = Location(city=max_entity)

        return max_loc


if __name__ == "__main__":

    print("Connecting to mongo .....")
    connect_to_mongo()

    # define filters/rankers
    locations = load_locations("data/locations/grouped-locations.csv")
    filters = {
        DenylistFilter(DENYLIST),
        LocationFilter(locations)
    }
    rankers = {FrequencyRanker()}
    nlp = get_nlp()

    # instantiate model
    model = V1(filters, rankers, locations, nlp)

    use_top = True
    print("Getting users .....")

    if use_top:
        # get the users with the most posts
        pipeline = [{"$sortByCount": "$user"}, {"$limit": 5}]
        res = Post.objects().aggregate(pipeline)
        ids = [str(r["_id"]) for r in res if r["_id"]]
        users = User.objects(id__in=ids)
    else:
        usernames = pd.read_csv("data/rand_user_200.csv", squeeze=True).tolist()
        users = User.objects(username__in=usernames).all()

    print("Making predictions .....")
    scores = [model.score_locations(user) for user in tqdm(users)]
    breakpoint()
