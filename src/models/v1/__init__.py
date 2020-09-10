import functools as ft
import itertools as it
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Set

import pandas as pd
from spacy.lang.en import English
from tqdm import tqdm

from src.schema import Post, User
from src.tasks.spacy import bytes_to_spacy
from src.utils import Location, connect_to_mongo, get_nlp

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


def get_locations(fp: str) -> Set[Location]:
        """
        Retrieve US city/county/state locations from a csv filepath.
        """
        df = pd.read_csv(fp, sep="|")

        def row_to_loc(row: pd.Series) -> Location:
            return Location(
                city=row["City"].lower() if type(row["City"]) == str else None,
                county=row["County"].lower() if type(row["County"]) == str else None,
                state=row["State full"].lower() if type(row["State full"]) == str else None,
                state_short=row["State short"].lower() if type(row["State short"]) == str else None,
            )

        return set([row_to_loc(row) for _, row in df.iterrows()])

# -- MODEL DECLARATION --


@dataclass
class V1:

    filters: Set[BaseFilter]
    rankers: Set[BaseRanker]
    rankings_combiner: Callable[[str, List[Dict[str, float]]], float] = rankings_averager
    nlp: English = get_nlp()

    def score_user(self, user: User) -> Dict[str, float]:
        """
        Strategy for ranking possible user locations.

        :param filters: functions that remove certain entities
        :param rankers: functions that generate a likeliness score for each entity.
        """
        # extract user locations
        spacy_docs = get_user_spacy(user, self.nlp)
        gpes = get_ents(spacy_docs, "GPE")

        # apply filters
        possible_gpes = ft.reduce(lambda acc, f: f.filter(acc), self.filters, set(gpes))
        filtered_gpes = [gpe for gpe in gpes if gpe in possible_gpes]

        # rank gpes with different methods
        rankings = [r.rank(filtered_gpes) for r in self.rankers]

        # combine rankings
        scores = {gpe: self.rankings_combiner(gpe, rankings) for gpe in possible_gpes}

        return scores


    def predict(self, user: User) -> Optional[Location]:
        """
        Compute likeliness scores for each entity in a user's posting history and
        package them into a single most likely location.
        """
        entity_scores = self.score_user(user)

        max_entity = sorted(entity_scores.items(), key=lambda _: _[1], reverse=True)[0]

        # TODO: add step for forward/backward traversal to fill in other loc attrs of max entity
        max_loc = Location(city=max_entity)

        return max_loc


if __name__ == "__main__":

    print("Connecting to mongo .....")
    connect_to_mongo()

    # define filters/rankers
    locations = get_locations("data/cities-states.csv")
    filters = {
        DenylistFilter(DENYLIST),
        LocationFilter(locations)
    }
    rankers = {FrequencyRanker()}

    # instantiate model
    model = V1(filters, rankers)

    print("Getting users .....")
    usernames = pd.read_csv("data/rand_user_200.csv", squeeze=True).tolist()
    users = User.objects(username__in=usernames).all()

    # get the users with the most posts
    """
    pipeline = [{"$sortByCount": "$user"}, {"$limit": 15}]
    res = Post.objects().aggregate(pipeline)
    ids = [str(r["_id"]) for r in res if r["_id"]]
    users = User.objects(id__in=ids)
    """

    print("Making predictions .....")
    scores = [model.score_user(user) for user in users]
    breakpoint()
