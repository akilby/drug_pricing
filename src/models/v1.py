import dataclasses
import functools as ft
import itertools as it
from collections import Counter
from typing import Callable, Dict, List, Optional, Sequence, Set

import pandas as pd
from scipy.special import softmax
from spacy.lang.en import English

from src.schema import Post
from src.tasks.spacy import literal_bytes_to_spacy
from src.utils import Location, connect_to_mongo

DENYLIST = {"china", "russia", "turkey", "op"}


def get_user_spacy(username: str) -> List[English]:
    """Retrieves all of the spacy docs for a given user."""
    posts = Post.objects(username=username)
    return [literal_bytes_to_spacy(p.spacy) for p in posts if p.spacy]


def get_ents(docs: List[English], entity_type: str) -> List[str]:
    """Return all entities in the given docs of the given type."""
    all_ents = it.chain(*[d.ents for d in docs])
    filt_ents = [e.text.lower() for e in all_ents if e.label_ == entity_type]
    return filt_ents


def filter_by_denylist(gpes: Set[str], denylist: Set[str]) -> Set[str]:
    """Remove gpes in the denylist."""
    return gpes - denylist


def filter_by_location(gpes: Set[str], locations: Set[Location]) -> Set[str]:
    """Remove any gpes that are not an incorporated location."""
    return ft.reduce(
        lambda acc, loc: acc | (gpes & {dataclasses.asdict(loc).values()}), locations, set()
    )


def rank_by_frequency(gpes: Sequence[str]) -> Dict[str, float]:
    """Rank each gpe by frequency of occurrence."""
    counts = Counter(gpes)
    keys = list(counts.keys())
    normalized_counts = softmax(list(counts.values()))
    return {k: v for k, v in zip(keys, normalized_counts)}


def rankings_averager(gpe: str, rankings: List[Dict[str, float]]) -> float:
    """Combine rankings for a given gpe by averaging each rankings respective score."""
    scores = [r[gpe] for r in rankings]
    return sum(scores) / len(scores)


def score_user(
    username: str,
    filters: List[Callable[[Set[str]], Set[str]]],
    rankers: Sequence[Callable[[Sequence[str]], Dict[str, float]]],
) -> Dict[str, float]:
    """
    Strategy for ranking possible user locations.

    :param filters: functions that remove certain entities
    :param rankers: functions that generate a likeliness score for each entity.
    :param ranking_combiner: a function that combines the resulting rankings
                             into a score for each entity.
    """
    # extract user locations
    spacy_docs = get_user_spacy(username)
    gpes = get_ents(spacy_docs, "GPE")

    # apply filters
    possible_gpes = ft.reduce(lambda acc, f: f(acc), filters, set(gpes))
    filtered_gpes = [gpe for gpe in gpes if gpe in possible_gpes]

    # rank gpes with different methods
    rankings = [r(filtered_gpes) for r in rankers]

    # combine rankings
    scores = {gpe: rankings_averager(gpe, rankings) for gpe in possible_gpes}

    return scores


def _predict(
    username: str,
    filters: List[Callable[[Set[str]], Set[str]]],
    rankers: Sequence[Callable[[Sequence[str]], Dict[str, float]]],
) -> Optional[Location]:
    """
    Compute likeliness scores for each entity in a user's posting history and
    package them into a single most likely location.
    """
    entity_scores = score_user(username, filters, rankers)

    if len(entity_scores) == 0:
        return None

    max_entity = sorted(entity_scores.items(), key=lambda _: _[1], reverse=True)[0]
    max_loc = Location(city=max_entity)

    return max_loc


def get_locations(fp: str) -> Set[Location]:
    """
    Retrieve US city/county/state locations from a csv filepath.
    """
    df = pd.read_csv(fp, sep="|")

    def row_to_loc(row: pd.Series) -> Location:
        return Location(
            city=row["City"],
            county=row["County"],
            state=row["State full"],
            state_short=row["State short"],
        )

    return set([row_to_loc(row) for _, row in df.iterrows()])


def predict(usernames: List[str]):
    """Predict a collection of usernames."""

    locations = get_locations("data/cities-states.csv")

    filters = [
        lambda _: filter_by_denylist(_, DENYLIST),
        lambda _: filter_by_location(_, locations),
    ]

    rankers = [rank_by_frequency]

    return [score_user(u, filters, rankers) for u in usernames]


if __name__ == "__main__":
    connect_to_mongo()
    users = pd.read_csv("data/rand_user_200.csv", squeeze=True).tolist()
    preds = predict(users)
    breakpoint()
