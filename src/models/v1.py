import itertools as it
import functools as ft
from collections import Counter
from typing import List, Dict, Set, Callable, Optional

from spacy.lang.en import English
import dataclasses

from src.utils import Location
from src.schema import Post
from src.tasks.spacy import literal_bytes_to_spacy


BLOCKLIST = ["china", "russia", "turkey", "op"]


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
    return ft.reduce(lambda acc, loc: acc | (gpes & {dataclasses.asdict(loc).values()}),
                     locations,
                     set())


def rank_by_frequency(gpes: List[str]) -> Dict[str, float]:
    """Rank each gpe by frequency of occurrence."""
    return dict(Counter(gpes))


def rankings_averager(gpe: str, rankings: List[Dict[str, float]]) -> float:
    """Combine rankings for a given gpe by averaging each rankings respective score."""
    scores = [r[gpe] for r in rankings]
    return sum(scores) / len(scores)


def score_user(
        username: str,
        filters: List[Callable[[Set[str]], Set[str]]],
        rankers: List[Callable[[List[str]], Dict[str, float]]],
        ranking_combiner: Callable[[str, List[Dict[str, float]]], float]
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

    # TODO: transform them into locations

    # rank gpes with different methods
    rankings = [r(filtered_gpes) for r in rankers]

    # combine rankings
    scores = {gpe: ranking_combiner(gpe, rankings) for gpe in possible_gpes}

    return scores


def _predict(
        username: str,
        filters: List[Callable[[Set[str]], Set[str]]],
        rankers: List[Callable[[List[str]], Dict[str, float]]],
        ranking_combiner: Callable[[str, List[Dict[str, float]]], float]
) -> Optional[Location]:
    """
    Compute likeliness scores for each entity in a user's posting history and
    package them into a single most likely location.
    """
    entity_scores = score_user(username, filters, rankers, ranking_combiner)

    if len(entity_scores) == 0:
        return None

    max_entity = sorted(entity_scores.items(), key=lambda _: _[1], reverse=True)[0]
    max_loc = Location(city=max_entity)

    return max_loc


def predict(usernames: List[str]):
    """"""
