import itertools as it
from typing import List

from spacy.lang.en import English
import geocoder

from src.schema import Post, User
from src.tasks.spacy import bytes_to_spacy
from src.utils import GEONAMES_KEY, MAPBOX_KEY, GOOGLE_KEY


DENYLIST = {"china", "russia", "turkey", "op"}


def get_user_spacy(user: User, nlp: English) -> List[English]:
    """Retrieves all of the spacy docs for a given user."""
    posts = Post.objects(user=user)
    return [bytes_to_spacy(p.spacy, nlp) for p in posts if p.spacy]


def get_ents(docs: List[English], entity_type: str) -> List[str]:
    """Return all entities in the given docs of the given type."""
    all_ents = it.chain(*[d.ents for d in docs])
    filt_ents = [e.text.lower() for e in all_ents if e.label_ == entity_type]
    return filt_ents


def forward_geocode(location: str,
                    service="mapbox",
                    session=None) -> List[geocoder.base.OneResult]:
    """Extract the latitude/longitude from a given location."""
    if service == "geonames":
        geocodes = geocoder.geonames(location, key="cccdenhart", session=session)
    elif service == "mapbox":
        geocodes = geocoder.mapbox(location, key=MAPBOX_KEY, session=session)
    elif service == "google":
        geocodes = geocoder.google(location, key=GOOGLE_KEY, session=session)
    else:
        raise Exception("Unsupported geocoding service provided.")
    return list(geocodes)


def reverse_geocode(lat: float,
                    lng: float,
                    service="mapbox",
                    session=None) -> geocoder.base.OneResult:
    """Return a geocode location given a coordinate pair."""
    coords = (lat, lng)
    if service == "mapbox":
        location = geocoder.mapbox(coords, key=MAPBOX_KEY, method="reverse")
    else:
        raise Exception("Unsupported geocoding service provided.")

    if hasattr(location, "__getitem__") and len(location) > 0:
        return location[0]
    else:
        return location
