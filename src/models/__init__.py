import itertools as it
from typing import List, Union, Dict

from spacy.lang.en import English
import geocoder
import requests

from src.schema import Post, User
from src.tasks.spacy import bytes_to_spacy
from src.utils import GEONAMES_KEY, MAPBOX_KEY, GOOGLE_KEY

MAPBOX_KEY = 'fake'  # "pk.eyJ1IjoiY2NjZGVuaGFydCIsImEiOiJjamtzdjNuNHAyMjB4M3B0ZHVoY3l2MndtIn0.jkJIFGPTN7oSkQlHi0xtow"
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
					session=None,
					as_list=True) -> List[geocoder.base.OneResult]:
	"""Extract the latitude/longitude from a given location."""
	if service == "geonames":
		geocodes = geocoder.geonames(location, key="cccdenhart", fuzzy=0.8, session=session)
	elif service == "mapbox":
		geocodes = geocoder.mapbox(location, key=MAPBOX_KEY, session=session)
	elif service == "google":
		geocodes = geocoder.google(location, key=GOOGLE_KEY, session=session)
	else:
		raise Exception("Unsupported geocoding service provided.")
	if as_list:
		return list(geocodes)
	return geocodes


def geonames_reverse(lat: float, lng: float):
	base_url = "http://api.geonames.org/findNearbyJSON"
	full_url = base_url + "?lat=" + str(lat) + "&lng=" + str(lng) + "&username=cccdenhart"
	return requests.get(full_url).json()


def reverse_geocode(lat: float,
					lng: float,
					service="mapbox",
					session=None) -> Union[geocoder.base.OneResult, Dict]:
	"""Return a geocode location given a coordinate pair."""
	coords = (lat, lng)
	if service == "mapbox":
		location = geocoder.mapbox(coords, key=MAPBOX_KEY, method="reverse")
	if service == "geonames":
		location = geonames_reverse(lat, lng)
	else:
		raise Exception("Unsupported geocoding service provided.")

	if hasattr(location, "__getitem__") and len(location) > 0:
		if service == "mapbox":
			return location[0]
		else:
			return location["geonames"][0]
	else:
		return location
