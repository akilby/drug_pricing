"""Schemas for MongoDB."""
import itertools as it
from typing import List, Any

from mongoengine import (BinaryField, DateTimeField, Document,
                         EmbeddedDocument, EmbeddedDocumentField,
                         EmbeddedDocumentListField, IntField, ReferenceField,
                         StringField, FloatField)


class Location(EmbeddedDocument):
    """A representation of an incorporated location."""

    lat = FloatField()
    lng = FloatField()
    neighborhood = StringField()
    city = StringField()
    county = StringField()
    metro = StringField()
    state_full = StringField()
    state = StringField()
    country = StringField()
    population = IntField()

    def __repr__(self) -> str:
        return ", ".join([str(v) for v in self.to_mongo().values()])

    def __hash__(self) -> int:
        return hash(self.neighborhood) + \
            10 * hash(self.city) + \
            100 * hash(self.county) + \
            1000 * hash(self.state) + \
            10000 * hash(self.country)

    def __eq__(self, other: Any) -> bool:
        return self.neighborhood == other.neighborhood and \
               self.city == other.city and \
               self.state == other.state and \
               self.country == other.country

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def subset_of(self, other: "Location") -> bool:
        """Check if this location is a subset of the given location."""
        self_map = self.to_mongo()
        other_map = other.to_mongo()
        return set(self_map.keys()).issubset(set(other_map.keys())) and \
            all([self_map[field] == other_map[field] for field in self_map.keys()])

    def match_entity(self, entity: str) -> List["Location"]:
        """Return all possible sublocations that this entity could be of this location."""
        self_map = self.to_mongo()

        matching_fields = [k for k, v in self_map.items() if v == entity]

        field_combinations = it.chain(
            *map(lambda i: it.combinations(matching_fields, i),
                 range(1, 1 + len(matching_fields))))

        possible_locations = [
            Location(**{
                k: self_map[k]
                for k in set(self_map.keys()) & set(field_combo)
            }) for field_combo in field_combinations
        ]

        return possible_locations


class DateRangeLocation(EmbeddedDocument):
    """A location that can exist over a datetime range."""

    location = EmbeddedDocumentField(Location, required=True)
    start_datetime = DateTimeField()
    end_datetime = DateTimeField()


class User(Document):
    """A reddit user."""

    username = StringField(required=True)
    locations = EmbeddedDocumentListField(DateRangeLocation)
    meta = {"indexes": [{"fields": ["username"], "unique": True}]}

    def __str__(self) -> str:
        return self.username


class Post(Document):
    """An abstraction over Reddit Submission and Comments."""

    pid = StringField(required=True)
    text = StringField()
    user = ReferenceField(User)
    datetime = DateTimeField()
    subreddit = StringField()
    spacy = BinaryField()
    meta = {
        "allow_inheritance": True,
        "indexes": ["user", "$text", "-datetime", {
            "fields": ["pid"],
            "unique": True
        }],
    }


class SubmissionPost(Post):
    """A Reddit Submission."""

    url = StringField()
    title = StringField()
    num_comments = IntField()


class CommentPost(Post):
    """A Reddit Comment."""

    parent_id = StringField()
