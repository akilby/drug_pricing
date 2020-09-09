"""Schemas for MongoDB."""
from mongoengine import (BinaryField, DateTimeField, Document,
                         EmbeddedDocument, EmbeddedDocumentField,
                         EmbeddedDocumentListField, IntField, ReferenceField,
                         StringField)


class Location(EmbeddedDocument):
    """A representation of an incorporated location."""

    name = StringField(required=True)
    meta = {"allow_inheritance": True}


class State(Location):
    pass


class County(Location):
    state = ReferenceField("State")


class City(Location):
    county = ReferenceField("County")


class Neighborhood(Location):
    city = ReferenceField("City")


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
        "indexes": ["$text", "-datetime", {"fields": ["pid"], "unique": True}],
    }


class SubmissionPost(Post):
    """A Reddit Submission."""

    url = StringField()
    title = StringField()
    num_comments = IntField()


class CommentPost(Post):
    """A Reddit Comment."""

    parent_id = StringField()
