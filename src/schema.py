"""Schemas for MongoDB."""
from mongoengine import Document, StringField, DateTimeField, IntField


class Post(Document):
    """An abstraction over Reddit Submission and Comments."""
    pid = StringField()
    text = StringField()
    username = StringField()
    datetime = DateTimeField()
    subreddit = StringField()
    spacy = StringField()
    meta = {
        'allow_inheritance': True,
        'indexes': [
            '$text',
            '-datetime',
            {
                'fields': ['pid'],
                'unique': True
            }
        ]
    }


class SubmissionPost(Post):
    """A Reddit Submission."""
    url = StringField()
    title = StringField()
    num_comments = IntField()


class CommentPost(Post):
    """A Reddit Comment."""
    parent_id = StringField()

