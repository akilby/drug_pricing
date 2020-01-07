"""Defines tasks to add data to a MongoDB client."""

import json

import luigi
from luigi import Task

from drug_pricing import MONGO_COLL

from .files import ParseFiles
from .praw import ParsePraw


class PrawToMongo(Task):
    """Define a task that adds data loaded from praw to a mongo collection."""

    dates = luigi.DateIntervalParameter()

    collection = MONGO_COLL

    def requires(self) -> Task:
        """Return the praw parsing dependency of this task."""
        return ParsePraw(self.dates)

    def run(self) -> None:
        """Add data from praw parsing to the this mongo collection."""
        infile = self.input().open("r")
        posts = json.load(infile)["posts"]
        self.collection.insert_many(posts)


class FilesToMongo(PrawToMongo):
    """Define a task that adds to mongo from parsed file data instead of praw."""

    def requires(self) -> Task:
        """Return the file parsing dependency of this task."""
        return ParseFiles(self.dates)
