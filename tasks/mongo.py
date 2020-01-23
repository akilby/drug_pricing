"""Defines tasks to add data to a MongoDB client."""
import json
import os

import luigi
from luigi import Task

from constants import COLL

from .read_data import ParseFiles, ParsePraw


class PrawToMongo(Task):
    """Define a task that adds data loaded from praw to a mongo collection."""

    # assign the data to run this task
    dates = luigi.DateIntervalParameter()

    # assign the mongo collection to add data to
    collection = COLL

    def requires(self) -> Task:
        """Return the praw parsing dependency of this task."""
        return ParsePraw(self.dates)

    def run(self) -> None:
        """Add data from praw parsing to the this mongo collection."""
        # load data
        data_fn = self.input()
        posts = json.load(data_fn.open("r"))["posts"]

        # add data to mongo
        # TODO: prevent duplicate ids from being added
        self.collection.insert_many(posts)

        # remove the cached data file
        os.remove(data_fn)


class FilesToMongo(PrawToMongo):
    """Define a task that adds to mongo from parsed file data instead of praw."""

    def requires(self) -> Task:
        """Return the file parsing dependency of this task."""
        return ParseFiles(self.dates)
