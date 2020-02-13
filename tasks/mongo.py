"""Defines tasks to add data to a MongoDB client."""
import json
import os
from datetime import datetime

import luigi

from constants import COLL
from tasks.read_data import ParseCsv, ParsePraw


class PrawToMongo(luigi.Task):
    """Define a task that adds data loaded from praw to a mongo collection."""

    # assign the data to run this task
    # dates = luigi.DateIntervalParameter()
    dates: datetime = datetime.utcnow()

    # assign the mongo collection to add data to
    collection = COLL

    def requires(self) -> luigi.Task:
        """Return the praw parsing dependency of this task."""
        return ParsePraw(self.dates)

    def run(self) -> None:
        """Add data from praw parsing to the this mongo collection."""
        # load data
        data_fn = self.input()
        posts = json.load(data_fn.open("r"))["posts"]

        # add data to mongo
        self.collection.insert_many(posts)

        # remove the cached data file
        os.remove(data_fn)


class CsvToMongo(PrawToMongo):
    """Define a task that adds to mongo from parsed file data instead of praw."""

    def requires(self) -> luigi.Task:
        """Return the file parsing dependency of this task."""
        return ParseCsv(self.dates)
