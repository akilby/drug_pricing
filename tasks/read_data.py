"""Defines luigi tasks to read data from a source."""
import json
import os
from datetime import datetime
from typing import Any, Dict, List

import luigi
from luigi import LocalTarget, Task
from luigi.target import Target

from drug_pricing import COMM_DIR, PROJ_DIR, SUB_DIR, SUB_LIMIT, SUBR
from utils.functions import extract_posts, read_all_files
from utils.post import Post


class ParsePraw(Task):
    """Parses data from the Praw api."""

    # assign date to run
    dates = luigi.DateIntervalParameter()
    run_date: datetime = datetime.utcnow()

    # define the name of the data file to be cached
    out_fn = "~cached_data.json"

    def __retrieve_posts(self) -> List[Post]:
        """Retrieve posts from praw."""
        return extract_posts(SUBR, self.run_date, SUB_LIMIT)

    def output(self) -> Target:
        """Define the Target to be written to."""
        return LocalTarget(os.path.join(PROJ_DIR, self.out_fn))

    def run(self) -> None:
        """Extract newest posts and write to json file."""
        # extract most recent r/opiates posts
        # TODO: check if len(new_subs) == 1000.  If T, query again.
        posts: List[Post] = self.__retrieve_posts()

        # serialize post objects in dictionary form
        serial_posts: List[Dict[str, Any]] = [post.to_dict() for post in posts]

        # create a data cache file and write extracted data to it
        outfile = self.output().open("w+")
        outdata = {"posts": serial_posts}
        json.dump(outdata, outfile)
        outfile.close()


class ParseFiles(ParsePraw):
    """Parses reddit data stored in local CSV files."""

    def __retrieve_posts(self) -> List[Post]:
        """
        Retrieve comments and submissions stored in files.

        Note that this provides an abstraction layer by overriding the praw retrieve_posts method.
        """
        # retrieve submissions
        subs: List[Post] = read_all_files(SUB_DIR, True)

        # retrieve comments
        comms: List[Post] = read_all_files(COMM_DIR, False)

        # return combined submissions and comments
        return subs + comms
