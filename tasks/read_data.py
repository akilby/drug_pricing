"""Defines luigi tasks to read data from a source."""
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import luigi
from luigi import LocalTarget
from luigi.target import Target

from constants import COMM_COLNAMES, PROJ_DIR, SUB_COLNAMES
from utils.functions import extract_csv, extract_praw
from utils.post import Post


class ParsePraw(luigi.Task):
    """Parses data from the Praw api."""

    # initialize user input
    # TODO: change to luigi params
    subr: str = ""
    run_date: datetime = datetime.utcnow()
    start_time: str = ""
    end_time: str = ""
    limit: Optional[int] = None

    # define class constants
    data_fn = "~cached_data.json"
    out_dir = PROJ_DIR

    def __retrieve_posts(self) -> List[Post]:
        """Retrieve posts from praw."""
        return extract_praw(self.subr, self.start_time, self.limit,
                            self.end_time)

    def output(self) -> Target:
        """Define the Target to be written to."""
        return LocalTarget(os.path.join(self.out_dir, self.data_fn))

    def run(self) -> None:
        """Extract newest posts and write to json file."""
        # extract most recent r/opiates posts
        posts: List[Post] = self.__retrieve_posts()

        # serialize post objects in dictionary form
        serial_posts: List[Dict[str, Any]] = [post.to_dict() for post in posts]

        # create a data cache file and write extracted data to it
        outfile = self.output().open("w+")
        outdata = {"posts": serial_posts}
        json.dump(outdata, outfile)
        outfile.close()


class ParseCsv(ParsePraw):
    """Parses reddit data stored in local CSV files."""

    # initialize user input
    # TODO: change this to luigi param
    filepath: str = ""
    is_sub: bool = True

    # define class constants
    sub_cols: List[str] = SUB_COLNAMES
    comm_cols: List[str] = COMM_COLNAMES

    def __retrieve_posts(self) -> List[Post]:
        """
        Retrieve comments and submissions stored in files.

        Note that this overrides the ParsePraw retrieve_posts method.
        """
        # determine column names to use for csv parsing
        cols: List[str] = self.sub_cols if self.is_sub else self.comm_cols

        # retrieve submissions
        subs: List[Post] = extract_csv(self.filepath, cols)

        # retrieve comments
        comms: List[Post] = extract_csv(self.filepath, cols)

        # return combined submissions and comments
        return subs + comms
