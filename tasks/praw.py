"""Defines a luigi task to parse data from Praw."""

import json
from datetime import datetime
from typing import Any, Dict, List

import luigi
from luigi import LocalTarget, Task
from luigi.target import Target

from drug_pricing import SUB_LIMIT, SUBR
from utils.pipeline import extract_posts
from utils.post import Post


class ParsePraw(Task):
    """Parses data from the Praw api."""

    dates = luigi.DateIntervalParameter()

    out_fn = "data/praw_posts.json"

    def output(self) -> Target:
        """Define the Target to be written to."""
        return LocalTarget(self.out_fn)

    def run(self) -> None:
        """Extract newest posts and write to json file."""
        run_date: datetime = datetime.utcnow()
        new_subs: List[Post] = extract_posts(SUBR, run_date, SUB_LIMIT)
        serial_subs: List[Dict[str, Any]] = [sub.to_dict() for sub in new_subs]
        outfile = self.output().open("w")
        outdata = {"posts": serial_subs}
        json.dump(outdata, outfile)
        outfile.close()
