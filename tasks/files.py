"""Defines a luigi task that compiles a collection of files into posts."""

import luigi
from luigi import Task
from luigi.target import Target


class ParseFiles(Task):
    """Parses reddit data stored in CSV files."""

    dates = luigi.DateIntervalParameter()

    def output(self) -> Target:
        """Template."""

    def run(self) -> None:
        """Template."""
