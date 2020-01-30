"""Tests for parsing data from Reddit."""
import os
import unittest
from typing import List

from constants import PROJ_DIR
from utils.functions import extract_files
from utils.post import Post

from .test_extract_praw import TestExtractPraw


class TestExtractFiles(TestExtractPraw):
    """
    Tests for the read_all_files method.

    This object inherits from the object for testing the extract_posts method.
    Thus, the same tests are run except the posts to be used are overriden here.
    """

    # file paths that test data are stored in
    threads_dir = os.path.join(PROJ_DIR, "sample_data", "threads")
    comments_dir = os.path.join(PROJ_DIR, "sample_data", "comments")

    def __get_posts(self) -> List[Post]:
        """Overwrite the method for retrieving posts to read from files."""
        threads = extract_files(self.threads_dir, True)
        comments = extract_files(self.comments_dir, False)
        return threads + comments


if __name__ == '__main__':
    unittest.main()
