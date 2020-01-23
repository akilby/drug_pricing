"""Tests for parsing data from Reddit."""
import os
import unittest

from constants import PROJ_DIR
from utils.functions import read_all_files

from .test_extract_posts import TestExtractPosts


class TestReadAllFiles(TestExtractPosts):
    """
    Tests for the read_all_files method.

    This object inherits from the object for testing the extract_posts method.
    Thus, the same tests are run except the posts to be used are overriden here.
    """

    # file paths that test data are stored in
    threads_dir = os.path.join(PROJ_DIR, "sample_data", "threads")
    comments_dir = os.path.join(PROJ_DIR, "sample_data", "comments")

    # retrieved comments/threads from these file paths
    threads = read_all_files(threads_dir, True)
    comments = read_all_files(comments_dir, False)
    posts = threads + comments


if __name__ == '__main__':
    unittest.main()
