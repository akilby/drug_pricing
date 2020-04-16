"""Tests for parsing data from Reddit."""
import os
import unittest
from typing import List
from datetime import datetime
import functools as ft

from utils import PROJ_DIR, SUBR, DB
from scripts.pipeline import Post, extract_csv, extract_praw, to_mongo, Sub


class TestExtractPraw(unittest.TestCase):
    """Tests for the extract_posts method."""

    # arbitrary date to test with
    date = datetime(2019, 12, 20)

    def __get_posts(self) -> List[Post]:
        """Retrieve posts from Reddit."""
        return extract_praw(SUBR, self.date, limit=50)

    def test_is_post(self) -> None:
        """Test that the objects returned are all post objects."""
        posts = self.__get_posts()
        are_posts = ft.reduce(
            lambda acc, p: acc and isinstance(p, Post), posts, True)
        self.assertTrue(are_posts)

    def test_at_least_one(self) -> None:
        """Test that at least post is extracted."""
        posts = self.__get_posts()
        self.assertTrue(len(posts) > 0)


class TestExtractCsv(TestExtractPraw):
    """
    Tests for the read_all_files method.

    This object inherits from the object for testing the extract_posts method.
    Thus, the same tests are run except posts to be used are overriden here.
    """

    # file paths that test data are stored in
    threads_dir = os.path.join(PROJ_DIR, "sample_data", "threads")
    comments_dir = os.path.join(PROJ_DIR, "sample_data", "comments")

    def __get_posts(self) -> List[Post]:
        """Overwrite the method for retrieving posts to read from files."""
        threads = extract_csv(self.threads_dir, True)
        comments = extract_csv(self.comments_dir, False)
        return threads + comments


class TestToMongo(unittest.TestCase):
    """Tests for the to_mongo method."""

    # instantiate sample posts
    p1 = Sub(pid="abc", text="sample text")
    p2 = Sub(pid="abc", text="sample text")

    # initialize test collection connection
    coll = DB["test"]

    def test_insert(self) -> None:
        """Test that insertion works correctly."""
        # define query
        query = {"hash": hash(self.p1)}

        # remove p1 if in mongo coll
        if self.coll.count_documents(query) > 0:
            self.coll.remove(query)

        # insert p1
        to_mongo(self.coll, [self.p1])

        # assert is exists
        self.assertEqual(self.coll.count_documents(query), 1)

    def test_no_dup(self) -> None:
        """Test that identical posts are not added to mongo."""
        # define query
        query1 = {"hash": hash(self.p1)}

        # remove p1 if in mongo coll
        if self.coll.count_documents(query1) > 0:
            self.coll.remove(query1)

        # insert p1 to test coll
        to_mongo(self.coll, [self.p1])

        # count total in mongo
        count1 = self.coll.count_documents({})

        # insert p2 to test coll
        to_mongo(self.coll, [self.p2])

        # assert count is count1
        count2 = self.coll.count_documents({})
        self.assertEqual(count1, count2)


if __name__ == '__main__':
    unittest.main()
