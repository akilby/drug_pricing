"""Tests for parsing data from Reddit."""
import functools as ft
import os
import unittest
from datetime import datetime
from typing import List

from pipeline import Post, Sub, extract_csv, extract_praw, to_mongo
from utils import (COMM_COLNAMES, DB_NAME, PROJ_DIR, SUB_COLNAMES,
                   TEST_COLL_NAME, dt_to_utc, get_mongo, get_praw, get_psaw,
                   utc_to_dt)


class TestExtractPraw(unittest.TestCase):
    """Tests for the extract_posts method."""

    # arbitrary date to test with
    date = datetime(2019, 12, 20)
    subreddit = "opiates"
    praw = get_praw()
    psaw = get_psaw(praw)

    def __get_posts(self) -> List[Post]:
        """Retrieve posts from Reddit."""
        return extract_praw(self.psaw, self.subreddit, self.date, limit=50)

    def test_is_post(self) -> None:
        """Test that the objects returned are all post objects."""
        posts = self.__get_posts()
        are_posts = ft.reduce(lambda acc, p: acc and isinstance(p, Post),
                              posts, True)
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
    data_dir = os.path.join(PROJ_DIR, "data")

    def __get_posts(self) -> List[Post]:
        """Overwrite the method for retrieving posts to read from files."""
        thread_fp = os.path.join(self.data_dir, "all_subms.csv")
        comm_fp = os.path.join(self.data_dir, "all_comments.csv")
        threads = extract_csv(thread_fp, SUB_COLNAMES)
        comments = extract_csv(comm_fp, COMM_COLNAMES)
        return threads + comments


class TestToMongo(unittest.TestCase):
    """Tests for the to_mongo method."""

    # instantiate sample posts
    p1 = Sub(pid="abc", text="sample text")
    p2 = Sub(pid="abc", text="sample text")

    # initialize test collection connection
    coll = get_mongo()[DB_NAME][TEST_COLL_NAME]

    def test_insert(self) -> None:
        """Test that insertion works correctly."""
        # define query
        query = {"hash": hash(self.p1)}

        # remove p1 if in mongo coll
        if self.coll.count_documents(query) > 0:
            self.coll.delete_many(query)

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
            self.coll.delete_many(query1)

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
