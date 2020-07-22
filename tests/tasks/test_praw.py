"""Tests for parsing data from Reddit."""
import unittest
from datetime import datetime
from typing import List
from mongoengine import connect, disconnect

from src.tasks.praw import extract_praw
from src.utils import (
    Post,
    get_praw,
    get_psaw
)


class TestExtractPraw(unittest.TestCase):
    """Tests for the extract_posts method."""

    @classmethod
    def setUpClass(cls):
        connect('mongoenginetest', host='mongomock://localhost')

    @classmethod
    def tearDownClass(cls):
        disconnect()

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
        assert all([isinstance(p, Post) for p in posts])

    def test_at_least_one(self) -> None:
        """Test that at least post is extracted."""
        posts = self.__get_posts()
        assert len(posts) > 0
