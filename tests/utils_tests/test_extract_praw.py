"""Tests for parsing data from Reddit."""
import functools as ft
import unittest
from datetime import datetime
from typing import List

from constants import SUBR
from utils.functions import extract_praw
from utils.post import Post


class TestExtractPraw(unittest.TestCase):
    """Tests for the extract_posts method."""

    # arbitrary date to test with
    date = datetime(2019, 12, 20)

    def __get_posts(self) -> List[Post]:
        """Retrieve posts from Reddit."""
        return extract_praw(SUBR, self.date, 50)

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


if __name__ == '__main__':
    unittest.main()