"""Tests for parsing data from Reddit."""
import functools as ft
import unittest
from datetime import datetime

from constants import SUBR
from utils.functions import extract_posts
from utils.post import Post


class TestExtractPosts(unittest.TestCase):
    """Tests for the extract_posts method."""

    # arbitrary date to test with
    date = datetime(2019, 12, 20)

    # posts extracted from this date
    posts = extract_posts(SUBR, date, 50)

    def test_is_post(self):
        """Test that the objects returned are all post objects."""
        are_posts = ft.reduce(
            lambda acc, p: acc and isinstance(p, Post), self.posts, True)
        self.assertTrue(are_posts)

    def test_at_least_one(self):
        """Test that at least post is extracted."""
        self.assertTrue(len(self.posts) > 0)


if __name__ == '__main__':
    unittest.main()
