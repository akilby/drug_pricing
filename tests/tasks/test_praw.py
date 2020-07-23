"""Tests for parsing data from Reddit."""
import unittest
from datetime import datetime
from mongoengine import connect, disconnect


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
