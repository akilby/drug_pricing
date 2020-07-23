from src.utils import last_date
from src.schema import Post
from mongoengine import connect, disconnect
import unittest
from datetime import datetime


class TestUtils(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        connect('mongoenginetest', host='mongomock://localhost')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    def test_last_date(self) -> None:
        """Test that the last date of a post is properly retrieved."""
        p1 = Post(pid="123", datetime=datetime(2020, 7, 10), subreddit="subr")
        p2 = Post(pid="456", datetime=datetime(2019, 7, 10), subreddit="subr")
        p3 = Post(pid="789", datetime=datetime(2020, 7, 16))
        p4 = Post(pid="abc", datetime=datetime(2020, 6, 11), subreddit="subr_2")

        p1.save()
        p2.save()
        p3.save()
        p4.save()

        self.assertEqual(last_date(), datetime(2020, 7, 16))
        self.assertEqual(last_date(subreddit="subr"), datetime(2020, 7, 10))
        self.assertEqual(last_date(subreddit="subr_2"), datetime(2020, 6, 11))
