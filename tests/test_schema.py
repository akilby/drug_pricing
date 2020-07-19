import unittest
from mongoengine import connect, disconnect

from src.schema import SubmissionPost, Post


class TestToMongo(unittest.TestCase):
    """Tests for the to_mongo method."""

    # instantiate sample posts
    p1 = SubmissionPost(pid="abc", text="sample text")
    p2 = SubmissionPost(pid="abc", text="sample text")

    @classmethod
    def setUpClass(cls):
        connect('mongoenginetest', host='mongomock://localhost')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    def test_insert(self) -> None:
        """Test that insertion works correctly."""
        # remove p1 if in mongo coll
        Post.objects(pid=self.p1.pid).delete()

        self.p1.save()

        # assert is exists
        self.assertEqual(Post.objects(pid=self.p1.pid), 1)

        # count total in mongo
        pre_count = Post.objects.count()

        # insert p2 to test coll
        self.p2.save()

        # assert count is count1
        post_count = Post.objects.count()
        self.assertEqual(pre_count, post_count)

