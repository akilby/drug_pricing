"""Tests for mongo operations."""
import unittest
import mongoengine
from mongoengine import connect, disconnect
import spacy

from src.schema import SubmissionPost, Post
from src.tasks.spacy import add_spacy_to_mongo


class TestToMongo(unittest.TestCase):
    """Tests for the to_mongo method."""

    # instantiate sample posts
    p1 = SubmissionPost(pid="abc", text="sample text")
    p2 = SubmissionPost(pid="abc", text="sample text 2")

    @classmethod
    def setUpClass(cls):
        connect('mongoenginetest', host='mongomock://localhost')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    def test_insert(self) -> None:
        """Test that insertion works correctly."""
        # check that standard insertion works
        self.p1.save()
        self.assertEqual(Post.objects(pid=self.p1.pid).count(), 1)

        # check that duplicate post cannot be added
        pre_count = Post.objects.count()
        with self.assertRaises(mongoengine.errors.NotUniqueError):
            self.p2.save()

        # check that duplicate post was in fact not added
        post_count = Post.objects.count()
        self.assertEqual(pre_count, post_count)
        self.assertEqual(Post.objects(pid=self.p1.pid).first().text, self.p1.text)

    def test_update(self) -> None:
        """Test that updates work properly."""
        # check that standard update works
        self.p1.save()

        test_post = Post.objects(pid=self.p1.pid).first()
        self.assertEqual(test_post.text, self.p1.text)

        new_text = "updated text"
        test_post.text = new_text
        test_post.save()

        updated_post = Post.objects(pid=self.p1.pid).first()
        self.assertEqual(updated_post.text, new_text)

        # check that spacy updates specifically work
        nlp = spacy.load("en_core_web_sm")
        self.p2.save()

        self.assertEqual(Post.objects(spacy_exits=True).count(), 2)
        self.assertTrue(all([type(p.spacy) == bytes for p in Post.objects]))




