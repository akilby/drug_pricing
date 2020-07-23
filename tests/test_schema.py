"""Tests for mongo operations."""
import unittest
import mongoengine
from mongoengine import connect, disconnect

from src.schema import SubmissionPost, Post, User, State, DateRangeLocation
from src.tasks.spacy import add_spacy_to_mongo, bytes_to_spacy
from datetime import datetime
import spacy


class TestPost(unittest.TestCase):
    """Tests for post objects."""

    # instantiate sample posts
    p1 = SubmissionPost(pid="abc", text="sample text")
    p2 = SubmissionPost(pid="abc", text="sample text 2")
    p3 = SubmissionPost(pid="xyz", text="sample text 3")

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

    def test_add_user(self) -> None:
        """Test that a user can be added to post."""
        user = User(username="sample_user")
        user.save()

        self.p1.save()
        self.assertEqual(Post.objects(user__exists=True).count(), 0)

        self.p1.user = user
        self.p1.save()

        self.assertEqual(Post.objects(user__exists=True).count(), 1)
        self.assertEqual(Post.objects(pid=self.p1.pid).first().user, user)

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

    def test_spacy(self) -> None:
        """Test that to/from spacy works."""
        # test adding to spacy
        self.assertEqual(Post.objects(spacy__exists=True).count(), 0)

        self.p1.save()
        self.p3.save()

        nlp = spacy.load("en_core_web_sm")
        add_spacy_to_mongo(nlp)

        self.assertEqual(Post.objects(spacy__exists=True).count(), 2)

        bin_spacy = Post.objects.first().spacy
        en_spacy = bytes_to_spacy(bin_spacy, nlp)
        self.assertEqual(type(en_spacy), spacy.tokens.doc.Doc)
        self.assertEqual(en_spacy.text, nlp(Post.objects.first().text).text)


class TestUser(unittest.TestCase):
    """Tests for user objects."""

    # instantiate sample posts
    user_1 = User(username="user1")
    user_2 = User(username="user2")

    @classmethod
    def setUpClass(cls):
        connect('mongoenginetest', host='mongomock://localhost')

    @classmethod
    def tearDownClass(cls):
        disconnect()

    def test_insert(self) -> None:
        """Test that a user can be added."""
        self.user_1.save()
        self.assertEqual(User.objects(username=self.user_1.username).first(), self.user_1)

        # TODO: test insert duplicate user

    def test_add_location(self) -> None:
        """Test that a location can be added to a user."""
        self.user_1.save()
        self.assertEqual(User.objects(username=self.user_1.username).first().locations, [])

        location = State(name="massachusetts")
        drlocation = DateRangeLocation(location=location, start_datetime=datetime.now())
        self.user_1.locations = [drlocation]
        self.user_1.save()
        self.assertTrue(len(User.objects(username=self.user_1.username).first().locations) > 0)


