"""Tests for parsing data from Reddit."""
import unittest

from constants import DB
from utils.functions import to_mongo
from utils.post import Sub


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
