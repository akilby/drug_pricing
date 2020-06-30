"""Tests for adding spacy to mongo posts."""
import unittest
from typing import Dict

import pymongo
import spacy

from src.spacy import add_spacy_to_mongo
from src.utils import get_mongo, DB_NAME, TEST_COLL_NAME


class TestAddSpacyToMongo(unittest.TestCase):
    """Tests for adding a spacy doc to each post in mongo."""

    coll = get_mongo()[DB_NAME][TEST_COLL_NAME]
    nlp = spacy.load("en_core_web_sm")

    @staticmethod
    def add_to_mongo(doc: Dict, coll: pymongo.collection.Collection):
        """Add a document to mongo."""
        try:
            coll.insert_one(doc)
        except pymongo.errors.DuplicateKeyError:
            pass

    def test_add_spacy_to_mongo(self):
        pre_total_count = self.coll.count_documents({})
        pre_spacy_count = self.coll.count_documents(
            {"spacy": {
                "$exists": True
            }})

        # insert sample doc without spacy
        doc1 = {
            "name": "test-no-spacy",
            "text": "sample text for testing if spacy is added."
        }
        doc2 = {"name": "test-no-spacy-no-text"}
        self.add_to_mongo(doc1, self.coll)
        self.add_to_mongo(doc2, self.coll)

        n_updated = add_spacy_to_mongo(self.coll, self.nlp)

        post_total_count = self.coll.count_documents({})
        self.assertEqual(pre_total_count, post_total_count)

        post_spacy_count = self.coll.count_documents(
            {"spacy": {
                "$exists": True
            }})
        self.assertEqual(n_updated, (post_spacy_count - pre_spacy_count))


if __name__ == '__main__':
    unittest.main()
