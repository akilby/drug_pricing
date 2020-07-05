import pymongo
import tqdm

from spacy.lang.en import English
from spacy.tokens import Doc


def add_spacy_to_mongo(coll: pymongo.collection.Collection,
                       nlp: English) -> int:
    """Add spacy field to all posts in mongo."""
    docs = list(
        coll.find({
            "$and": [{
                "spacy": {
                    "$exists": False
                }
            }, {
                "text": {
                    "$exists": True
                }
            }]
        }))
    for doc in tqdm.tqdm(docs):
        post = doc["text"]
        if type(post) == str:
            coll.update_one({"_id": doc["_id"]},
                            {"$set": {
                                "spacy": nlp(post).to_bytes()
                            }}, False)
    return len(docs)


def bytes_to_spacy(data: bytes, nlp: English) -> Doc:
    """Convert byte data to a spacy doc."""
    doc = Doc(nlp.vocab).from_bytes(data)
    return doc
