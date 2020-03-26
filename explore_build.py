import operator
import os
from collections import Counter
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple

import pandas as pd
import pymongo
import spacy
from psaw import PushshiftAPI
from spacy.tokens import DocBin
from spacy.lang.en import English

from constants import COLL
from constants import PSAW
from constants import TOPN_FP
from constants import TOPN_SPACY_FP


def proc_query_text(doc: Dict[Any, Any]) -> str:
    """Extract text from a mongo query."""
    text = doc["text"]
    return text if type(text) == str else ""


def text_to_docs(text: List[str], nlp: English) -> English:
    """Convert raw text to spacy docs."""
    return [nlp(t) for t in text]


def text_to_spacy(docs: Iterable[str], fp: str, nlp: English) -> None:
    """Process the docs with spacy and write to disk."""
    doc_bin = DocBin(attrs=["LEMMA", "ENT_IOB", "ENT_TYPE"],
                     store_user_data=True)
    for doc in nlp.pipe(docs):
        doc_bin.add(doc)
        bytes_data = doc_bin.to_bytes()

    # write bytes data to file
    out_f = open(fp, "wb")
    out_f.write(bytes_data)


def write_spacy(fp: str, coll: pymongo.collection.Collection,
                limit: Optional[int]) -> None:
    """Write documents from to disk as spacy objects."""
    # load documents from mongo
    if limit:
        text_res = coll.find({}, {"text": 1}).limit(limit)
    else:
        text_res = coll.find({}, {"text": 1})

    # process text as necessary
    all_text = map(proc_query_text, text_res)

    # train spacy on all text blocks
    print("Training spacy on text .....")
    text_to_spacy(all_text, fp)


def read_spacy(fp: str, nlp: English) -> List[English]:
    """Sample function to read spacy cache."""
    docbin = DocBin().from_bytes(open(fp, "rb").read())
    docs = docbin.get_docs(nlp.vocab)
    return list(docs)


def top_n_posters(coll: pymongo.collection.Collection, n: int,
                  subr: Optional[str] = None) -> List[str]:
    """
    Return the username of the top n posters.

    :param coll: the mongo collection to read from
    :param n: the number of top usernames to return
    :param subr: the subreddit to count from.  If None
                 then count from all subreddits

    :return: a list of the most frequent usernames
    """
    query = [
        {"$group": {"_id": "$username",
                    "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": n}]
    if subr:
        query.insert(0, {"$match": {"subr": subr}})
    res = coll.aggregate(query)
    users = [rec["_id"] for rec in res]
    filt_users = list(filter(lambda x: x and pd.notna(x), users))
    return filt_users


def user_posts(conn: PushshiftAPI, user: str) -> pd.DataFrame:
    """Retrieve the full reddit posting history for the given users."""
    subs = list(conn.search_submissions(author=user))
    comms = list(conn.search_comments(author=user))
    username = [user] * (len(subs) + len(comms))
    text = [s.selftext for s in subs] + [c.body for c in comms]
    subr = [s.subreddit for s in subs] + [c.subreddit for c in comms]
    is_sub = [True] * len(subs) + [False] * len(comms)
    ids = [s.id for s in subs] + [c.id for c in comms]
    data = {"username": username, "text": text, "subreddit": subr,
            "is_sub": is_sub, "id": ids}
    df = pd.DataFrame(data)
    return df


def users_posts(conn: PushshiftAPI, users: List[str]) -> List[str]:
    """
    Retrieve the full reddit posting history for all given users.

    :param conn: a psaw connection object
    :param users: a list of usernames

    :return: a dataframe of post features
    """
    dfs = list(map(lambda u: user_posts(conn, u), users))
    if len(dfs) > 0:
        return pd.concat(dfs)
    else:
        raise TypeError("No users were retrieved.")


def main():
    """Cache data generated for exploring raw data."""
    # extract the reddit data from top n users
    print("Retrieving top n user data .....")
    if os.path.exists(TOPN_FP):
        users_df = pd.read_csv(TOPN_FP)
    else:
        top_n = 100
        users = top_n_posters(COLL, top_n)
        users_df = users_posts(PSAW, users)
        users_df.to_csv(TOPN_FP, index=False)

    # load spacy object
    nlp = spacy.load("en_core_web_sm")

    # store user data as spacy
    print("Storing user data as spacy objs .....")
    if not os.path.exists(TOPN_SPACY_FP):
        text_docs = [t for t in users_df["text"].tolist() if type(t) == str]
        text_to_spacy(text_docs, TOPN_SPACY_FP, nlp)


if __name__ == "__main__":
    main()
