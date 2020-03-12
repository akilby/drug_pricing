"""Stores project constants and utility functions."""
import os
from datetime import datetime

import pymongo
from dotenv import load_dotenv
from praw import Reddit
from psaw import PushshiftAPI

# define project location
PROJ_DIR = os.path.dirname(os.path.abspath(__file__))

# load local environment variables
ENV_PATH = os.path.join(PROJ_DIR, ".env")
load_dotenv(dotenv_path=ENV_PATH)

# establish reddit connection
PRAW = Reddit(client_id=os.getenv("RCLIENT_ID"),
              client_secret=os.getenv("RSECRET_KEY"),
              password=os.getenv("RPASSWORD"),
              username=os.getenv("RUSERNAME"),
              user_agent=os.getenv("RUSER_AGENT"))
PSAW = PushshiftAPI(PRAW)

# define subreddit
SUBR_NAME = "opiates"
SUBR = PRAW.subreddit(SUBR_NAME)
SUB_LIMIT = 1000

# define mongo connection
MONGO = pymongo.MongoClient(os.getenv("HOST"),
                            int(os.getenv("PORT")),
                            username=os.getenv("MUSERNAME"),
                            password=os.getenv("MPASSWORD"),
                            authSource=os.getenv("DB_NAME"))
DB = MONGO[os.getenv("DB_NAME")]
COLL = DB[os.getenv("COLL_NAME")]

# define hardcoded file locations on the HPC cluster
BASE_DIR = "/work/akilby/drug_pricing_project"
SUB_DIR = os.path.join(BASE_DIR, "opiates/opiates/threads")
COMM_DIR = os.path.join(BASE_DIR, "opiates/opiates/comments/complete")
OUT_JSON = os.path.join(BASE_DIR, "all_posts.json")

# hardcode column names for legacy comment and submission csv files
SUB_COLNAMES = ["id", "url", "num_comments", "shortlink", "author", "title",
                "text", "utc"]
COMM_COLNAMES = ["id", "sub_url", "parent_id", "text", "author", "utc"]

# names for spacy
SPACY_FN = "spacy_docs"
SPACY_FP = os.path.join(PROJ_DIR, "data",  SPACY_FN)


def utc_to_dt(utc: float) -> datetime:
    """Convert a unix time to a python datetime."""
    return datetime.utcfromtimestamp(int(utc))
