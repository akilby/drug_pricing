"""Stores project constants and utility functions."""

import os
from datetime import datetime

import pymongo
from dotenv import load_dotenv
from praw import Reddit

# define project location
PROJ_DIR = os.path.dirname(os.path.abspath(__file__))

# load local environment variables
ENV_PATH = os.path.join(PROJ_DIR, ".env")
load_dotenv(dotenv_path=ENV_PATH)

# establish reddit connection
CONN = Reddit(client_id=os.getenv("RCLIENT_ID"),
              client_secret=os.getenv("RSECRET_KEY"),
              password=os.getenv("RPASSWORD"),
              username=os.getenv("RUSERNAME"),
              user_agent=os.getenv("RUSER_AGENT"))

# define subreddit
SUBR_NAME = "opiates"
SUBR = CONN.subreddit(SUBR_NAME)
SUB_LIMIT = 1000

# define mongo connection
MONGO = pymongo.MongoClient('localhost', int(os.getenv("PORT")))
DB = MONGO[os.getenv("DB_NAME")]
COLL = DB[os.getenv("COLL_NAME")]

# define hardcoded file locations on the HPC cluster
SUB_DIR = "/work/akilby/drug_pricing_project/opiates/opiates/threads"
COMM_DIR = "/work/akilby/drug_pricing_project/opiates/opiates/comments/complete"
OUT_JSON = "/work/akilby/drug_pricing_project/all_posts.json"

# hardcode column names for legacy comment and submission csv files
SUB_COLNAMES = ["id", "url", "num_comments",
                "shortlink", "author", "title", "text", "utc"]
COMM_COLNAMES = ["id", "sub_url", "parent_id", "text", "author", "utc"]


def utc_to_dt(utc: float) -> datetime:
    """Convert a unix time to a python datetime."""
    return datetime.utcfromtimestamp(int(utc))
