import os
from typing import List

import pandas as pd

from src.schema import CommentPost, Post, SubmissionPost
from src.utils import utc_to_dt

# hardcoded file locations on the HPC cluster
BASE_DIR = "/work/akilby/drug_pricing_project"
SUB_DIR = os.path.join(BASE_DIR, "opiates/opiates/threads")
COMM_DIR = os.path.join(BASE_DIR, "opiates/opiates/comments/complete")
OUT_JSON = os.path.join(BASE_DIR, "all_posts.json")

# hardcoded column names for legacy comment and submission csv files
SUB_COLNAMES = ["id", "url", "num_comments", "shortlink", "author", "title", "text", "utc"]
COMM_COLNAMES = ["id", "sub_url", "parent_id", "text", "author", "utc"]


def row_to_post(row: pd.Series, is_sub: bool) -> Post:
    """Convert a dataframe row to a post object."""
    # generic attributes
    pid = row["id"]
    username = row["author"]
    time = utc_to_dt(row["utc"])
    text = row["text"]

    # submission attrs
    if is_sub:
        url = row["url"]
        title = row["title"]
        num_comments = row["num_comments"]
        return SubmissionPost(
            pid=pid,
            username=username,
            time=time,
            text=text,
            url=url,
            title=title,
            num_comments=num_comments,
            subr="opiates",
        )

    # comment attrs
    parent_id = row["parent_id"]
    return CommentPost(
        pid=pid, username=username, time=time, text=text, parent_id=parent_id, subr="opiates"
    )


def extract_csv(filepath: str, colnames: List[str]) -> List[Post]:
    """
    Read all submissions or comments from the given csv file.

    :param filepath: the csv filepath
    :param colnames: the column names associated with the csv file

    :returns: a list of Post objects derived from the file
    """
    # parse the given file if it is a csv
    if os.path.splitext(filepath)[1] == ".csv":
        # read data into csv file
        df: pd.DataFrame = pd.read_csv(filepath, header=None)
        df.columns = colnames

        return [row_to_post(row, "parent_id" not in df.columns) for _, row in df.iterrows()]

    # else, return empty list
    return []


def read_csv(filepath: str, posttype: str) -> List[Post]:
    """Read data from the given csv file using the given post type."""
    if posttype.lower() in {"s", "c"}:
        if os.path.isfile(filepath):
            is_sub = posttype.lower() == "s"
            colnames = SUB_COLNAMES if is_sub else COMM_COLNAMES

            print("Reading Posts from csv file .....")
            file_data = extract_csv(filepath, colnames)

            print(f"{len(file_data)} posts from files retrieved.")
            return file_data

        raise ValueError("The given file does not exist.")

    raise ValueError("Invalid post type provided.")
