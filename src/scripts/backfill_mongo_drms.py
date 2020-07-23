from datetime import datetime
from typing import Any, Dict, Optional

from tqdm import tqdm

from src.schema import CommentPost, Post, SubmissionPost, User
from src.utils import connect_to_mongo, get_mongo


def praw_to_post() -> None:
    """Convert existing documents in praw to Post/User objects."""
    collection = get_mongo()["drug_pricing"]["praw"]
    print("Getting all documents from praw collection .....")
    all_praw = list(collection.find({}))

    connect_to_mongo()

    def maybe_attr(attr: str, pair: Dict[str, Any], dtype) -> Optional[Any]:
        if attr in pair:
            item = pair[attr]
            if isinstance(item, dtype):
                return item
        return None

    print("Converting praw documents to posts .....")
    for praw in tqdm(all_praw):
        # assign a user
        user = None
        if "username" in praw and isinstance(praw["username"], str):
            if User.objects(username=praw["username"]).count() == 0:
                user = User(username=praw["username"])
                user.save()
            else:
                user = User.objects(username=praw["username"]).first()

        # check if post exists
        if "pid" in praw and Post.objects(pid=praw["pid"]).count() == 0:

            # build general post features
            kwargs = {
                "pid": praw["pid"],
                "text": maybe_attr("text", praw, str),
                "user": user,
                "datetime": maybe_attr("time", praw, datetime),
                "subreddit": maybe_attr("subr", praw, str),
                "spacy": maybe_attr("spacy", praw, object),
            }

            # instantiate and assign specific post features
            if (
                ("is_sub" in praw and praw["is_sub"])
                or "url" in praw
                or "title" in praw
                or "num_comments" in praw
            ):
                post = SubmissionPost
                if "url" in praw:
                    url = praw["url"]
                    if isinstance(url, str):
                        praw["url"] = url
                if "title" in praw:
                    title = praw["title"]
                    if isinstance(title, str):
                        praw["title"] = title
                kwargs["num_comments"] = maybe_attr("num_comments", praw, int)
            elif "parent_id" in praw:
                post = CommentPost
                kwargs["parent_id"] = maybe_attr("parent_id", praw, str)
            else:
                post = Post
            kwargs = {k: v for k, v in kwargs.items() if v is not None}
            post(**kwargs).save()

    print("Done.")


if __name__ == "__main__":
    praw_to_post()
