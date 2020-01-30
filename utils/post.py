"""Defines a Post object."""
from datetime import datetime
from typing import Any, Dict, Optional, Union

import prawcore
from praw.models import Comment, Submission

from constants import utc_to_dt


class Post():
    """Represents an intersection of attributes from praw Submission and Comment objects."""

    def __init__(self, subcomm: Union[Submission, Comment]) -> None:
        """Initialize attributes of this object."""
        # initialize attributes particular to Submissions and Comments
        if isinstance(subcomm, Submission):
            self.text: str = subcomm.selftext
            self.parent_id: Optional[str] = None
            self.is_sub: bool = True
        elif isinstance(subcomm, Comment):
            self.text = subcomm.body
            self.parent_id = subcomm.parent_id
            self.is_sub = False
        else:
            raise ValueError(
                "The given object is not a Submission or Comment.")

        # initialize attributes common between Submissions and Comments
        try:
            self.author_id: str = subcomm.author.id
        except (AttributeError, prawcore.exceptions.NotFound):
            # if praw cannot find the author, assign the string 'none'
            self.author_id = "none"
        self.time: datetime = utc_to_dt(subcomm.created_utc)
        self.pid: str = subcomm.id
        self.score: int = subcomm.score

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the attributes of this object to a dictionary.

        Note: if a post does not have an Author, the string 'none' is substituted
        for the author id.
        """
        return {"text": self.text,
                "parent_id": self.parent_id,
                "is_sub": self.is_sub,
                "author_id": self.author_id,
                "time": self.time,
                "pid": self.pid,
                "score": self.score}

    def __eq__(self, obj: Any) -> bool:
        """Determine if the given object equals this object."""
        return isinstance(obj, Post) and (obj.pid == self.pid) and (obj.is_sub == self.is_sub)

    def __ne__(self, obj: Any) -> bool:
        """Determine if the given object does not equal this object."""
        return not obj == self

    def __hash__(self) -> int:
        """Establish a hash value for this object."""
        comb_id = self.pid + ("T" if self.is_sub else "F")
        return hash(comb_id)
