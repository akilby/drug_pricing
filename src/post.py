"""Defines a Post object."""
from typing import Any, Union

from praw.models import Comment, Submission

from .load import utc_to_dt


class Post():
    """Represents an intersection of attributes from Submission and Comment objects."""

    def __init__(self, subcomm: Union[Submission, Comment]) -> None:
        """Initialize attributes of this object."""
        # initialize attributes particular to Submissions and Comments
        if isinstance(subcomm, Submission):
            self.text = subcomm.selftext
            self.parent_id = None
            self.is_sub = True
        elif isinstance(subcomm, Comment):
            self.text = subcomm.body
            self.parent_id = subcomm.parent_id
            self.is_sub = False
        else:
            raise ValueError(
                "The given object is not a Submission or Comment.")

        # initialize attributes common between Submissions and Comments
        self.author = subcomm.author
        self.time = utc_to_dt(subcomm.created_utc)
        self.pid = subcomm.id
        self.score = subcomm.score

    def __eq__(self, obj: Any) -> bool:
        """Determine if the given object equals this object."""
        return isinstance(obj, Post) and (obj.pid == self.pid) and (obj.is_sub == self.is_sub)

    def __ne__(self, obj: Any) -> bool:
        """Determine if the given object does not equal this object."""
        return not obj == self
