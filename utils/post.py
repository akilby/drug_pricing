"""Defines a Post object."""
from datetime import datetime
from typing import Any, Dict, Optional, TypeVar, Union

import prawcore
from praw.models import Comment, Submission

from constants import utc_to_dt

T = TypeVar('T')


class Post():
    """Represents an intersection of attributes from praw Submission and Comment objects."""

    def __init__(self,
                 pid: str,
                 text: str,
                 author: str,
                 time: datetime,
                 is_sub: bool,
                 parent_id: Optional[str] = None) -> None:
        """Initialize attributes of this object."""
        self.pid = pid
        self.text = text
        self.author = author
        self.time = time
        self.is_sub = is_sub
        self.parent_id = parent_id

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the attributes of this object to a dictionary.

        Note: if a post does not have an Author, the string 'none' is substituted
        for the author id.
        """
        return {"text": self.text,
                "parent_id": self.parent_id,
                "is_sub": self.is_sub,
                "author": self.author,
                "time": str(self.time),
                "pid": self.pid}

    def __eq__(self, obj: Any) -> bool:
        """Determine if the given object equals this object."""
        return isinstance(obj, Post) and (obj.pid == self.pid) and (obj.is_sub == self.is_sub) and (obj.text == self.text)

    def __ne__(self, obj: Any) -> bool:
        """Determine if the given object does not equal this object."""
        return not obj == self

    def __hash__(self) -> int:
        """Establish a hash value for this object."""
        comb_id = 100 * self.pid + 10 * hash(self.is_sub) + hash(self.text)
        return hash(comb_id)
