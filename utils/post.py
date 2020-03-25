"""Defines Post objects."""
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional
from constants import dt_to_utc


@dataclass
class Post():
    """An abstract representation of Submission and Comment objects."""

    def __init__(self, pid: Optional[str] = None,
                 text: Optional[str] = None,
                 username: Optional[str] = None,
                 time: Optional[datetime] = None,
                 subr: Optional[str] = None) -> None:
        """Initialize attributes of the Post."""
        self.pid = pid
        self.text = text
        self.username = username
        self.time = time
        self.subr = subr
        self.utc = dt_to_utc(self.time)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the attributes of this object to a dictionary."""
        return {"text": self.text,
                "username": self.username,
                "time": self.utc,
                "pid": self.pid,
                "hash": hash(self),
                "subr": self.subr}

    def __eq__(self, obj: object) -> bool:
        """Determine if the given object equals this object."""
        return isinstance(obj, self) and (obj.pid == self.pid) and \
            (obj.text == self.text)

    def __ne__(self, obj: Any) -> bool:
        """Determine if the given object does not equal this object."""
        return not obj == self

    def __hash__(self) -> int:
        """Establish a hash value for this object."""
        return 10 * hash(self.text) + hash(self.pid)


@dataclass(eq=False)
class Sub(Post):
    """Represents a Submission Object."""

    def __init__(self, pid: Optional[str] = None,
                 text: Optional[str] = None,
                 username: Optional[str] = None,
                 time: Optional[datetime] = None,
                 url: Optional[str] = None,
                 title: Optional[str] = None,
                 num_comments: Optional[int] = None,
                 subr: Optional[str] = None) -> None:
        """Initialize attributes of the Submission."""
        super().__init__(pid=pid, text=text, username=username, time=time,
                         subr=subr)
        self.url = url
        self.title = title
        self.num_comments = num_comments

    def to_dict(self) -> Dict[str, Any]:
        """Convert the attributes of this object to a dictionary."""
        base_dict = super().to_dict()
        base_dict.update({"title": self.title,
                          "num_comments": self.num_comments,
                          "is_sub": True})
        return base_dict


@dataclass(eq=False)
class Comm(Post):
    """Represents a Comment object."""

    def __init__(self, pid: Optional[str] = None,
                 text: Optional[str] = None,
                 username: Optional[str] = None,
                 time: Optional[datetime] = None,
                 parent_id: Optional[str] = None,
                 subr: Optional[str] = None) -> None:
        """Initialize attributes of the Comment."""
        super().__init__(pid=pid, text=text, username=username, time=time,
                         subr=subr)
        self.parent_id = parent_id

    def to_dict(self) -> Dict[str, Any]:
        """Convert the attributes of this object to a dictionary."""
        base_dict = super().to_dict()
        base_dict.update({"parent_id": self.parent_id,
                          "is_sub": False})
        return base_dict
