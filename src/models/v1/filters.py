import dataclasses
import functools as ft
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Set

from src.utils import Location


class BaseFilter(ABC):

    @abstractmethod
    def filter(self, gpes: Set[str]) -> Set[str]:
        pass


@dataclass
class DenylistFilter(BaseFilter):
    denylist: Set[str]

    def filter(self, gpes: Set[str]) -> Set[str]:
        return gpes - self.denylist

    def __hash__(self) -> int:
        return hash(frozenset(self.denylist))


@dataclass
class LocationFilter(BaseFilter):
    locations: Set[Location]

    def filter(self, gpes: Set[str]) -> Set[str]:
        """Remove any gpes that are not an incorporated location."""
        return ft.reduce(
            lambda acc, loc: acc | (gpes & set(dataclasses.asdict(loc).values())),
            self.locations,
            set()
        )

    def __hash__(self) -> int:
        return hash(frozenset(self.locations))
