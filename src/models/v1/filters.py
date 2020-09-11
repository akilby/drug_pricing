import dataclasses
import functools as ft
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Set

from src.schema import Location


class BaseFilter(ABC):

    @abstractmethod
    def filter(self, gpes: Set[str]) -> Set[str]:
        pass


class DenylistFilter(BaseFilter):

    def __init__(self, denylist: Set[str]):
        self.denylist = denylist

    def filter(self, gpes: Set[str]) -> Set[str]:
        return gpes - self.denylist


class LocationFilter(BaseFilter):

    def __init__(self, locations: Set[Location]):
        self.locations = locations

    def filter(self, gpes: Set[str]) -> Set[str]:
        """Remove any gpes that are not an incorporated location."""
        return ft.reduce(
            lambda acc, loc: acc | (gpes & set(loc.to_mongo().values())),
            self.locations,
            set()
        )
