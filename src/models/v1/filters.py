import dataclasses
import functools as ft
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Set

import pandas as pd

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

    def __init__(self, locations: pd.DataFrame):
        self.locations = locations

    def filter(self, gpes: Set[str]) -> Set[str]:
        """Remove any gpes that are not an incorporated location."""
        records = self.locations.to_dict(orient="records")
        return ft.reduce(
            lambda acc, loc: acc | (gpes & set(loc.values())),
            records,
            set()
        )
