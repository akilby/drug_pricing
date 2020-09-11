from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import dataclass
from typing import Dict, Sequence

from scipy.special import softmax


class BaseRanker(ABC):

    @abstractmethod
    def rank(self, gpes: Sequence[str]) -> Dict[str, float]:
        pass


@dataclass
class FrequencyRanker(BaseRanker):

    def rank(self, gpes: Sequence[str]) -> Dict[str, float]:
        """Rank the following gpes by their frequency."""
        counts = Counter(gpes)
        keys = list(counts.keys())
        normalized_counts = softmax(list(counts.values())) if len(keys) > 0 else []
        return {k: v for k, v in zip(keys, normalized_counts)}

    def __hash__(self) -> int:
        return hash("frequencyranker")