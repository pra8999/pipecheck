"""Data sampling utilities for pipecheck pipelines."""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional


@dataclass
class SampleResult:
    """Result of a sampling operation."""

    total_records: int
    sample_size: int
    records: List[Dict[str, Any]] = field(default_factory=list)
    seed: Optional[int] = None

    @property
    def sample_rate(self) -> float:
        """Fraction of total records included in the sample."""
        if self.total_records == 0:
            return 0.0
        return self.sample_size / self.total_records

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_records": self.total_records,
            "sample_size": self.sample_size,
            "sample_rate": round(self.sample_rate, 4),
            "seed": self.seed,
            "records": self.records,
        }


class DataSampler:
    """Samples records from a dataset using various strategies."""

    def __init__(self, seed: Optional[int] = None) -> None:
        self.seed = seed
        self._rng = random.Random(seed)

    def random_sample(
        self,
        records: List[Dict[str, Any]],
        n: int,
    ) -> SampleResult:
        """Return up to *n* randomly selected records."""
        total = len(records)
        n = min(n, total)
        sampled = self._rng.sample(records, n) if n > 0 else []
        return SampleResult(
            total_records=total,
            sample_size=len(sampled),
            records=sampled,
            seed=self.seed,
        )

    def head(
        self,
        records: List[Dict[str, Any]],
        n: int = 10,
    ) -> SampleResult:
        """Return the first *n* records."""
        sampled = records[:n]
        return SampleResult(
            total_records=len(records),
            sample_size=len(sampled),
            records=sampled,
            seed=None,
        )

    def stratified_sample(
        self,
        records: List[Dict[str, Any]],
        key: str,
        n_per_group: int,
    ) -> SampleResult:
        """Sample up to *n_per_group* records from each unique value of *key*."""
        groups: Dict[Any, List[Dict[str, Any]]] = {}
        for record in records:
            group_val = record.get(key)
            groups.setdefault(group_val, []).append(record)

        sampled: List[Dict[str, Any]] = []
        for group_records in groups.values():
            sampled.extend(
                self._rng.sample(group_records, min(n_per_group, len(group_records)))
            )

        return SampleResult(
            total_records=len(records),
            sample_size=len(sampled),
            records=sampled,
            seed=self.seed,
        )
