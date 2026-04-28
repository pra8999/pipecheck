"""Data profiling module for CSV/JSON pipeline inspection."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from collections import Counter
import statistics


@dataclass
class ColumnProfile:
    name: str
    total_count: int = 0
    null_count: int = 0
    unique_count: int = 0
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    mean: Optional[float] = None
    most_common: Optional[List[tuple]] = None

    @property
    def null_rate(self) -> float:
        if self.total_count == 0:
            return 0.0
        return self.null_count / self.total_count

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "total_count": self.total_count,
            "null_count": self.null_count,
            "null_rate": round(self.null_rate, 4),
            "unique_count": self.unique_count,
            "min_value": self.min_value,
            "max_value": self.max_value,
            "mean": self.mean,
            "most_common": self.most_common,
        }


@dataclass
class ProfileReport:
    record_count: int = 0
    column_profiles: Dict[str, ColumnProfile] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "record_count": self.record_count,
            "columns": {name: cp.to_dict() for name, cp in self.column_profiles.items()},
        }

    def high_null_columns(self, threshold: float = 0.5) -> List[str]:
        """Return column names where the null rate exceeds the given threshold.

        Args:
            threshold: Null rate threshold (0.0 to 1.0). Defaults to 0.5.

        Returns:
            List of column names with null_rate > threshold.
        """
        return [
            name
            for name, cp in self.column_profiles.items()
            if cp.null_rate > threshold
        ]


def profile_records(records: List[Dict[str, Any]]) -> ProfileReport:
    """Generate a ProfileReport from a list of record dicts."""
    if not records:
        return ProfileReport()

    report = ProfileReport(record_count=len(records))
    all_keys = {key for record in records for key in record.keys()}

    for col in all_keys:
        values = [r.get(col) for r in records]
        non_null = [v for v in values if v is not None and v != ""]
        null_count = len(values) - len(non_null)
        counter = Counter(non_null)

        numeric_vals = []
        for v in non_null:
            try:
                numeric_vals.append(float(v))
            except (TypeError, ValueError):
                pass

        profile = ColumnProfile(
            name=col,
            total_count=len(values),
            null_count=null_count,
            unique_count=len(counter),
            min_value=min(numeric_vals) if numeric_vals else (min(non_null, default=None)),
            max_value=max(numeric_vals) if numeric_vals else (max(non_null, default=None)),
            mean=round(statistics.mean(numeric_vals), 4) if numeric_vals else None,
            most_common=counter.most_common(3) if non_null else [],
        )
        report.column_profiles[col] = profile

    return report
