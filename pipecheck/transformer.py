"""Lightweight record transformer for normalizing data before validation."""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class TransformRule:
    """Defines a single transformation to apply to a named column."""

    column: str
    fn: Callable[[Any], Any]
    description: str = ""


@dataclass
class TransformResult:
    """Holds the outcome of applying a set of transform rules to records."""

    records: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    def error_count(self) -> int:
        return len(self.errors)


class RecordTransformer:
    """Applies a sequence of TransformRules to a list of records."""

    def __init__(self, rules: Optional[List[TransformRule]] = None) -> None:
        self.rules: List[TransformRule] = rules or []

    def add_rule(self, rule: TransformRule) -> None:
        """Register an additional transform rule."""
        self.rules.append(rule)

    def transform(self, records: List[Dict[str, Any]]) -> TransformResult:
        """Apply all rules to every record, collecting errors without stopping."""
        result = TransformResult()
        for idx, record in enumerate(records):
            transformed = dict(record)
            for rule in self.rules:
                if rule.column not in transformed:
                    continue
                try:
                    transformed[rule.column] = rule.fn(transformed[rule.column])
                except Exception as exc:  # noqa: BLE001
                    result.errors.append(
                        f"Row {idx}: column '{rule.column}' transform "
                        f"'{rule.description}' failed — {exc}"
                    )
            result.records.append(transformed)
        return result


# ---------------------------------------------------------------------------
# Built-in convenience factories
# ---------------------------------------------------------------------------

def strip_whitespace(column: str) -> TransformRule:
    return TransformRule(column=column, fn=lambda v: v.strip() if isinstance(v, str) else v,
                         description="strip_whitespace")


def to_lowercase(column: str) -> TransformRule:
    return TransformRule(column=column, fn=lambda v: v.lower() if isinstance(v, str) else v,
                         description="to_lowercase")


def cast_to_int(column: str) -> TransformRule:
    return TransformRule(column=column, fn=lambda v: int(v) if v is not None else v,
                         description="cast_to_int")


def cast_to_float(column: str) -> TransformRule:
    return TransformRule(column=column, fn=lambda v: float(v) if v is not None else v,
                         description="cast_to_float")
