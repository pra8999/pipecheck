"""Schema definition and validation models for pipecheck."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


VALID_TYPES = {"string", "integer", "float", "boolean", "date", "datetime", "null"}


@dataclass
class ColumnSchema:
    """Defines expected schema for a single column/field."""

    name: str
    dtype: str
    required: bool = True
    nullable: bool = False
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    allowed_values: Optional[List[Any]] = None

    def __post_init__(self):
        if self.dtype not in VALID_TYPES:
            raise ValueError(
                f"Invalid dtype '{self.dtype}' for column '{self.name}'. "
                f"Must be one of: {sorted(VALID_TYPES)}"
            )


@dataclass
class PipeSchema:
    """Top-level schema definition for a data pipeline source."""

    name: str
    columns: List[ColumnSchema] = field(default_factory=list)
    allow_extra_columns: bool = False

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PipeSchema":
        """Build a PipeSchema from a plain dictionary (e.g. loaded from YAML/JSON)."""
        columns = [
            ColumnSchema(
                name=col["name"],
                dtype=col["dtype"],
                required=col.get("required", True),
                nullable=col.get("nullable", False),
                min_value=col.get("min_value"),
                max_value=col.get("max_value"),
                allowed_values=col.get("allowed_values"),
            )
            for col in data.get("columns", [])
        ]
        return cls(
            name=data["name"],
            columns=columns,
            allow_extra_columns=data.get("allow_extra_columns", False),
        )

    def column_map(self) -> Dict[str, ColumnSchema]:
        """Return a dict mapping column names to their ColumnSchema."""
        return {col.name: col for col in self.columns}
