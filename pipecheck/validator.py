"""Core validation logic for checking records against a PipeSchema."""

from dataclasses import dataclass, field
from typing import Any, Dict, List

from pipecheck.schema import ColumnSchema, PipeSchema


TYPE_CHECKS = {
    "integer": int,
    "float": (int, float),
    "string": str,
    "boolean": bool,
}


@dataclass
class ValidationError:
    row: int
    column: str
    message: str

    def __str__(self) -> str:
        return f"Row {self.row} | Column '{self.column}': {self.message}"


@dataclass
class ValidationResult:
    total_rows: int = 0
    errors: List[ValidationError] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    @property
    def error_count(self) -> int:
        return len(self.errors)


def _check_column(row_idx: int, col: ColumnSchema, value: Any) -> List[ValidationError]:
    errors: List[ValidationError] = []

    if value is None or value == "":
        if not col.nullable:
            errors.append(ValidationError(row_idx, col.name, "Null/empty value not allowed"))
        return errors

    if col.dtype in TYPE_CHECKS:
        expected = TYPE_CHECKS[col.dtype]
        if not isinstance(value, expected):
            errors.append(
                ValidationError(row_idx, col.name, f"Expected {col.dtype}, got {type(value).__name__}")
            )
            return errors

    if col.min_value is not None and isinstance(value, (int, float)) and value < col.min_value:
        errors.append(ValidationError(row_idx, col.name, f"Value {value} below min {col.min_value}"))

    if col.max_value is not None and isinstance(value, (int, float)) and value > col.max_value:
        errors.append(ValidationError(row_idx, col.name, f"Value {value} above max {col.max_value}"))

    if col.allowed_values is not None and value not in col.allowed_values:
        errors.append(
            ValidationError(row_idx, col.name, f"Value '{value}' not in allowed set {col.allowed_values}")
        )

    return errors


def validate_records(records: List[Dict[str, Any]], schema: PipeSchema) -> ValidationResult:
    """Validate a list of record dicts against the given PipeSchema."""
    result = ValidationResult(total_rows=len(records))
    col_map = schema.column_map()

    for idx, record in enumerate(records, start=1):
        present_keys = set(record.keys())

        for col in schema.columns:
            if col.required and col.name not in present_keys:
                result.errors.append(ValidationError(idx, col.name, "Required column missing from record"))
                continue
            if col.name in record:
                result.errors.extend(_check_column(idx, col, record[col.name]))

        if not schema.allow_extra_columns:
            extra = present_keys - set(col_map.keys())
            for extra_col in sorted(extra):
                result.errors.append(ValidationError(idx, extra_col, "Unexpected column not in schema"))

    return result
