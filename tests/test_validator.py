"""Tests for pipecheck schema and validator modules."""

import pytest

from pipecheck.schema import ColumnSchema, PipeSchema
from pipecheck.validator import validate_records, ValidationResult


@pytest.fixture
def simple_schema():
    return PipeSchema(
        name="users",
        columns=[
            ColumnSchema(name="id", dtype="integer", required=True, nullable=False),
            ColumnSchema(name="name", dtype="string", required=True, nullable=False),
            ColumnSchema(name="age", dtype="integer", required=False, nullable=True, min_value=0, max_value=150),
            ColumnSchema(name="status", dtype="string", required=True, allowed_values=["active", "inactive"]),
        ],
        allow_extra_columns=False,
    )


def test_valid_records(simple_schema):
    records = [
        {"id": 1, "name": "Alice", "age": 30, "status": "active"},
        {"id": 2, "name": "Bob", "age": None, "status": "inactive"},
    ]
    result = validate_records(records, simple_schema)
    assert result.is_valid
    assert result.total_rows == 2


def test_missing_required_column(simple_schema):
    records = [{"id": 1, "age": 25, "status": "active"}]  # missing 'name'
    result = validate_records(records, simple_schema)
    assert not result.is_valid
    assert any(e.column == "name" for e in result.errors)


def test_wrong_type(simple_schema):
    records = [{"id": "not-an-int", "name": "Alice", "status": "active"}]
    result = validate_records(records, simple_schema)
    assert not result.is_valid
    assert any("Expected integer" in e.message for e in result.errors)


def test_value_out_of_range(simple_schema):
    records = [{"id": 1, "name": "Alice", "age": 200, "status": "active"}]
    result = validate_records(records, simple_schema)
    assert not result.is_valid
    assert any("above max" in e.message for e in result.errors)


def test_disallowed_value(simple_schema):
    records = [{"id": 1, "name": "Alice", "status": "pending"}]
    result = validate_records(records, simple_schema)
    assert not result.is_valid
    assert any("not in allowed set" in e.message for e in result.errors)


def test_extra_column_rejected(simple_schema):
    records = [{"id": 1, "name": "Alice", "status": "active", "extra_field": "oops"}]
    result = validate_records(records, simple_schema)
    assert not result.is_valid
    assert any(e.column == "extra_field" for e in result.errors)


def test_extra_column_allowed():
    schema = PipeSchema(
        name="flexible",
        columns=[ColumnSchema(name="id", dtype="integer")],
        allow_extra_columns=True,
    )
    records = [{"id": 1, "bonus": "allowed"}]
    result = validate_records(records, schema)
    assert result.is_valid


def test_pipe_schema_from_dict():
    data = {
        "name": "orders",
        "allow_extra_columns": True,
        "columns": [
            {"name": "order_id", "dtype": "integer"},
            {"name": "amount", "dtype": "float", "min_value": 0.0},
        ],
    }
    schema = PipeSchema.from_dict(data)
    assert schema.name == "orders"
    assert len(schema.columns) == 2
    assert schema.allow_extra_columns is True


def test_invalid_dtype_raises():
    with pytest.raises(ValueError, match="Invalid dtype"):
        ColumnSchema(name="bad", dtype="unknown_type")
