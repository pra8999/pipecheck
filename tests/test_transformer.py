"""Tests for pipecheck.transformer module."""

import pytest

from pipecheck.transformer import (
    RecordTransformer,
    TransformRule,
    cast_to_float,
    cast_to_int,
    strip_whitespace,
    to_lowercase,
)


@pytest.fixture()
def sample_records():
    return [
        {"name": "  Alice ", "age": "30", "score": "9.5"},
        {"name": "BOB", "age": "25", "score": "8.0"},
        {"name": None, "age": None, "score": None},
    ]


def test_strip_whitespace(sample_records):
    transformer = RecordTransformer(rules=[strip_whitespace("name")])
    result = transformer.transform(sample_records)
    assert result.success
    assert result.records[0]["name"] == "Alice"
    assert result.records[1]["name"] == "BOB"  # no leading/trailing spaces


def test_to_lowercase(sample_records):
    transformer = RecordTransformer(rules=[to_lowercase("name")])
    result = transformer.transform(sample_records)
    assert result.records[1]["name"] == "bob"


def test_cast_to_int(sample_records):
    transformer = RecordTransformer(rules=[cast_to_int("age")])
    result = transformer.transform(sample_records)
    assert result.success
    assert result.records[0]["age"] == 30
    assert isinstance(result.records[0]["age"], int)
    assert result.records[2]["age"] is None  # None passes through


def test_cast_to_float(sample_records):
    transformer = RecordTransformer(rules=[cast_to_float("score")])
    result = transformer.transform(sample_records)
    assert result.success
    assert result.records[0]["score"] == pytest.approx(9.5)


def test_multiple_rules(sample_records):
    transformer = RecordTransformer(rules=[
        strip_whitespace("name"),
        to_lowercase("name"),
        cast_to_int("age"),
        cast_to_float("score"),
    ])
    result = transformer.transform(sample_records)
    assert result.success
    assert result.records[0]["name"] == "alice"
    assert result.records[0]["age"] == 30
    assert result.records[0]["score"] == pytest.approx(9.5)


def test_transform_error_collected():
    records = [{"age": "not_a_number"}]
    transformer = RecordTransformer(rules=[cast_to_int("age")])
    result = transformer.transform(records)
    assert not result.success
    assert result.error_count() == 1
    assert "cast_to_int" in result.errors[0]


def test_missing_column_skipped():
    """Rules targeting absent columns should not raise errors."""
    records = [{"name": "Alice"}]
    transformer = RecordTransformer(rules=[cast_to_int("age")])
    result = transformer.transform(records)
    assert result.success
    assert result.records[0] == {"name": "Alice"}


def test_add_rule_dynamically():
    transformer = RecordTransformer()
    transformer.add_rule(strip_whitespace("name"))
    result = transformer.transform([{"name": "  Eve  "}])
    assert result.records[0]["name"] == "Eve"
