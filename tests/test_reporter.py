"""Tests for the PipelineReport reporter module."""

import pytest
from pipecheck.schema import PipeSchema, ColumnSchema
from pipecheck.validator import Validator, ValidationResult
from pipecheck.profiler import Profiler
from pipecheck.reporter import PipelineReport


@pytest.fixture
def schema():
    return PipeSchema(
        columns=[
            ColumnSchema(name="id", dtype="int", required=True),
            ColumnSchema(name="name", dtype="str", required=True),
            ColumnSchema(name="score", dtype="float", required=False),
        ]
    )


@pytest.fixture
def valid_records():
    return [
        {"id": 1, "name": "Alice", "score": 9.5},
        {"id": 2, "name": "Bob", "score": 7.2},
    ]


@pytest.fixture
def invalid_records():
    return [
        {"id": 1, "name": "Alice", "score": 9.5},
        {"id": "bad", "name": None, "score": 7.2},
    ]


def test_report_is_valid(schema, valid_records):
    result = Validator(schema).validate(valid_records)
    report = PipelineReport(validation=result)
    assert report.validation.is_valid


def test_report_summary_contains_pass(schema, valid_records):
    result = Validator(schema).validate(valid_records)
    report = PipelineReport(validation=result)
    summary = report.summary()
    assert "PASS" in summary
    assert "PIPECHECK REPORT" in summary


def test_report_summary_contains_fail(schema, invalid_records):
    result = Validator(schema).validate(invalid_records)
    report = PipelineReport(validation=result)
    summary = report.summary()
    assert "FAIL" in summary


def test_report_with_profile(schema, valid_records):
    result = Validator(schema).validate(valid_records)
    profile = Profiler().profile(valid_records)
    report = PipelineReport(validation=result, profile=profile)
    summary = report.summary()
    assert "Profile Summary" in summary
    assert "id" in summary


def test_report_to_dict_structure(schema, valid_records):
    result = Validator(schema).validate(valid_records)
    profile = Profiler().profile(valid_records)
    report = PipelineReport(validation=result, profile=profile)
    d = report.to_dict()
    assert "validation" in d
    assert "profile" in d
    assert "is_valid" in d["validation"]
    assert "record_count" in d["validation"]
    assert "errors" in d["validation"]


def test_report_to_dict_no_profile(schema, valid_records):
    result = Validator(schema).validate(valid_records)
    report = PipelineReport(validation=result)
    d = report.to_dict()
    assert "profile" not in d
