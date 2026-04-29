"""Tests for pipecheck.exporter module."""

import json
from unittest.mock import MagicMock

import pytest

from pipecheck.exporter import ExportError, ReportExporter


@pytest.fixture()
def mock_report():
    report = MagicMock()
    report.is_valid = True
    report.to_dict.return_value = {
        "is_valid": True,
        "total_records": 5,
        "total_errors": 0,
    }
    report.summary.return_value = {
        "total_records": 5,
        "total_errors": 0,
        "validation_errors": [],
        "profile": {
            "age": {"count": 5, "null_count": 1, "null_rate": 0.2, "unique_count": 4},
            "name": {"count": 5, "null_count": 0, "null_rate": 0.0, "unique_count": 5},
        },
    }
    return report


@pytest.fixture()
def exporter(mock_report):
    return ReportExporter(mock_report)


def test_to_json_returns_string(exporter):
    result = exporter.to_json()
    assert isinstance(result, str)


def test_to_json_is_valid_json(exporter):
    result = exporter.to_json()
    parsed = json.loads(result)
    assert parsed["is_valid"] is True
    assert parsed["total_records"] == 5


def test_to_json_custom_indent(exporter):
    result = exporter.to_json(indent=4)
    assert "    " in result  # 4-space indent present


def test_to_text_contains_status(exporter):
    result = exporter.to_text()
    assert "PASS" in result


def test_to_text_contains_record_count(exporter):
    result = exporter.to_text()
    assert "5" in result


def test_to_text_contains_column_profiles(exporter):
    result = exporter.to_text()
    assert "age" in result
    assert "name" in result


def test_to_text_fail_status(mock_report):
    mock_report.is_valid = False
    mock_report.summary.return_value["validation_errors"] = ["missing column: id"]
    exp = ReportExporter(mock_report)
    result = exp.to_text()
    assert "FAIL" in result
    assert "missing column: id" in result


def test_to_csv_returns_string(exporter):
    result = exporter.to_csv()
    assert isinstance(result, str)


def test_to_csv_has_header(exporter):
    result = exporter.to_csv()
    assert "column" in result
    assert "null_rate" in result


def test_to_csv_contains_columns(exporter):
    result = exporter.to_csv()
    assert "age" in result
    assert "name" in result


def test_to_csv_null_rate_formatted(exporter):
    result = exporter.to_csv()
    assert "0.2000" in result
