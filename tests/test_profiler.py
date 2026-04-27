"""Tests for pipecheck.profiler module."""

import pytest
from pipecheck.profiler import profile_records, ColumnProfile, ProfileReport


@pytest.fixture
def sample_records():
    return [
        {"id": 1, "name": "Alice", "score": 95.0, "tag": "A"},
        {"id": 2, "name": "Bob", "score": 82.5, "tag": "B"},
        {"id": 3, "name": "Charlie", "score": 78.0, "tag": "A"},
        {"id": 4, "name": None, "score": None, "tag": "B"},
        {"id": 5, "name": "Eve", "score": 91.0, "tag": "A"},
    ]


def test_profile_empty_records():
    report = profile_records([])
    assert report.record_count == 0
    assert report.column_profiles == {}


def test_profile_record_count(sample_records):
    report = profile_records(sample_records)
    assert report.record_count == 5


def test_profile_columns_present(sample_records):
    report = profile_records(sample_records)
    assert set(report.column_profiles.keys()) == {"id", "name", "score", "tag"}


def test_null_count(sample_records):
    report = profile_records(sample_records)
    assert report.column_profiles["name"].null_count == 1
    assert report.column_profiles["score"].null_count == 1
    assert report.column_profiles["id"].null_count == 0


def test_null_rate(sample_records):
    report = profile_records(sample_records)
    assert report.column_profiles["name"].null_rate == pytest.approx(0.2)
    assert report.column_profiles["id"].null_rate == pytest.approx(0.0)


def test_unique_count(sample_records):
    report = profile_records(sample_records)
    assert report.column_profiles["tag"].unique_count == 2
    assert report.column_profiles["id"].unique_count == 5


def test_numeric_mean(sample_records):
    report = profile_records(sample_records)
    score_profile = report.column_profiles["score"]
    assert score_profile.mean == pytest.approx((95.0 + 82.5 + 78.0 + 91.0) / 4, rel=1e-3)


def test_numeric_min_max(sample_records):
    report = profile_records(sample_records)
    score_profile = report.column_profiles["score"]
    assert score_profile.min_value == pytest.approx(78.0)
    assert score_profile.max_value == pytest.approx(95.0)


def test_most_common(sample_records):
    report = profile_records(sample_records)
    tag_profile = report.column_profiles["tag"]
    most_common_values = [v for v, _ in tag_profile.most_common]
    assert "A" in most_common_values


def test_to_dict(sample_records):
    report = profile_records(sample_records)
    d = report.to_dict()
    assert d["record_count"] == 5
    assert "columns" in d
    assert "score" in d["columns"]
    assert "null_rate" in d["columns"]["score"]
