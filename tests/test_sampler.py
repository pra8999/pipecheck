"""Tests for pipecheck.sampler."""

import pytest
from pipecheck.sampler import DataSampler, SampleResult


@pytest.fixture
def sample_records():
    return [
        {"id": i, "category": "A" if i % 2 == 0 else "B", "value": i * 1.5}
        for i in range(20)
    ]


def test_head_returns_first_n(sample_records):
    sampler = DataSampler()
    result = sampler.head(sample_records, n=5)
    assert result.sample_size == 5
    assert result.records == sample_records[:5]


def test_head_clamps_to_total(sample_records):
    sampler = DataSampler()
    result = sampler.head(sample_records, n=100)
    assert result.sample_size == len(sample_records)


def test_random_sample_size(sample_records):
    sampler = DataSampler(seed=42)
    result = sampler.random_sample(sample_records, n=7)
    assert result.sample_size == 7
    assert len(result.records) == 7


def test_random_sample_reproducible(sample_records):
    r1 = DataSampler(seed=0).random_sample(sample_records, n=5)
    r2 = DataSampler(seed=0).random_sample(sample_records, n=5)
    assert r1.records == r2.records


def test_random_sample_different_seeds(sample_records):
    r1 = DataSampler(seed=1).random_sample(sample_records, n=10)
    r2 = DataSampler(seed=99).random_sample(sample_records, n=10)
    # Very unlikely to be identical with different seeds
    assert r1.records != r2.records


def test_random_sample_empty_records():
    sampler = DataSampler(seed=1)
    result = sampler.random_sample([], n=5)
    assert result.sample_size == 0
    assert result.records == []
    assert result.sample_rate == 0.0


def test_stratified_sample_groups(sample_records):
    sampler = DataSampler(seed=7)
    result = sampler.stratified_sample(sample_records, key="category", n_per_group=3)
    categories = {r["category"] for r in result.records}
    assert categories == {"A", "B"}
    assert result.sample_size == 6  # 3 per group * 2 groups


def test_stratified_sample_clamps_per_group():
    records = [{"cat": "X", "v": i} for i in range(2)]
    sampler = DataSampler(seed=0)
    result = sampler.stratified_sample(records, key="cat", n_per_group=10)
    assert result.sample_size == 2


def test_sample_rate(sample_records):
    sampler = DataSampler(seed=3)
    result = sampler.random_sample(sample_records, n=10)
    assert result.sample_rate == pytest.approx(0.5)


def test_to_dict(sample_records):
    sampler = DataSampler(seed=5)
    result = sampler.head(sample_records, n=3)
    d = result.to_dict()
    assert d["total_records"] == 20
    assert d["sample_size"] == 3
    assert "sample_rate" in d
    assert len(d["records"]) == 3
