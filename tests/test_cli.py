"""Tests for the pipecheck CLI entry point."""

import json
import pytest
from unittest.mock import patch
from pathlib import Path
from pipecheck.cli import load_records, build_parser, main


@pytest.fixture
def json_data_file(tmp_path):
    records = [
        {"id": 1, "name": "Alice", "score": 9.5},
        {"id": 2, "name": "Bob", "score": 7.2},
    ]
    p = tmp_path / "data.json"
    p.write_text(json.dumps(records))
    return str(p)


@pytest.fixture
def csv_data_file(tmp_path):
    p = tmp_path / "data.csv"
    p.write_text("id,name,score\n1,Alice,9.5\n2,Bob,7.2\n")
    return str(p)


@pytest.fixture
def schema_file(tmp_path):
    schema = {
        "columns": [
            {"name": "id", "dtype": "int", "required": True},
            {"name": "name", "dtype": "str", "required": True},
            {"name": "score", "dtype": "float", "required": False},
        ]
    }
    p = tmp_path / "schema.json"
    p.write_text(json.dumps(schema))
    return str(p)


def test_load_records_json(json_data_file):
    records = load_records(json_data_file)
    assert len(records) == 2
    assert records[0]["name"] == "Alice"


def test_load_records_csv(csv_data_file):
    records = load_records(csv_data_file)
    assert len(records) == 2
    assert records[1]["name"] == "Bob"


def test_load_records_missing_file():
    with pytest.raises(SystemExit):
        load_records("/nonexistent/path/data.json")


def test_load_records_unsupported_format(tmp_path):
    p = tmp_path / "data.txt"
    p.write_text("hello")
    with pytest.raises(SystemExit):
        load_records(str(p))


def test_build_parser_defaults():
    parser = build_parser()
    args = parser.parse_args(["data.json"])
    assert args.data == "data.json"
    assert args.schema is None
    assert args.profile is False
    assert args.output == "text"


def test_cli_main_valid_json_output(json_data_file, schema_file, capsys):
    with patch("sys.argv", ["pipecheck", json_data_file, "--schema", schema_file, "--output", "json"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
    captured = capsys.readouterr()
    output = json.loads(captured.out)
    assert output["validation"]["is_valid"] is True


def test_cli_main_profile_flag(json_data_file, schema_file, capsys):
    with patch("sys.argv", ["pipecheck", json_data_file, "--schema", schema_file, "--profile"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "Profile Summary" in captured.out
