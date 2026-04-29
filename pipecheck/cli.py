"""CLI entry point for pipecheck."""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import List, Dict, Any

from pipecheck.schema import PipeSchema
from pipecheck.validator import ValidationResult
from pipecheck.profiler import ProfileReport
from pipecheck.reporter import PipelineReport
from pipecheck.sampler import DataSampler


def load_records(path: str) -> List[Dict[str, Any]]:
    """Load records from a JSON or CSV file."""
    p = Path(path)
    if p.suffix.lower() == ".json":
        with p.open() as fh:
            data = json.load(fh)
        return data if isinstance(data, list) else [data]
    elif p.suffix.lower() == ".csv":
        with p.open(newline="") as fh:
            reader = csv.DictReader(fh)
            return list(reader)
    else:
        raise ValueError(f"Unsupported file format: {p.suffix}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipecheck",
        description="Validate and profile CSV/JSON data pipelines.",
    )
    parser.add_argument("data", help="Path to the data file (CSV or JSON)")
    parser.add_argument("--schema", help="Path to schema JSON file")
    parser.add_argument(
        "--sample",
        type=int,
        default=0,
        metavar="N",
        help="Print a random sample of N records before validation",
    )
    parser.add_argument(
        "--sample-seed",
        type=int,
        default=None,
        metavar="SEED",
        help="Random seed for reproducible sampling",
    )
    parser.add_argument(
        "--profile", action="store_true", help="Display column profile report"
    )
    parser.add_argument(
        "--output",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:  # type: ignore[type-arg]
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        records = load_records(args.data)
    except (ValueError, FileNotFoundError) as exc:
        print(f"Error loading data: {exc}", file=sys.stderr)
        return 1

    # Optional sampling preview
    if args.sample > 0:
        sampler = DataSampler(seed=args.sample_seed)
        sample_result = sampler.random_sample(records, n=args.sample)
        if args.output == "json":
            print(json.dumps(sample_result.to_dict(), indent=2))
        else:
            print(f"--- Sample ({sample_result.sample_size}/{sample_result.total_records} records) ---")
            for rec in sample_result.records:
                print(rec)
        return 0

    schema: PipeSchema | None = None
    if args.schema:
        try:
            with open(args.schema) as fh:
                schema = PipeSchema.from_dict(json.load(fh))
        except (ValueError, FileNotFoundError) as exc:
            print(f"Error loading schema: {exc}", file=sys.stderr)
            return 1

    profile = ProfileReport.from_records(records)

    if args.profile:
        if args.output == "json":
            print(json.dumps(profile.to_dict(), indent=2))
        else:
            print(profile)
        return 0

    if schema is None:
        print("No schema provided. Use --schema to validate.", file=sys.stderr)
        return 1

    from pipecheck.validator import Validator  # type: ignore[attr-defined]

    validator = Validator(schema)
    validation = validator.validate(records)
    report = PipelineReport(validation=validation, profile=profile)

    if args.output == "json":
        print(json.dumps(report.to_dict(), indent=2))
    else:
        print(report.summary())

    return 0 if validation.is_valid else 1


if __name__ == "__main__":
    sys.exit(main())
