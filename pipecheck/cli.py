"""CLI entry point for pipecheck."""

import argparse
import json
import sys
from pathlib import Path

from pipecheck.schema import PipeSchema
from pipecheck.validator import Validator
from pipecheck.profiler import Profiler
from pipecheck.reporter import PipelineReport


def load_records(file_path: str) -> list[dict]:
    path = Path(file_path)
    if not path.exists():
        print(f"Error: File not found: {file_path}", file=sys.stderr)
        sys.exit(1)

    suffix = path.suffix.lower()
    if suffix == ".json":
        with open(path) as f:
            data = json.load(f)
        return data if isinstance(data, list) else [data]
    elif suffix == ".csv":
        import csv
        with open(path, newline="") as f:
            reader = csv.DictReader(f)
            return list(reader)
    else:
        print(f"Error: Unsupported file format '{suffix}'. Use .csv or .json", file=sys.stderr)
        sys.exit(1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pipecheck",
        description="Validate and profile CSV/JSON data pipelines.",
    )
    parser.add_argument("data", help="Path to data file (.csv or .json)")
    parser.add_argument("--schema", help="Path to schema JSON file", default=None)
    parser.add_argument("--profile", action="store_true", help="Include profiling report")
    parser.add_argument("--output", choices=["text", "json"], default="text", help="Output format")
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    records = load_records(args.data)

    schema = None
    if args.schema:
        with open(args.schema) as f:
            schema_data = json.load(f)
        schema = PipeSchema.from_dict(schema_data)

    validation_result = Validator(schema).validate(records) if schema else None
    profile_report = Profiler().profile(records) if args.profile else None

    if validation_result is None and profile_report is None:
        print("No schema or --profile flag provided. Nothing to do.", file=sys.stderr)
        sys.exit(1)

    if validation_result:
        report = PipelineReport(validation=validation_result, profile=profile_report)
        if args.output == "json":
            print(json.dumps(report.to_dict(), indent=2))
        else:
            print(report.summary())
        sys.exit(0 if validation_result.is_valid else 2)
    elif profile_report:
        if args.output == "json":
            print(json.dumps(profile_report.to_dict(), indent=2))
        else:
            print(f"Records: {profile_report.record_count}")
            for col, p in profile_report.columns.items():
                print(f"  {col}: null_rate={p.null_rate:.1%}, unique={p.unique_count}")


if __name__ == "__main__":
    main()
