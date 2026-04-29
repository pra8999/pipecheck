"""Export pipeline reports to various output formats (JSON, CSV, plain text)."""

from __future__ import annotations

import csv
import io
import json
from typing import Any

from pipecheck.reporter import PipelineReport


class ExportError(Exception):
    """Raised when an export operation fails."""


class ReportExporter:
    """Exports a PipelineReport to different serialization formats."""

    def __init__(self, report: PipelineReport) -> None:
        self.report = report

    def to_json(self, indent: int = 2) -> str:
        """Serialize the full report as a JSON string."""
        try:
            return json.dumps(self.report.to_dict(), indent=indent, default=str)
        except (TypeError, ValueError) as exc:
            raise ExportError(f"JSON serialization failed: {exc}") from exc

    def to_text(self) -> str:
        """Render a human-readable plain-text summary of the report."""
        lines: list[str] = []
        summary = self.report.summary()
        lines.append("=" * 40)
        lines.append("PipeCheck Pipeline Report")
        lines.append("=" * 40)
        lines.append(f"Status  : {'PASS' if self.report.is_valid else 'FAIL'}")
        lines.append(f"Records : {summary.get('total_records', 0)}")
        lines.append(f"Errors  : {summary.get('total_errors', 0)}")

        validation_errors = summary.get("validation_errors", [])
        if validation_errors:
            lines.append("")
            lines.append("Validation Errors:")
            for err in validation_errors:
                lines.append(f"  - {err}")

        profile_data: dict[str, Any] = summary.get("profile", {})
        if profile_data:
            lines.append("")
            lines.append("Column Profiles:")
            for col, stats in profile_data.items():
                null_rate = stats.get("null_rate", 0.0)
                lines.append(f"  {col}: null_rate={null_rate:.2%}")

        lines.append("=" * 40)
        return "\n".join(lines)

    def to_csv(self) -> str:
        """Export per-column profile stats as CSV."""
        summary = self.report.summary()
        profile_data: dict[str, Any] = summary.get("profile", {})

        output = io.StringIO()
        fieldnames = ["column", "count", "null_count", "null_rate", "unique_count"]
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        for col, stats in profile_data.items():
            writer.writerow(
                {
                    "column": col,
                    "count": stats.get("count", ""),
                    "null_count": stats.get("null_count", ""),
                    "null_rate": f"{stats.get('null_rate', 0.0):.4f}",
                    "unique_count": stats.get("unique_count", ""),
                }
            )

        return output.getvalue()
