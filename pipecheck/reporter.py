"""Reporter module for generating human-readable pipeline check summaries."""

from dataclasses import dataclass
from typing import Optional
from pipecheck.validator import ValidationResult
from pipecheck.profiler import ProfileReport


@dataclass
class PipelineReport:
    """Combined report from validation and profiling."""

    validation: ValidationResult
    profile: Optional[ProfileReport] = None

    def summary(self) -> str:
        lines = []
        lines.append("=" * 50)
        lines.append("PIPECHECK REPORT")
        lines.append("=" * 50)

        # Validation summary
        status = "PASS" if self.validation.is_valid else "FAIL"
        lines.append(f"Validation Status : {status}")
        lines.append(f"Records Checked   : {self.validation.record_count}")
        lines.append(f"Errors Found      : {self.validation.error_count}")

        if not self.validation.is_valid:
            lines.append("\nValidation Errors:")
            for err in self.validation.errors:
                lines.append(f"  - {err}")

        # Profile summary
        if self.profile:
            lines.append("\nProfile Summary:")
            lines.append(f"  Total Records : {self.profile.record_count}")
            lines.append(f"  Columns       : {len(self.profile.columns)}")
            for col_name, col_profile in self.profile.columns.items():
                lines.append(
                    f"  [{col_name}] null_rate={col_profile.null_rate:.1%}, "
                    f"unique={col_profile.unique_count}"
                )

        lines.append("=" * 50)
        return "\n".join(lines)

    def to_dict(self) -> dict:
        result = {
            "validation": {
                "is_valid": self.validation.is_valid,
                "record_count": self.validation.record_count,
                "error_count": self.validation.error_count,
                "errors": [str(e) for e in self.validation.errors],
            }
        }
        if self.profile:
            result["profile"] = self.profile.to_dict()
        return result
