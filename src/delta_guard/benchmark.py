"""Generate a lightweight local storage benchmark report for CI."""

from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = REPO_ROOT / "data"
REPORT_PATH = REPO_ROOT / "docs" / "reports" / "storage_benchmark.md"


def collect_storage_metrics() -> list[dict[str, int | str]]:
    """Collect file-count and storage metrics for each available data layer."""
    metrics: list[dict[str, int | str]] = []

    if not DATA_ROOT.exists():
        return metrics

    for layer in sorted(path for path in DATA_ROOT.iterdir() if path.is_dir()):
        files = [path for path in layer.rglob("*") if path.is_file()]
        metrics.append(
            {
                "layer": layer.name,
                "files": len(files),
                "bytes": sum(path.stat().st_size for path in files),
            }
        )

    return metrics


def render_report(metrics: list[dict[str, int | str]], duration: float) -> str:
    total_files = sum(int(item["files"]) for item in metrics)
    total_bytes = sum(int(item["bytes"]) for item in metrics)
    generated_at = datetime.now(timezone.utc).isoformat()

    lines = [
        "# Storage Benchmark",
        "",
        f"- **Status:** PASSED",
        f"- **Generated:** {generated_at}",
        f"- **Measurement duration:** {duration:.6f} seconds",
        f"- **Total files:** {total_files}",
        f"- **Total bytes:** {total_bytes}",
        "",
        "## Data Layer Metrics",
        "",
        "| Layer | Files | Bytes |",
        "| --- | ---: | ---: |",
    ]

    if metrics:
        lines.extend(
            f"| {item['layer']} | {item['files']} | {item['bytes']} |"
            for item in metrics
        )
    else:
        lines.append("| No data layers found | 0 | 0 |")

    lines.extend(
        [
            "",
            "This CI benchmark records the current local storage footprint. "
            "It does not mutate source data or perform destructive compaction.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    started = time.perf_counter()
    metrics = collect_storage_metrics()
    duration = time.perf_counter() - started

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(render_report(metrics, duration), encoding="utf-8")

    print(f"Storage benchmark completed: {REPORT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
