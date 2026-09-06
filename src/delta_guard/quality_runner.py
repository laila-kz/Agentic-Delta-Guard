import argparse
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = REPO_ROOT / "dbt_delta_guard"
DEFAULT_MARKDOWN_REPORT_PATH = REPO_ROOT / "docs" / "reports" / "quality_audit.md"
JSON_REPORT_PATH = REPO_ROOT / "docs" / "reports" / "quality_report.json"
SUMMARY_PATTERN = re.compile(
	r"PASS=(?P<pass>\d+)\s+WARN=(?P<warn>\d+)\s+"
	r"ERROR=(?P<error>\d+)\s+SKIP=(?P<skip>\d+)\s+"
	r"NO-OP=(?P<no_op>\d+)\s+TOTAL=(?P<total>\d+)"
)


def parse_summary(output: str) -> dict[str, int]:
	match = SUMMARY_PATTERN.search(output)
	if match is None:
		return {}
	return {key: int(value) for key, value in match.groupdict().items()}


def build_report(
	result: subprocess.CompletedProcess[str],
	started_at: datetime,
	finished_at: datetime,
) -> dict:
	output = f"{result.stdout}\n{result.stderr}".strip()
	return {
		"status": "passed" if result.returncode == 0 else "failed",
		"returncode": result.returncode,
		"command": ["dbt", "test", "--profiles-dir", "."],
		"project_directory": str(DBT_PROJECT_DIR),
		"started_at": started_at.isoformat(),
		"finished_at": finished_at.isoformat(),
		"duration_seconds": round((finished_at - started_at).total_seconds(), 3),
		"summary": parse_summary(output),
		"stdout": result.stdout,
		"stderr": result.stderr,
	}


def render_markdown(report: dict) -> str:
	lines = [
		"# Week 2 Quality Audit",
		"",
		f"- **Status:** {report['status'].upper()}",
		f"- **Return code:** {report['returncode']}",
		f"- **Started:** {report['started_at']}",
		f"- **Finished:** {report['finished_at']}",
		f"- **Duration:** {report['duration_seconds']} seconds",
		"",
		"## Test Summary",
		"",
	]

	if report["summary"]:
		lines.extend(
			[
				"| Result | Count |",
				"| --- | ---: |",
				f"| Passed | {report['summary'].get('pass', 0)} |",
				f"| Warnings | {report['summary'].get('warn', 0)} |",
				f"| Errors | {report['summary'].get('error', 0)} |",
				f"| Skipped | {report['summary'].get('skip', 0)} |",
				f"| Total | {report['summary'].get('total', 0)} |",
			]
		)
	else:
		lines.append("No dbt test summary was found in the command output.")

	lines.extend(
		[
			"",
			"## dbt Output",
			"",
			"```text",
			report["stdout"].rstrip(),
			report["stderr"].rstrip(),
			"```",
			"",
		]
	)
	return "\n".join(lines)


def run_quality_checks(report_path: Path) -> int:
	started_at = datetime.now(timezone.utc)
	result = subprocess.run(
		["dbt", "test", "--profiles-dir", "."],
		cwd=DBT_PROJECT_DIR,
		capture_output=True,
		text=True,
		check=False,
	)
	finished_at = datetime.now(timezone.utc)

	report = build_report(result, started_at, finished_at)
	report_path.parent.mkdir(parents=True, exist_ok=True)
	report_path.write_text(render_markdown(report), encoding="utf-8")
	JSON_REPORT_PATH.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

	print(f"Quality checks: {report['status'].upper()}")
	print(f"Report: {report_path}")
	if report["summary"]:
		print(f"Summary: {report['summary']}")
	return 0 if result.returncode == 0 else 1


def main() -> int:
	parser = argparse.ArgumentParser(description="Run dbt quality checks and write a JSON report.")
	parser.add_argument(
		"--report-path",
		type=Path,
		default=DEFAULT_MARKDOWN_REPORT_PATH,
		help="Path for the generated Markdown report.",
	)
	args = parser.parse_args()
	return run_quality_checks(args.report_path)


if __name__ == "__main__":
	sys.exit(main())
