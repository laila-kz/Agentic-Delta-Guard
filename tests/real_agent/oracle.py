"""
tests/real_agent/oracle.py
The Automated Oracle for Agentic Delta Guard.

Compares ground-truth manifest against committed Bronze and Quarantine Delta tables.
Computes the confusion matrix: True Positives, True Negatives, False Positives, False Negatives,
Precision, Recall, Specificity, and Error Signature Accuracy.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pandas as pd

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("oracle")


def load_delta_table(table_path: Path) -> pd.DataFrame:
    """Reads a Delta Lake table via DuckDB delta_scan or parquet files with schema union."""
    if not table_path.exists():
        return pd.DataFrame()

    conn = duckdb.connect()
    posix_path = table_path.as_posix()

    # 1. Try DuckDB delta extension
    try:
        conn.execute("INSTALL delta; LOAD delta;")
        df = conn.execute(f"SELECT * FROM delta_scan('{posix_path}')").df()
        if not df.empty:
            logger.info("Loaded %d rows from Delta table %s via delta_scan", len(df), table_path.name)
            return df
    except Exception as exc:
        logger.debug("delta_scan fallback: %s", exc)

    # 2. Try direct read_parquet with union_by_name=true (handles schema evolution)
    try:
        pattern = f"{posix_path}/*.parquet"
        df = conn.execute(f"SELECT * FROM read_parquet('{pattern}', union_by_name=true)").df()
        if not df.empty:
            logger.info("Loaded %d rows from Parquet glob %s", len(df), table_path.name)
            return df
    except Exception as exc:
        logger.debug("read_parquet direct failed: %s", exc)

    # 3. Try recursive read_parquet with union_by_name=true
    try:
        pattern = f"{posix_path}/**/*.parquet"
        df = conn.execute(f"SELECT * FROM read_parquet('{pattern}', union_by_name=true)").df()
        if not df.empty:
            logger.info("Loaded %d rows from recursive Parquet %s", len(df), table_path.name)
            return df
    except Exception as exc:
        logger.warning("Failed to load table %s: %s", table_path, exc)
    finally:
        conn.close()

    return pd.DataFrame()


def load_manifest(manifest_path: Path) -> List[Dict[str, Any]]:
    """Loads oracle manifest entries from JSONL."""
    if not manifest_path.exists():
        logger.error("Manifest not found: %s", manifest_path)
        return []

    records = []
    with open(manifest_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def _clean_str(val: Any) -> str:
    """Normalizes string values, mapping null/None/NaN/empty to empty string."""
    if val is None or pd.isna(val):
        return ""
    s = str(val).strip()
    if s.lower() in ("nan", "none", "null", "<na>"):
        return ""
    return s


def evaluate_routing(
    manifest_records: List[Dict[str, Any]],
    bronze_df: pd.DataFrame,
    quarantine_df: pd.DataFrame,
) -> Dict[str, Any]:
    """Evaluates actual table rows against expected manifest verdicts."""
    tp = 0  # Expected Bronze, in Bronze
    tn = 0  # Expected Quarantine, in Quarantine
    fp = 0  # Expected Bronze, landed in Quarantine
    fn = 0  # Expected Quarantine, leaked into Bronze (CRITICAL)
    dropped_correct = 0
    dropped_failed = 0
    signature_matches = 0

    per_scenario_stats: Dict[str, Dict[str, int]] = {}
    details: List[Dict[str, Any]] = []

    def _make_key(row_or_dict: Any) -> Tuple[str, str, str]:
        get_fn = row_or_dict.get if hasattr(row_or_dict, "get") else lambda k: row_or_dict[k]
        return (
            _clean_str(get_fn("agent_id")),
            _clean_str(get_fn("session_id")),
            _clean_str(get_fn("action_id")),
        )

    # 1. Index Bronze table
    bronze_keys = set()
    bronze_actions = set()
    if not bronze_df.empty:
        for _, row in bronze_df.iterrows():
            k = _make_key(row)
            bronze_keys.add(k)
            act = _clean_str(row.get("action_id"))
            if act:
                bronze_actions.add(act)

    # 2. Index Quarantine table
    quarantine_keys = set()
    quarantine_actions = set()
    quarantine_raw_payloads = set()
    quarantine_sig_map: Dict[Any, str] = {}
    quarantine_raw_sig_map: Dict[str, str] = {}

    if not quarantine_df.empty:
        for _, row in quarantine_df.iterrows():
            k = _make_key(row)
            quarantine_keys.add(k)
            act = _clean_str(row.get("action_id"))
            sig = str(row.get("error_summary", ""))

            quarantine_sig_map[k] = sig
            if act:
                quarantine_actions.add(act)
                quarantine_sig_map[act] = sig

            if "raw_payload" in row and pd.notna(row["raw_payload"]):
                raw_val = str(row["raw_payload"])
                quarantine_raw_payloads.add(raw_val)
                quarantine_raw_sig_map[raw_val] = sig

    # 3. Evaluate each manifest record against actual destination
    for item in manifest_records:
        scenario = item.get("scenario", "unknown")
        expected_route = item.get("expected_route", "BRONZE")
        expected_sigs = item.get("expected_signatures", [])
        raw_snippet = item.get("raw_payload_snippet")
        key_dict = item.get("key", {})

        key = _make_key(key_dict)
        act_id = _clean_str(key_dict.get("action_id"))

        # Check membership in Bronze
        in_bronze = (key in bronze_keys) or bool(act_id and act_id in bronze_actions)

        # Check membership in Quarantine
        in_quarantine = (
            (key in quarantine_keys)
            or bool(act_id and act_id in quarantine_actions)
            or bool(raw_snippet and any(raw_snippet in p for p in quarantine_raw_payloads))
        )

        actual_route = "UNKNOWN"
        verdict = "INCORRECT"

        if in_bronze and not in_quarantine:
            actual_route = "BRONZE"
        elif in_quarantine and not in_bronze:
            actual_route = "QUARANTINE"
        elif in_bronze and in_quarantine:
            actual_route = "DUAL_ROUTED"
        else:
            actual_route = "ABSENT"

        if scenario not in per_scenario_stats:
            per_scenario_stats[scenario] = {"total": 0, "correct": 0, "failed": 0}
        per_scenario_stats[scenario]["total"] += 1

        # Evaluate against expectation
        if expected_route == "BRONZE":
            if actual_route == "BRONZE":
                tp += 1
                verdict = "CORRECT_TP"
                per_scenario_stats[scenario]["correct"] += 1
            else:
                fp += 1
                verdict = "FALSE_POSITIVE"
                per_scenario_stats[scenario]["failed"] += 1

        elif expected_route == "QUARANTINE":
            if actual_route == "QUARANTINE":
                tn += 1
                verdict = "CORRECT_TN"
                per_scenario_stats[scenario]["correct"] += 1

                # Check error signature
                actual_sig = quarantine_sig_map.get(key, "") or quarantine_sig_map.get(act_id, "")
                if not actual_sig and raw_snippet:
                    for raw_p, s in quarantine_raw_sig_map.items():
                        if raw_snippet in raw_p:
                            actual_sig = s
                            break

                if all(s in actual_sig for s in expected_sigs):
                    signature_matches += 1
            elif actual_route == "BRONZE":
                fn += 1
                verdict = "FALSE_NEGATIVE_BREACH"
                per_scenario_stats[scenario]["failed"] += 1
            else:
                verdict = "QUARANTINE_DROPPED_ABSENT"
                per_scenario_stats[scenario]["failed"] += 1

        elif expected_route == "DROPPED":
            if actual_route == "ABSENT":
                dropped_correct += 1
                verdict = "CORRECT_WATERMARK_DROP"
                per_scenario_stats[scenario]["correct"] += 1
            else:
                dropped_failed += 1
                verdict = "FAILED_TO_DROP"
                per_scenario_stats[scenario]["failed"] += 1

        details.append({
            "key": key_dict,
            "scenario": scenario,
            "expected_route": expected_route,
            "actual_route": actual_route,
            "verdict": verdict,
        })

    total_evaluated = len(manifest_records)
    precision = tp / (tp + fp) if (tp + fp) > 0 else 1.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 1.0
    sig_accuracy = signature_matches / tn if tn > 0 else 1.0

    return {
        "total_manifest_events": total_evaluated,
        "confusion_matrix": {
            "true_positives_tp": tp,
            "true_negatives_tn": tn,
            "false_positives_fp": fp,
            "false_negatives_fn_breach": fn,
            "watermark_dropped_correct": dropped_correct,
            "watermark_dropped_failed": dropped_failed,
        },
        "metrics": {
            "precision": round(precision, 4),
            "recall": round(recall, 4),
            "signature_match_accuracy": round(sig_accuracy, 4),
            "contract_breach_rate": round(fn / max(total_evaluated, 1), 4),
        },
        "per_scenario": per_scenario_stats,
        "details_sample": details[:10],
    }


def print_oracle_report(report: Dict[str, Any], run_id: str) -> None:
    """Prints a beautiful CLI evaluation summary."""
    cm = report["confusion_matrix"]
    m = report["metrics"]

    print("\n" + "=" * 65)
    print("  AGENTIC DELTA GUARD -- THE AUTOMATED ORACLE REPORT")
    print(f"  Run Identifier           : {run_id}")
    print(f"  Total Manifest Events    : {report['total_manifest_events']}")
    print("=" * 65)
    print("  CONFUSION MATRIX & ROUTING AUDIT:")
    print(f"    [TP] Valid -> Bronze (Correct)       : {cm['true_positives_tp']}")
    print(f"    [TN] Poison -> Quarantine (Correct)  : {cm['true_negatives_tn']}")
    print(f"    [FP] Valid -> Quarantine (False Pos) : {cm['false_positives_fp']}")
    print(f"    [FN] Poison -> Bronze (BREACH!)      : {cm['false_negatives_fn_breach']}")
    print(f"    [WD] Late Watermark (Dropped)        : {cm['watermark_dropped_correct']}")
    print("-" * 65)
    print("  ACCURACY & INTEGRITY METRICS:")
    print(f"    Routing Precision                    : {m['precision'] * 100:.2f}%")
    print(f"    Routing Recall                       : {m['recall'] * 100:.2f}%")
    print(f"    Signature Match Accuracy             : {m['signature_match_accuracy'] * 100:.2f}%")
    print(f"    Data Contract Breach Rate            : {m['contract_breach_rate'] * 100:.2f}%")
    print("=" * 65)

    print("\n  PER-SCENARIO BREAKDOWN:")
    for sc, stats in sorted(report["per_scenario"].items()):
        status_flag = "[OK]" if stats["failed"] == 0 else "[FAIL]"
        print(f"    {status_flag} Scenario {sc:8s}: {stats['correct']}/{stats['total']} correct")
    print("=" * 65 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Automated Oracle for Delta Lake Contract Routing")
    parser.add_argument("--run-id", type=str, default=None, help="Run ID under runs/")
    parser.add_argument("--manifest", type=str, default=None, help="Explicit path to manifest.jsonl")
    parser.add_argument("--bronze", type=str, default="data/bronze/agent_events", help="Bronze Delta table path")
    parser.add_argument("--quarantine", type=str, default="data/quarantine/agent_events", help="Quarantine Delta table path")
    args = parser.parse_args()

    # Determine run ID and paths
    if args.manifest:
        manifest_path = Path(args.manifest)
        run_id = manifest_path.parent.name
    elif args.run_id:
        run_id = args.run_id
        manifest_path = PROJECT_ROOT / "runs" / run_id / "manifest.jsonl"
    else:
        # Pick latest directory under runs/
        runs_dir = PROJECT_ROOT / "runs"
        if runs_dir.exists():
            candidates = sorted([p for p in runs_dir.iterdir() if p.is_dir()], key=lambda p: p.stat().st_mtime)
            if candidates:
                run_id = candidates[-1].name
                manifest_path = candidates[-1] / "manifest.jsonl"
            else:
                logger.error("No runs found in runs/ directory.")
                sys.exit(1)
        else:
            logger.error("runs/ directory does not exist.")
            sys.exit(1)

    logger.info("Evaluating Run: %s", run_id)
    manifest = load_manifest(manifest_path)
    if not manifest:
        logger.error("Manifest is empty or missing: %s", manifest_path)
        sys.exit(1)

    bronze_df = load_delta_table(PROJECT_ROOT / args.bronze)
    quarantine_df = load_delta_table(PROJECT_ROOT / args.quarantine)

    report = evaluate_routing(manifest, bronze_df, quarantine_df)
    print_oracle_report(report, run_id)

    # Save oracle report
    report_path = manifest_path.parent / "oracle_report.json"
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
    logger.info("Oracle report saved to %s", report_path)


if __name__ == "__main__":
    main()
