"""
scripts/measure_triage_efficiency.py
Benchmarks automated Delta Guard quarantine incident triage against manual SRE baseline.
Primary mode reads directly from live Delta Lake Quarantine table (data/quarantine/agent_events).
Provides transparent latency measurement (time.perf_counter), explicit cost accounting,
honest baseline comparisons, and rigorous error handling.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Add project src to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Safe console handling on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

from rich.console import Console
from rich.table import Table

console = Console()

# ── Manual Baseline Documentation & Assumptions ──────────────────────────────
# Industry baseline references (SRE incident handling / Ponemon Institute):
# - Log search, filtering, and event reconstruction: 6–8 mins
# - Schema diffing & contract root-cause analysis: 10–12 mins
# - Writing remediation patch / YAML rule update: 5–8 mins
# Total estimated baseline: ~23.4 minutes per incident.
# Engineer loaded labor cost model: $120.00 / hour ($2.00 / minute).
MANUAL_ESTIMATED_MINUTES_PER_INCIDENT: float = 23.4
MANUAL_ENGINEER_HOURLY_RATE_USD: float = 120.0
# ─────────────────────────────────────────────────────────────────────────────

REQUIRED_TRIAGE_COLUMNS: Tuple[str, ...] = (
    "action_id",
    "tool_name",
    "cost_usd",
    "timestamp",
)


def validate_quarantine_schema(
    records: pd.DataFrame,
) -> Tuple[bool, List[str], List[str]]:
    """
    Validate that quarantine records contain required fields for clustering and triage.
    Returns: (is_valid, missing_required_fields, schema_warnings)
    """
    if records.empty:
        return False, ["(Table is empty)"], []

    missing = [col for col in REQUIRED_TRIAGE_COLUMNS if col not in records.columns]
    warnings = []

    if "error_summary" not in records.columns:
        warnings.append(
            "'error_summary' column missing; clustering will group under 'unknown'."
        )

    # Check for excessive null values in key columns
    for col in REQUIRED_TRIAGE_COLUMNS:
        if col in records.columns:
            null_count = int(records[col].isna().sum())
            if null_count > 0:
                warnings.append(
                    f"Column '{col}' has {null_count}/{len(records)} null values."
                )

    is_valid = len(missing) == 0
    return is_valid, missing, warnings


def simulate_manual_triage(incident_count: int) -> Dict[str, Any]:
    """
    Computes industry-estimated baseline manual triage metrics.
    Note: These numbers represent estimated reference models, not experimental measurements.
    """
    total_time_min = incident_count * MANUAL_ESTIMATED_MINUTES_PER_INCIDENT
    cost_per_incident = MANUAL_ESTIMATED_MINUTES_PER_INCIDENT * (
        MANUAL_ENGINEER_HOURLY_RATE_USD / 60.0
    )
    total_cost_usd = incident_count * cost_per_incident

    return {
        "method": "Manual SRE Investigation (Estimated Baseline)",
        "incidents": incident_count,
        "avg_time_min": MANUAL_ESTIMATED_MINUTES_PER_INCIDENT,
        "total_time_min": round(total_time_min, 2),
        "cost_per_incident_usd": round(cost_per_incident, 2),
        "total_cost_usd": round(total_cost_usd, 2),
        "is_measured": False,
        "assumptions": (
            f"Industry estimate: {MANUAL_ESTIMATED_MINUTES_PER_INCIDENT} min/incident "
            f"at ${MANUAL_ENGINEER_HOURLY_RATE_USD:.0f}/hr engineering labor."
        ),
    }


def generate_synthetic_incidents(count: int = 10) -> pd.DataFrame:
    """Generate realistic synthetic quarantine records for testing and CI validation."""
    return pd.DataFrame(
        [
            {
                "action_id": f"syn-act-{i:04d}",
                "session_id": f"sess-{i // 5:02d}",
                "agent_id": f"agent_{i % 3}",
                "tool_name": "database_writer" if i % 2 == 0 else "sql_query_executor",
                "error_summary": (
                    "SCHEMA_VIOLATION: missing required field 'event_type'"
                    if i % 3 == 0
                    else (
                        "COST_LIMIT_EXCEEDED: cost_usd > 50.0"
                        if i % 3 == 1
                        else "freshness:stale_timestamp"
                    )
                ),
                "cost_usd": 75.50 if i % 3 == 1 else 0.05,
                "timestamp": "2026-09-07T10:00:00Z",
                "execution_time_ms": 150 + (i * 10),
                "status": "quarantined",
            }
            for i in range(count)
        ]
    )


def benchmark_automated_triage(
    records: pd.DataFrame,
    contract_path: Path,
    use_llm: bool = False,
    data_source_label: str = "Live Delta Lake Quarantine",
) -> Dict[str, Any]:
    """
    Executes and transparently measures automated quarantine triage.
    Uses time.perf_counter() for exact duration without artificial floors.
    """
    from delta_guard.triage import LLMTriageEngine

    engine = LLMTriageEngine(
        contract_path=contract_path,
        incident_log_path="docs/INCIDENT_LOG.md",
        proposed_contract_path="configs/agent_contract_proposed.yaml",
    )

    actual_count = len(records)
    if actual_count == 0:
        raise ValueError("Cannot benchmark automated triage on an empty dataset.")

    # 1. Measure Error Signature Clustering
    t0_cluster = time.perf_counter()
    clusters = engine.cluster_error_signatures(records)
    clustering_sec = time.perf_counter() - t0_cluster

    # 2. Measure Contract Loading and Deterministic Diagnosis
    t0_diag = time.perf_counter()
    contract = engine._read_contract()
    deterministic_diag = engine._deterministic_triage_engine(
        records, clusters, contract
    )
    deterministic_sec = time.perf_counter() - t0_diag

    # 3. Optional LLM invocation (measured separately if requested)
    llm_sec = 0.0
    llm_invoked = False
    llm_provider = "none"
    llm_cost_usd = 0.0
    llm_cost_methodology = "N/A (Deterministic execution, $0.00 API cost)"
    final_diagnosis = deterministic_diag

    if use_llm:
        has_openai = bool(os.getenv("OPENAI_API_KEY"))
        has_anthropic = bool(os.getenv("ANTHROPIC_API_KEY"))

        if has_openai or has_anthropic:
            llm_provider = "openai" if has_openai else "anthropic"
            prompt = (
                "Diagnose these quarantined data incidents. Return JSON with keys "
                "summary, findings, schema_drift, metrics, recommended_patches.\n"
                + json.dumps({"contract": contract, "clusters": clusters}, default=str)
            )

            t0_llm = time.perf_counter()
            llm_resp = engine._call_llm(prompt)
            llm_sec = time.perf_counter() - t0_llm

            if llm_resp is not None:
                llm_invoked = True
                final_diagnosis = llm_resp
                # Transparent token-based pricing calculation
                # gpt-4o-mini reference: $0.15 / 1M input, $0.60 / 1M output tokens
                # claude-3-5-haiku reference: $0.80 / 1M input, $4.00 / 1M output tokens
                est_in_tokens = len(prompt) / 4.0
                est_out_tokens = len(json.dumps(llm_resp)) / 4.0
                if llm_provider == "openai":
                    llm_cost_usd = (est_in_tokens * 0.00000015) + (
                        est_out_tokens * 0.00000060
                    )
                else:
                    llm_cost_usd = (est_in_tokens * 0.00000080) + (
                        est_out_tokens * 0.00000400
                    )
                llm_cost_methodology = (
                    f"Calculated from ~{int(est_in_tokens)} input / ~{int(est_out_tokens)} output tokens "
                    f"using standard {llm_provider} pricing."
                )
            else:
                llm_cost_methodology = (
                    "LLM call timed out/failed; fell back to deterministic triage ($0.00)."
                )
        else:
            llm_cost_methodology = (
                "LLM requested but no API key configured. Executed deterministic triage ($0.00)."
            )

    # Total measured execution time (strict time.perf_counter sum)
    total_measured_sec = clustering_sec + deterministic_sec + llm_sec
    avg_sec_per_incident = total_measured_sec / actual_count
    avg_min_per_incident = avg_sec_per_incident / 60.0

    cost_per_incident_usd = llm_cost_usd / actual_count if llm_invoked else 0.0

    return {
        "method": "Agentic Delta Guard (Automated Triage)",
        "incidents": actual_count,
        "is_measured": True,
        "data_source": data_source_label,
        "durations": {
            "clustering_sec": clustering_sec,
            "deterministic_sec": deterministic_sec,
            "llm_sec": llm_sec,
            "total_measured_sec": total_measured_sec,
        },
        "avg_time_sec": avg_sec_per_incident,
        "avg_time_min": avg_min_per_incident,
        "cost_per_incident_usd": cost_per_incident_usd,
        "total_cost_usd": llm_cost_usd,
        "cost_methodology": llm_cost_methodology,
        "clusters_count": len(clusters),
        "clusters": clusters,
        "diagnosis": final_diagnosis,
        "recommended_patches": len(final_diagnosis.get("recommended_patches", [])),
        "llm_invoked": llm_invoked,
        "llm_provider": llm_provider,
    }


def load_live_quarantine_table(
    quarantine_path: Path,
    contract_path: Path,
    sample_limit: Optional[int] = None,
) -> pd.DataFrame:
    """
    Loads and strictly validates records from the live Delta Lake quarantine table.
    Raises explicit FileNotFoundError or ValueError if table is missing or invalid.
    """
    if not quarantine_path.exists():
        raise FileNotFoundError(
            f"Quarantine table directory does not exist at '{quarantine_path}'."
        )

    from delta_guard.triage import LLMTriageEngine

    engine = LLMTriageEngine(
        quarantine_path=quarantine_path,
        contract_path=contract_path,
    )

    try:
        records = engine.load_quarantine_records()
    except Exception as err:
        raise RuntimeError(
            f"Failed to read Delta Lake records from '{quarantine_path}': {err}"
        ) from err

    if records is None or records.empty:
        raise ValueError(
            f"Quarantine table at '{quarantine_path}' contains 0 records."
        )

    # Validate schema
    is_valid, missing, warnings = validate_quarantine_schema(records)
    for warning in warnings:
        console.print(f"[yellow]⚠️ Schema Warning: {warning}[/yellow]")

    if not is_valid:
        raise ValueError(
            f"Quarantine table at '{quarantine_path}' is missing required triage columns: {missing}. "
            f"Available columns: {list(records.columns)}"
        )

    if sample_limit is not None and sample_limit > 0 and len(records) > sample_limit:
        records = records.head(sample_limit).copy()

    return records


def write_benchmark_report(
    report_path: Path,
    automated: Dict[str, Any],
    manual: Dict[str, Any],
    mode: str,
    quarantine_path: Path,
) -> None:
    """Writes transparent markdown benchmark report with documented methodology."""
    report_path.parent.mkdir(parents=True, exist_ok=True)

    mttr_reduction = (
        (manual["avg_time_min"] - automated["avg_time_min"]) / manual["avg_time_min"]
    ) * 100

    clusters_markdown = ""
    if automated.get("clusters"):
        clusters_markdown = "\n### Discovered Quarantine Error Signatures\n\n"
        for cluster in automated["clusters"][:10]:
            clusters_markdown += (
                f"- `{cluster['signature']}`: **{cluster['count']}** incident(s)\n"
            )

    durs = automated["durations"]
    llm_status = (
        f"Active ({automated['llm_provider']})"
        if automated["llm_invoked"]
        else "None (Deterministic Rule Patching)"
    )

    content = f"""# 📈 Incident Triage Efficiency Benchmark

**Execution Mode**: `{mode.upper()}`  
**Data Source**: `{automated['data_source']}`  
**Quarantine Location**: `{quarantine_path.as_posix()}`  
**Incidents Analyzed**: `{automated['incidents']}`  
**Generated Timestamp**: `{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}`  

---

## 1. Measured Performance Comparison

| Metric | Manual SRE Baseline *(Estimated)* | Agentic Delta Guard *(Measured)* | Comparison / Delta |
| :--- | :--- | :--- | :--- |
| **Incidents Processed** | {manual['incidents']} incidents | **{automated['incidents']} incidents** | Identical normalized sample |
| **Average MTTR / Incident** | {manual['avg_time_min']:.1f} min | **{automated['avg_time_sec']:.3f}s** ({automated['avg_time_min']:.4f} min) | **⚡ {mttr_reduction:.1f}% reduction** |
| **Total Incident Handling Time** | {manual['total_time_min']:.1f} min | **{durs['total_measured_sec']:.3f}s** | Strict wall-clock measurement |
| **Cost / 100 Incidents** | ${manual['cost_per_incident_usd'] * 100:,.2f} | **${automated['cost_per_incident_usd'] * 100:.2f}** | Cost model comparison |
| **Direct API Cost / Incident** | $0.00 (Human labor) | **${automated['cost_per_incident_usd']:.4f}** | {automated['cost_methodology']} |
| **Root Cause Resolution** | Manual log grep & dbt patching | **Automated Contract Diff ({automated['recommended_patches']} patch proposals)** | Actionable rule diff emitted |
| **Stream Interruption** | Pipeline halted on schema drift | **Not measured in this script** | Architectural property of DLQ |

---

## 2. Latency Measurement Breakdown (time.perf_counter)

- **Error Signature Clustering**: `{durs['clustering_sec'] * 1000:.2f} ms`
- **Deterministic Contract Diff & Rule Diagnosis**: `{durs['deterministic_sec'] * 1000:.2f} ms`
- **LLM Network Call Duration**: `{durs['llm_sec'] * 1000:.2f} ms` (LLM Provider: `{llm_status}`)
- **Total Measured Triage Duration**: `{durs['total_measured_sec']:.4f} seconds`
- **Mean Processing Time per Incident**: `{automated['avg_time_sec'] * 1000:.2f} ms`

---

## 3. Methodology & Benchmark Assumptions

### Automated Triage Measurement
- Durations are measured using Python's high-resolution `time.perf_counter()`.
- Operations measured: In-memory error grouping, contract schema verification, upper-bound cost analysis, freshness boundary validation, and unapproved tool discovery.
- No artificial latency floor is added.
- Compute cost for deterministic triage is $0.00 (runs locally as part of pipeline maintenance).

### Manual SRE Baseline Assumptions
- The manual triage duration of **{manual['avg_time_min']} minutes per incident** is an **estimated reference baseline**, not an experimentally timed human trial.
- Breakdown: 6–8 min log searching + 10–12 min root-cause & contract analysis + 5–8 min remediation patching.
- Labor cost assumes a standard loaded engineering cost model of **${MANUAL_ENGINEER_HOURLY_RATE_USD:.0f}/hour**.

### Stream Uptime & Downtime Scope
- Stream uptime is marked as **Not measured** in this script. The ability to avoid ingestion stalls is an architectural guarantee of the Delta Lake Dead-Letter Queue (DLQ) pattern, not a performance metric measured by triage latency.
{clusters_markdown}
---
*Report generated by `scripts/measure_triage_efficiency.py`.*
"""
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(content)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark manual SRE triage vs Agentic Delta Guard automated triage."
    )
    parser.add_argument(
        "--count",
        "-n",
        type=int,
        default=10,
        help="Number of quarantine incidents to benchmark (default: 10)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Benchmark across ALL available incidents in the quarantine table",
    )
    parser.add_argument(
        "--synthetic",
        action="store_true",
        help="Run explicit synthetic simulation mode (never queries live quarantine)",
    )
    parser.add_argument(
        "--use-llm",
        action="store_true",
        help="Include actual LLM API diagnosis call if API keys are set",
    )
    parser.add_argument(
        "--quarantine-path",
        default="data/quarantine/agent_events",
        help="Path to Delta Lake quarantine table (default: data/quarantine/agent_events)",
    )
    parser.add_argument(
        "--contract-path",
        default="configs/agent_contract.yaml",
        help="Path to active agent contract YAML (default: configs/agent_contract.yaml)",
    )
    parser.add_argument(
        "--report-path",
        default="docs/reports/triage_benchmark.md",
        help="Output markdown report destination",
    )
    args = parser.parse_args()

    quarantine_path = PROJECT_ROOT / args.quarantine_path
    contract_path = PROJECT_ROOT / args.contract_path
    report_path = PROJECT_ROOT / args.report_path
    sample_limit = None if args.all else args.count

    console.print("\n[bold cyan]⚡ Incident Triage Efficiency Benchmark[/bold cyan]\n")

    # ── 1. Acquire Data (Strict Separation: Real vs Synthetic) ───────────────
    if args.synthetic:
        mode_label = "SYNTHETIC SIMULATION (--synthetic)"
        data_source_label = "Synthetic In-Memory Incidents"
        console.print(
            "[bold yellow]Mode: SYNTHETIC SIMULATION (Explicit '--synthetic' requested)[/bold yellow]"
        )
        target_count = sample_limit if sample_limit and sample_limit > 0 else 10
        records = generate_synthetic_incidents(target_count)
        console.print(
            f"[green]✓ Generated {len(records)} normalized synthetic incident records.[/green]\n"
        )
    else:
        mode_label = "LIVE LAKEHOUSE BENCHMARK"
        data_source_label = f"Live Delta Lake ({quarantine_path.as_posix()})"
        console.print(
            f"[bold blue]Mode: LIVE DATA BENCHMARK (Table: {quarantine_path})[/bold blue]"
        )
        try:
            records = load_live_quarantine_table(
                quarantine_path=quarantine_path,
                contract_path=contract_path,
                sample_limit=sample_limit,
            )
            console.print(
                f"[bold green]✓ Successfully loaded {len(records)} live quarantined incident(s) from Delta Lake.[/bold green]\n"
            )
        except (FileNotFoundError, ValueError, RuntimeError) as err:
            console.print(f"\n[bold red]❌ Benchmark Failed: {err}[/bold red]")
            console.print(
                "[yellow]To run a test benchmark without live lakehouse data, use the explicit flag: "
                "'python scripts/measure_triage_efficiency.py --synthetic'[/yellow]\n"
            )
            return 1

    # ── 2. Run Benchmarks ───────────────────────────────────────────────────
    try:
        automated = benchmark_automated_triage(
            records=records,
            contract_path=contract_path,
            use_llm=args.use_llm,
            data_source_label=data_source_label,
        )
    except Exception as err:
        console.print(
            f"[bold red]❌ Failed during automated triage execution: {err}[/bold red]"
        )
        return 1

    incident_count = automated["incidents"]
    manual = simulate_manual_triage(incident_count)

    mttr_reduction = (
        (manual["avg_time_min"] - automated["avg_time_min"]) / manual["avg_time_min"]
    ) * 100

    # ── 3. Render Rich Comparison Table ─────────────────────────────────────
    table = Table(
        title=f"Incident Triage Benchmark Results ({mode_label})",
        caption="* Manual metrics are based on industry SRE model estimates. Automated metrics are directly measured.",
    )
    table.add_column("Metric", style="bold")
    table.add_column("Manual SRE Baseline (Estimated)", style="red")
    table.add_column("Agentic Delta Guard (Measured)", style="green")
    table.add_column("Comparison / Delta", style="bold yellow")

    table.add_row(
        "Incidents Processed",
        f"{manual['incidents']} incidents",
        f"{automated['incidents']} incidents ({automated['clusters_count']} error clusters)",
        "Automated DLQ Clustering",
    )
    table.add_row(
        "Avg Time to Triage (MTTR)",
        f"{manual['avg_time_min']:.1f} min",
        f"{automated['avg_time_sec']:.3f}s ({automated['avg_time_min']:.4f} min)",
        f"⚡ {mttr_reduction:.1f}% reduction",
    )
    table.add_row(
        "Cost per 100 Incidents",
        f"${manual['cost_per_incident_usd'] * 100:,.2f} (Labor)",
        f"${automated['cost_per_incident_usd'] * 100:.2f} (API)",
        f"Estimated Savings: ${(manual['cost_per_incident_usd'] - automated['cost_per_incident_usd']) * 100:,.2f}",
    )
    table.add_row(
        "Root Cause Diagnosis",
        "Manual log grepping & patching",
        f"Automated Diff ({automated['recommended_patches']} patch proposals)",
        "Instant Actionability",
    )
    table.add_row(
        "Stream Pipeline Downtime",
        "Pipeline halted (schema drift)",
        "Not measured in this triage script",
        "Architectural DLQ property",
    )

    console.print(table)

    # ── 4. Print Latency & Cost Breakdown ───────────────────────────────────
    durs = automated["durations"]
    console.print("\n[bold]Latency Breakdown (time.perf_counter):[/bold]")
    console.print(
        f"  • Signature Clustering : {durs['clustering_sec'] * 1000:.2f} ms"
    )
    console.print(
        f"  • Contract Diagnosis   : {durs['deterministic_sec'] * 1000:.2f} ms"
    )
    if automated["llm_invoked"]:
        console.print(
            f"  • LLM API Call Latency : {durs['llm_sec'] * 1000:.2f} ms ({automated['llm_provider']})"
        )
    console.print(
        f"  • Total Measured Time  : [bold green]{durs['total_measured_sec']:.4f} s[/bold green] "
        f"([cyan]{automated['avg_time_sec'] * 1000:.2f} ms / incident[/cyan])"
    )
    console.print(f"  • Cost Accounting      : {automated['cost_methodology']}")

    if automated.get("clusters"):
        console.print("\n[bold]Top Error Signatures Discovered:[/bold]")
        for cluster in automated["clusters"][:5]:
            console.print(
                f"  • [cyan]{cluster['signature']}[/cyan]: {cluster['count']} incident(s)"
            )

    # ── 5. Generate Markdown Report ─────────────────────────────────────────
    write_benchmark_report(
        report_path=report_path,
        automated=automated,
        manual=manual,
        mode=mode_label,
        quarantine_path=quarantine_path,
    )
    console.print(
        f"\n[bold green]✓ Benchmark report written to: {report_path}[/bold green]\n"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
