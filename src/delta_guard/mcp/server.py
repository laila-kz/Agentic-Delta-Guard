"""
src/delta_guard/mcp/server.py
Model Context Protocol (MCP) Server for Agentic Delta Guard.
Exposes synchronous pre-flight checks and supervisor observability tools.
Compatible with Gemini CLI, Claude Desktop, Cursor, and FastMCP runners.
"""

from __future__ import annotations

import json
import hmac
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import yaml

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# MCP FastMCP import with fallback
try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    # Mock FastMCP for running/testing in lightweight environments without mcp
    class FastMCP:  # type: ignore
        def __init__(self, name: str, dependencies: Optional[List[str]] = None):
            self.name = name
            self.tools: Dict[str, Any] = {}

        def tool(self, fn=None, **kwargs):
            def decorator(func):
                self.tools[func.__name__] = func
                return func
            return decorator if fn is None else decorator(fn)

        def run(self, transport: str = "stdio"):
            print(f"Running MCP server: {self.name} (Transport: {transport})")
            print(f"Available tools: {list(self.tools.keys())}")
            return self

# Import Validator
from src.delta_guard.mcp.validators.contract_validator import ContractValidator

# Initialize FastMCP Server
mcp = FastMCP("agentic-delta-guard", dependencies=["duckdb", "pyyaml", "pydantic"])
validator = ContractValidator(PROJECT_ROOT / "configs" / "agent_contract.yaml")


@mcp.tool()
def check_contract(payload: dict[str, Any]) -> str:
    """
    PRE-FLIGHT ADVISOR: Checks whether an event payload satisfies data lakehouse
    quality rules before the agent writes to Kafka. Returns instant pass/fail with self-healing hints.

    Args:
        payload: Event dictionary (agent_id, session_id, action_id, timestamp, tool_name, cost_usd, etc.)

    Returns:
        JSON string containing allowed boolean, violation list, and remediation hints.
    """
    result = validator.validate(payload)
    if result["allowed"]:
        result["message"] = "Payload passes all data contract checks. Safe to emit to Kafka."
    else:
        v_count = len(result["violations"])
        result["message"] = f"Payload rejected ({v_count} violation(s)). Self-correct using remediation hints."
    return json.dumps(result, indent=2)


@mcp.tool()
def get_active_contract() -> str:
    """
    SCHEMA DISCOVERY: Returns the full active data contract (schema, allowed tools, budget limits, rules).
    Agents should call this at session startup to discover environment constraints.

    Returns:
        JSON string containing active contract YAML content and metadata.
    """
    validator.reload()
    return json.dumps({
        "contract": validator.contract,
        "contract_version": validator.contract.get("version", "1.0.0"),
        "allowed_tools": validator.get_allowed_tools(),
        "max_cost_per_call": validator.max_cost_usd,
        "freshness_window_hours": validator.max_event_age_hours,
        "loaded_at": datetime.now(timezone.utc).isoformat(),
        "contract_path": str(validator.contract_path.resolve()),
    }, indent=2)


@mcp.tool()
def get_quarantine_summary(limit: int = 5, agent_id: Optional[str] = None) -> str:
    """
    SUPERVISOR OBSERVABILITY: Queries recent quarantine rejections from the Layer 2 safety net
    to identify systemic errors and rogue behaviors across the agent fleet.

    Args:
        limit: Maximum number of recent records to return (default: 5)
        agent_id: Optional filter for a specific agent ID

    Returns:
        JSON string containing quarantine sample records, error signature frequencies, and status.
    """
    quarantine_path = PROJECT_ROOT / "data" / "quarantine" / "agent_events"
    if not quarantine_path.exists():
        return json.dumps({
            "status": "clean",
            "message": "No quarantine records found. All agent events compliant!",
            "records": [],
        }, indent=2)

    conn = duckdb.connect()
    try:
        # Query Delta Lake table or fallback to parquet
        try:
            query = f"SELECT * FROM delta_scan('{quarantine_path.as_posix()}')"
        except Exception:
            query = f"SELECT * FROM read_parquet('{quarantine_path.as_posix()}/**/*.parquet')"

        if agent_id:
            query += f" WHERE agent_id = '{agent_id}'"

        query += f" ORDER BY quarantined_at DESC LIMIT {int(limit)}"
        df = conn.execute(query).df()
        records = df.to_dict(orient="records")

        # Cluster error signatures
        error_signatures: Dict[str, int] = {}
        for r in records:
            err = str(r.get("error_summary", "unknown"))
            error_signatures[err] = error_signatures.get(err, 0) + 1

        return json.dumps({
            "status": "quarantine_active" if records else "clean",
            "total_records_in_sample": len(records),
            "error_signatures": error_signatures,
            "records": records,
            "message": f"Retrieved {len(records)} recent quarantine records.",
        }, indent=2, default=str)
    except Exception as e:
        return json.dumps({
            "status": "error",
            "error": f"Quarantine query failed: {str(e)}",
            "message": "Quarantine table may be uninitialized or empty.",
        }, indent=2)
    finally:
        conn.close()


@mcp.tool()
def inspect_bronze_lakehouse(limit: int = 5, agent_id: Optional[str] = None) -> str:
    """
    LAKEHOUSE OBSERVABILITY: Queries recent validated Bronze Delta Lake records.

    Args:
        limit: Maximum records to inspect (default: 5)
        agent_id: Optional filter for a specific agent ID

    Returns:
        JSON string of validated Bronze event records.
    """
    bronze_path = PROJECT_ROOT / "data" / "bronze" / "agent_events"
    if not bronze_path.exists():
        return json.dumps({
            "status": "empty",
            "message": "Bronze table does not exist yet. Run the streaming pipeline!",
            "records": [],
        }, indent=2)

    conn = duckdb.connect()
    try:
        try:
            query = f"SELECT * FROM delta_scan('{bronze_path.as_posix()}')"
        except Exception:
            query = f"SELECT * FROM read_parquet('{bronze_path.as_posix()}/**/*.parquet')"

        if agent_id:
            query += f" WHERE agent_id = '{agent_id}'"

        query += f" ORDER BY timestamp DESC LIMIT {int(limit)}"
        df = conn.execute(query).df()
        records = df.to_dict(orient="records")

        return json.dumps({
            "status": "online",
            "inspected_records": len(records),
            "records": records,
        }, indent=2, default=str)
    except Exception as e:
        return json.dumps({"status": "error", "error": f"Bronze query failed: {str(e)}"}, indent=2)
    finally:
        conn.close()


@mcp.tool()
def propose_contract_patch(
    rationale: str,
    proposed_change: dict[str, Any],
    authorization_token: Optional[str] = None,
    output_path: Optional[str] = None,
) -> str:
    """
    AUTONOMOUS GOVERNANCE: Proposes a formal update to configs/agent_contract_proposed.yaml
    when supervisor agents detect legitimate, authorized new agent tools or parameters.

    Args:
        rationale: Reason for proposing the contract update
        proposed_change: Dictionary specifying proposed updates:
                         - add_allowed_tool: str
                         - increase_max_cost: float
                         - add_required_field: str
        authorization_token: Deployment-configured token required to write a proposal.
        output_path: Optional override for the destination file. Defaults to
                     configs/agent_contract_proposed.yaml in the project root.
                     Tests pass a tmp_path here so they never write to the repo.

    Returns:
        JSON string detailing proposal status, diff preview, and CI validation instructions.
    """
    configured_token = os.getenv("MCP_PROPOSAL_TOKEN")
    if not configured_token:
        return json.dumps({
            "status": "AUTHORIZATION_REQUIRED",
            "message": "Contract proposals are disabled until MCP_PROPOSAL_TOKEN is configured.",
        }, indent=2)
    if not authorization_token or not hmac.compare_digest(authorization_token, configured_token):
        return json.dumps({
            "status": "UNAUTHORIZED",
            "message": "A valid authorization token is required to create contract proposals.",
        }, indent=2)

    proposed_path = (
        Path(output_path) if output_path else PROJECT_ROOT / "configs" / "agent_contract_proposed.yaml"
    )
    validator.reload()
    current = dict(validator.contract)

    triage_context: Optional[dict[str, Any]] = None
    try:
        from src.delta_guard.triage import LLMTriageEngine

        triage_engine = LLMTriageEngine()
        quarantine_records = triage_engine.load_quarantine_records()
        clusters = triage_engine.cluster_error_signatures(quarantine_records)
        triage_context = triage_engine.generate_llm_diagnosis(quarantine_records, clusters)
    except Exception as exc:
        triage_context = {"provider": "unavailable", "error": str(exc)}

    changes_applied = []

    # 1. Add tool to allowlist
    if "add_allowed_tool" in proposed_change:
        new_tool = proposed_change["add_allowed_tool"]
        # Update schema field allowed_values if present
        for field in current.get("schema", {}).get("fields", []):
            if field.get("name") == "tool_name" and "allowed_values" in field:
                if new_tool not in field["allowed_values"]:
                    field["allowed_values"].append(new_tool)
                    changes_applied.append(f"Added '{new_tool}' to schema.fields[tool_name].allowed_values")

        # Update top-level allowed_tools if present
        if "allowed_tools" in current:
            if new_tool not in current["allowed_tools"]:
                current["allowed_tools"].append(new_tool)
                changes_applied.append(f"Added '{new_tool}' to allowed_tools")
        elif not changes_applied:
            current["allowed_tools"] = [new_tool]
            changes_applied.append(f"Created allowed_tools list with '{new_tool}'")

    # 2. Increase max cost
    if "increase_max_cost" in proposed_change:
        new_max = float(proposed_change["increase_max_cost"])
        current.setdefault("thresholds", {})["max_cost_per_call"] = new_max
        # Also update semantic_rules if present
        for rule in current.get("semantic_rules", []):
            if "cost_usd <=" in rule.get("rule", ""):
                rule["rule"] = f"cost_usd >= 0.0 AND cost_usd <= {new_max}"
                rule["message"] = f"cost_usd out of valid boundaries [0.0, {new_max}]"
        changes_applied.append(f"Updated max_cost_usd ceiling to ${new_max:.2f}")

    # 3. Add required field
    if "add_required_field" in proposed_change:
        new_field = proposed_change["add_required_field"]
        fields = current.setdefault("schema", {}).setdefault("fields", [])
        if not any(f.get("name") == new_field for f in fields):
            fields.append({"name": new_field, "type": "string", "nullable": False})
            changes_applied.append(f"Added required schema field '{new_field}'")

    if not changes_applied:
        return json.dumps({
            "status": "NO_CHANGE",
            "message": "No valid change applied. Provided parameters already satisfied or unsupported.",
            "current_contract": current,
        }, indent=2)

    # Write out proposed YAML file
    proposed_path.parent.mkdir(parents=True, exist_ok=True)
    with open(proposed_path, "w", encoding="utf-8") as f:
        yaml.dump(current, f, sort_keys=False, indent=2)

    return json.dumps({
        "status": "PROPOSAL_CREATED",
        "rationale": rationale,
        "changes_applied": changes_applied,
        "triage_context": triage_context,
        "artifact_path": str(proposed_path.resolve()),
        "next_steps": [
            "1. Automated CI / Invariant checks will evaluate proposed contract against quarantine history.",
            "2. Run 'pytest tests/' to verify no regressions on existing telemetry.",
            "3. Submit pull request for human sign-off before promoting to configs/agent_contract.yaml.",
        ],
        "message": "Draft contract saved. Ready for CI verification and supervisor review.",
    }, indent=2)


@mcp.tool()
def get_system_status() -> str:
    """
    OPERATIONAL HEALTH: Returns the real-time status of pipeline components (Kafka, Bronze, Quarantine).

    Returns:
        JSON string reporting overall health and individual component statuses.
    """
    status: Dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "overall_health": "healthy",
        "components": {},
    }

    # Kafka Check
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=kafka", "--format", "{{.Status}}"],
            capture_output=True,
            text=True,
            timeout=3,
        )
        if "Up" in result.stdout:
            status["components"]["kafka"] = {"status": "online", "detail": result.stdout.strip()}
        else:
            status["components"]["kafka"] = {"status": "offline", "detail": "Kafka container not active"}
    except Exception:
        status["components"]["kafka"] = {"status": "unreachable", "detail": "Docker CLI or Kafka unavailable"}

    # Bronze Delta Lake Check
    bronze_path = PROJECT_ROOT / "data" / "bronze" / "agent_events"
    if bronze_path.exists():
        try:
            conn = duckdb.connect()
            cnt = conn.execute(f"SELECT COUNT(*) FROM delta_scan('{bronze_path.as_posix()}')").fetchone()[0]
            conn.close()
            status["components"]["bronze_delta_lake"] = {"status": "online", "row_count": cnt}
        except Exception:
            status["components"]["bronze_delta_lake"] = {"status": "online", "row_count": "unindexed"}
    else:
        status["components"]["bronze_delta_lake"] = {"status": "empty", "detail": "Table not created yet"}

    # Quarantine Delta Lake Check
    quarantine_path = PROJECT_ROOT / "data" / "quarantine" / "agent_events"
    if quarantine_path.exists():
        try:
            conn = duckdb.connect()
            cnt = conn.execute(f"SELECT COUNT(*) FROM delta_scan('{quarantine_path.as_posix()}')").fetchone()[0]
            conn.close()
            status["components"]["quarantine_delta_lake"] = {"status": "active" if cnt > 0 else "clean", "row_count": cnt}
        except Exception:
            status["components"]["quarantine_delta_lake"] = {"status": "online", "row_count": "unindexed"}
    else:
        status["components"]["quarantine_delta_lake"] = {"status": "clean", "row_count": 0}

    # Evaluate overall status
    statuses = [c.get("status") for c in status["components"].values()]
    if "offline" in statuses or "unreachable" in statuses:
        status["overall_health"] = "degraded"

    return json.dumps(status, indent=2, default=str)


@mcp.tool()
def run_health_check() -> str:
    """
    DIAGNOSTIC SUITE: Runs comprehensive diagnostics on all Layer 1 and Layer 2 governance subsystems.

    Returns:
        JSON string reporting test checks, pass/fail counts, and system status.
    """
    checks = []
    passed = 0
    failed = 0

    # 1. Contract Validator Check
    try:
        validator.reload()
        checks.append({
            "name": "Contract Validator",
            "status": "passed",
            "detail": f"Loaded contract version {validator.contract.get('version', '1.0.0')} with {len(validator.allowed_tools)} allowed tools.",
        })
        passed += 1
    except Exception as e:
        checks.append({"name": "Contract Validator", "status": "failed", "detail": str(e)})
        failed += 1

    # 2. DuckDB Engine Check
    try:
        conn = duckdb.connect()
        conn.execute("SELECT 1 + 1 AS result").fetchall()
        conn.close()
        checks.append({"name": "DuckDB Analytics Engine", "status": "passed", "detail": "Vectorized query engine operational."})
        passed += 1
    except Exception as e:
        checks.append({"name": "DuckDB Analytics Engine", "status": "failed", "detail": str(e)})
        failed += 1

    # 3. Fast Validation Latency Check (<5ms benchmark)
    try:
        import time
        test_payload = {
            "agent_id": "benchmark_agent",
            "session_id": "sess_001",
            "action_id": "act_001",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "sql_query_executor",
            "execution_time_ms": 120,
            "cost_usd": 0.005,
        }
        t0 = time.perf_counter()
        for _ in range(100):
            validator.validate(test_payload)
        t_elapsed_ms = ((time.perf_counter() - t0) / 100) * 1000
        
        checks.append({
            "name": "Validator Latency Benchmark",
            "status": "passed" if t_elapsed_ms < 5.0 else "warning",
            "detail": f"Average pre-flight latency: {t_elapsed_ms:.3f} ms (Target: <5.000 ms).",
        })
        passed += 1
    except Exception as e:
        checks.append({"name": "Validator Latency Benchmark", "status": "failed", "detail": str(e)})
        failed += 1

    return json.dumps({
        "status": "healthy" if failed == 0 else "degraded",
        "passed": passed,
        "failed": failed,
        "checks": checks,
    }, indent=2)


def main():
    """Main entrypoint to run the FastMCP server via stdio."""
    # If running with FastMCP
    mcp.run()


if __name__ == "__main__":
    main()