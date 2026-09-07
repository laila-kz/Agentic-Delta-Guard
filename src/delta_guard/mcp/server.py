"""
src/delta_guard/mcp/server.py
Model Context Protocol server for Agentic Delta Guard.
Exposes synchronous pre-flight checks and supervisor observability tools.
"""

from __future__ import annotations
import json
import os
from pathlib import Path
from typing import Any, Optional
from datetime import datetime, timezone

import duckdb
import yaml

# MCP imports
try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    print("⚠️  MCP not installed. Run: pip install mcp")
    print("Falling back to mock mode...")
    
    # Mock FastMCP for testing without MCP
    class FastMCP:
        def __init__(self, name, dependencies=None):
            self.name = name
            self.tools = {}
            
        def tool(self, fn=None, **kwargs):
            def decorator(func):
                self.tools[func.__name__] = func
                return func
            return decorator if fn is None else decorator(fn)
        
        def run(self):
            print(f"Running MCP server: {self.name}")
            print(f"Available tools: {list(self.tools.keys())}")
            print("\nTo test, call tools directly:")
            for name in self.tools:
                print(f"  - {name}()")
            return self

# Import our validator. Support both package imports and direct script execution.
try:
    from .validators.contract_validator import ContractValidator
except ImportError:
    from validators.contract_validator import ContractValidator

# Initialize server
mcp = FastMCP("agentic-delta-guard", dependencies=["duckdb", "pyyaml", "pydantic"])
validator = ContractValidator()

# Project root
PROJECT_ROOT = Path(__file__).resolve().parents[3]


@mcp.tool()
def check_contract(payload: dict[str, Any]) -> str:
    """
    PRE-FLIGHT ADVISOR: Checks whether an event payload satisfies data lakehouse 
    quality rules before the agent writes to Kafka.
    
    Args:
        payload: The event payload to validate (dict with agent_id, tool_name, cost_usd, etc.)
    
    Returns:
        JSON string with validation result including allowed flag, violations, and hints.
    """
    result = validator.validate(payload)
    
    # If allowed, add a helpful message
    if result["allowed"]:
        result["message"] = "✅ Payload passes all contract checks. Safe to emit."
    else:
        result["message"] = f"❌ Payload rejected ({len(result['violations'])} violation(s)). Review hints."
    
    return json.dumps(result, indent=2)


@mcp.tool()
def get_active_contract() -> str:
    """
    SCHEMA DISCOVERY: Returns the full active data contract.
    
    Returns:
        JSON string with contract schema, allowed tools, and thresholds.
    """
    validator.reload()  # Refresh on demand
    return json.dumps({
        "contract": validator.contract,
        "loaded_at": datetime.now(timezone.utc).isoformat(),
        "contract_path": str(validator.contract_path.absolute())
    }, indent=2)


@mcp.tool()
def get_quarantine_summary(limit: int = 5, agent_id: Optional[str] = None) -> str:
    """
    SUPERVISOR OBSERVABILITY: Queries recent quarantine rejections.
    
    Args:
        limit: Maximum number of records to return (default: 5)
        agent_id: Optional filter by specific agent
    
    Returns:
        JSON string with quarantine records summary.
    """
    quarantine_path = PROJECT_ROOT / "data" / "quarantine" / "agent_events"
    
    if not quarantine_path.exists():
        return json.dumps({
            "status": "clean",
            "message": "No quarantine records found. All agents are compliant! 🎉",
            "records": []
        }, indent=2)

    conn = duckdb.connect()
    try:
        # Try Delta scan first
        try:
            query = f"SELECT * FROM delta_scan('{quarantine_path}')"
        except:
            # Fallback to Parquet
            query = f"SELECT * FROM read_parquet('{quarantine_path}/**/*.parquet')"
        
        if agent_id:
            query += f" WHERE agent_id = '{agent_id}'"
        query += f" ORDER BY timestamp DESC LIMIT {limit}"
        
        df = conn.execute(query).df()
        records = df.to_dict(orient="records")
        
        # Extract error summaries
        error_types = {}
        for record in records:
            err = record.get("error_summary", "unknown")
            error_types[err] = error_types.get(err, 0) + 1
        
        return json.dumps({
            "status": "quarantine_active",
            "total_records_in_sample": len(records),
            "error_signatures": error_types,
            "records": records,
            "message": f"Found {len(records)} recent quarantine records"
        }, indent=2, default=str)
        
    except Exception as e:
        return json.dumps({
            "status": "error",
            "error": f"Quarantine query failed: {str(e)}",
            "suggestion": "Try running the pipeline to generate some quarantine data"
        }, indent=2)
    finally:
        conn.close()


@mcp.tool()
def propose_contract_patch(rationale: str, proposed_change: dict[str, Any]) -> str:
    """
    AUTONOMOUS GOVERNANCE: Proposes a formal update to the contract.
    
    Args:
        rationale: Why this change is needed (e.g., "New tool added to agent fleet")
        proposed_change: What to change (e.g., {"add_allowed_tool": "new_tool_name"})
    
    Returns:
        JSON string with proposal status and next steps.
    """
    proposed_path = PROJECT_ROOT / "configs" / "agent_contract_proposed.yaml"
    
    # Load current contract
    current = validator.contract
    
    # Apply changes
    if "add_allowed_tool" in proposed_change:
        tools = current.setdefault("allowed_tools", [])
        new_tool = proposed_change["add_allowed_tool"]
        if new_tool not in tools:
            tools.append(new_tool)
            print(f"✅ Added '{new_tool}' to allowed_tools")
        else:
            return json.dumps({
                "status": "NO_CHANGE",
                "message": f"Tool '{new_tool}' already in allowed_tools",
                "current_tools": tools
            }, indent=2)
    
    if "increase_max_cost" in proposed_change:
        new_max = float(proposed_change["increase_max_cost"])
        current.setdefault("thresholds", {})["max_cost_per_call"] = new_max
        print(f"✅ Increased max_cost_per_call to ${new_max}")
    
    if "add_required_field" in proposed_change:
        fields = current.setdefault("schema", {}).setdefault("fields", [])
        new_field = proposed_change["add_required_field"]
        fields.append({"name": new_field, "type": "string", "nullable": False})
        print(f"✅ Added required field: {new_field}")
    
    # Write proposal
    with open(proposed_path, "w", encoding="utf-8") as f:
        yaml.dump(current, f, sort_keys=False)
    
    return json.dumps({
        "status": "PROPOSAL_CREATED",
        "rationale": rationale,
        "changes_applied": list(proposed_change.keys()),
        "artifact_path": str(proposed_path.absolute()),
        "next_steps": [
            "1. Review the proposed changes in the artifact",
            "2. Run `make dbt-test` to verify the new contract",
            "3. If approved, manually merge to agent_contract.yaml",
            "4. Restart the gatekeeper to apply the new rules"
        ],
        "message": "✅ Contract proposal saved. CI pipeline will run tests before human merge."
    }, indent=2)


@mcp.tool()
def get_system_status() -> str:
    """
    HEALTH CHECK: Returns the overall health status of the pipeline.
    
    Returns:
        JSON string with Kafka, Gatekeeper, and Table status.
    """
    status = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "components": {}
    }
    
    # Check Kafka
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=kafka", "--format", "{{.Status}}"],
            capture_output=True, text=True
        )
        status["components"]["kafka"] = {
            "status": "online" if "Up" in result.stdout else "offline",
            "detail": result.stdout.strip() or "Not running"
        }
    except:
        status["components"]["kafka"] = {
            "status": "unknown",
            "detail": "Could not check Kafka status"
        }
    
    # Check Bronze table
    bronze_path = PROJECT_ROOT / "data" / "bronze" / "agent_events"
    if bronze_path.exists():
        try:
            conn = duckdb.connect()
            count = conn.execute(
                f"SELECT COUNT(*) FROM delta_scan('{bronze_path}')"
            ).fetchone()[0]
            conn.close()
            status["components"]["bronze"] = {
                "status": "online",
                "row_count": count,
                "detail": f"{count} validated records"
            }
        except:
            status["components"]["bronze"] = {
                "status": "online",
                "row_count": "unknown",
                "detail": "Table exists but query failed"
            }
    else:
        status["components"]["bronze"] = {
            "status": "empty",
            "detail": "No data yet. Run the producer!"
        }
    
    # Check Quarantine
    quarantine_path = PROJECT_ROOT / "data" / "quarantine" / "agent_events"
    if quarantine_path.exists():
        try:
            conn = duckdb.connect()
            count = conn.execute(
                f"SELECT COUNT(*) FROM delta_scan('{quarantine_path}')"
            ).fetchone()[0]
            conn.close()
            status["components"]["quarantine"] = {
                "status": "active" if count > 0 else "empty",
                "row_count": count,
                "detail": f"{count} quarantined records" if count > 0 else "No quarantine records"
            }
        except:
            status["components"]["quarantine"] = {
                "status": "online",
                "row_count": "unknown"
            }
    else:
        status["components"]["quarantine"] = {
            "status": "empty",
            "detail": "Quarantine table not yet created"
        }
    
    # Overall health
    all_online = all(
        c.get("status") in ["online", "active", "empty"] 
        for c in status["components"].values()
    )
    status["overall_health"] = "healthy" if all_online else "degraded"
    
    return json.dumps(status, indent=2, default=str)


@mcp.tool()
def run_health_check() -> str:
    """
    DIAGNOSTIC: Runs a comprehensive health check of the entire pipeline.
    
    Returns:
        JSON string with detailed diagnostic results.
    """
    results = {
        "checks": [],
        "passed": 0,
        "failed": 0
    }
    
    # Check 1: Validator loads
    try:
        validator.reload()
        results["checks"].append({
            "name": "Contract Validator",
            "status": "passed",
            "detail": f"Loaded version {validator.contract.get('version', 'unknown')}"
        })
        results["passed"] += 1
    except Exception as e:
        results["checks"].append({
            "name": "Contract Validator",
            "status": "failed",
            "detail": str(e)
        })
        results["failed"] += 1
    
    # Check 2: DuckDB connection
    try:
        conn = duckdb.connect()
        conn.execute("SELECT 1")
        conn.close()
        results["checks"].append({
            "name": "DuckDB Connection",
            "status": "passed",
            "detail": "Connection successful"
        })
        results["passed"] += 1
    except Exception as e:
        results["checks"].append({
            "name": "DuckDB Connection",
            "status": "failed",
            "detail": str(e)
        })
        results["failed"] += 1
    
    # Check 3: Kafka status
    try:
        import subprocess
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=kafka", "--format", "{{.Status}}"],
            capture_output=True, text=True, timeout=5
        )
        if "Up" in result.stdout:
            results["checks"].append({
                "name": "Kafka",
                "status": "passed",
                "detail": "Container is running"
            })
            results["passed"] += 1
        else:
            results["checks"].append({
                "name": "Kafka",
                "status": "warning",
                "detail": "Kafka not running or not found"
            })
            results["failed"] += 1
    except:
        results["checks"].append({
            "name": "Kafka",
            "status": "warning",
            "detail": "Could not check Kafka (maybe not running)"
        })
    
    return json.dumps({
        "status": "healthy" if results["failed"] == 0 else "degraded",
        "summary": results
    }, indent=2)


def run_server():
    """Run the MCP server"""
    print("=" * 60)
    print("🛡️  AGENTIC DELTA GUARD MCP SERVER")
    print("=" * 60)
    print(f"Contract: {validator.contract_path.absolute()}")
    print(f"Tools available:")
    print("  - check_contract(payload)")
    print("  - get_active_contract()")
    print("  - get_quarantine_summary(limit=5, agent_id=None)")
    print("  - propose_contract_patch(rationale, proposed_change)")
    print("  - get_system_status()")
    print("  - run_health_check()")
    print("=" * 60)
    print("\nStarting server... (Press Ctrl+C to stop)\n")
    
    mcp.run()


if __name__ == "__main__":
    run_server()