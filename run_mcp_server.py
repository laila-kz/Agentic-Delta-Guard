#!/usr/bin/env python
"""
run_mcp_server.py
=================
Stdio entrypoint for the Agentic Delta Guard Model Context Protocol (MCP) Server.

Compatible with Claude Desktop, Cursor, Gemini CLI, and any FastMCP-capable runner.

Usage:
    # Direct (stdio transport):
    python run_mcp_server.py

    # Via mcp_config.json (Claude Desktop / Cursor):
    {
      "mcpServers": {
        "agentic-delta-guard": {
          "command": "python",
          "args": ["run_mcp_server.py"],
          "env": {
            "PYTHONPATH": ".",
            "MCP_PROPOSAL_TOKEN": "your-secret-token"
          }
        }
      }
    }

Available MCP tools:
    check_contract              Pre-flight payload validator with remediation hints
    get_active_contract         Returns full ODCS contract: allowed tools, cost cap, freshness rules
    get_quarantine_summary      Queries recent quarantine records clustered by error signature
    inspect_bronze_lakehouse    Queries recent validated Bronze Delta Lake records
    propose_contract_patch      Writes a contract evolution proposal (requires MCP_PROPOSAL_TOKEN)
    get_system_status           Health check across all system components
    run_health_check            Validates Delta Lake tables, contract file, and data freshness

Environment Variables:
    MCP_PROPOSAL_TOKEN    Required to call propose_contract_patch (default: unset = disabled)
    KAFKA_BOOTSTRAP_SERVERS   Kafka broker (default: localhost:9092)
"""

import os
import sys
from pathlib import Path

# ── Python path setup ──────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ── Validate environment ───────────────────────────────────────────────────────
_token = os.environ.get("MCP_PROPOSAL_TOKEN")
if not _token:
    print(
        "[INFO] MCP_PROPOSAL_TOKEN is not set. "
        "propose_contract_patch will require explicit token in the call. "
        "Set MCP_PROPOSAL_TOKEN env var to pre-authorize.",
        file=sys.stderr,
    )

# ── Import and launch ──────────────────────────────────────────────────────────
from src.delta_guard.mcp.server import mcp  # noqa: E402

if __name__ == "__main__":
    try:
        mcp.run()
    except KeyboardInterrupt:
        print("\n[OK] MCP server stopped cleanly.", file=sys.stderr)
    except Exception as exc:
        print(f"\n[ERROR] MCP server crashed: {exc}", file=sys.stderr)
        sys.exit(1)
