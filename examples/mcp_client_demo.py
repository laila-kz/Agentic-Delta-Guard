"""
examples/mcp_client_demo.py -- Standalone Client Demo for Agentic Delta Guard MCP Server.

Spawns run_mcp_server.py via stdio, connects using the reference Python MCP SDK,
and executes an end-to-end governance lifecycle over the wire:
  1. Tool discovery (tools/list)
  2. Valid pre-flight check (check_contract)
  3. Policy breach pre-flight rejection (check_contract)
  4. Contract proposal patch (propose_contract_patch with token)

Run:
    python examples/mcp_client_demo.py
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# Safe UTF-8 encoding on Windows standard streams
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

PROJECT_ROOT = Path(__file__).resolve().parents[1]


async def run_demo():
    print("=" * 68)
    print("  [MCP] AGENTIC DELTA GUARD -- MCP Server Stdio Wire Client Demo")
    print("=" * 68)

    proposal_token = "demo-proposal-auth-token"
    temp_dir = Path(tempfile.gettempdir())
    output_path = temp_dir / "proposed_contract_demo.yaml"

    server_params = StdioServerParameters(
        command=sys.executable,
        args=[str(PROJECT_ROOT / "run_mcp_server.py")],
        env={
            "PYTHONPATH": str(PROJECT_ROOT),
            "MCP_PROPOSAL_TOKEN": proposal_token,
            "OPENAI_API_KEY": "",
            "ANTHROPIC_API_KEY": "",
        },
    )

    print("\n[1] Spawning MCP server subprocess over stdio...")
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            print("[2] Initializing ClientSession...")
            await session.initialize()

            # 1. Tools list
            print("\n[3] Calling tools/list ...")
            tools_response = await session.list_tools()
            print(f"  Received {len(tools_response.tools)} available tools from MCP server:")
            for t in tools_response.tools:
                first_line = (t.description or "").strip().split("\n")[0]
                print(f"    - {t.name:25s} - {first_line}")

            # 2. Check contract (Valid)
            print("\n[4] Executing check_contract (Valid Payload) ...")
            valid_payload = {
                "agent_id": "demo_agent_001",
                "session_id": "sess_demo_123",
                "action_id": "act_demo_456",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "tool_name": "web_scraper",
                "execution_time_ms": 145,
                "cost_usd": 0.02,
                "tool_args": {"url": "https://example.com"},
            }
            res_valid = await session.call_tool("check_contract", arguments={"payload": valid_payload})
            valid_parsed = json.loads(res_valid.content[0].text)
            print(f"  Allowed : {valid_parsed.get('allowed')}")
            print(f"  Message : {valid_parsed.get('message')}")

            # 3. Check contract (Poisoned)
            print("\n[5] Executing check_contract (Poisoned Payload - Cost & Tool Breach) ...")
            poisoned_payload = {
                "agent_id": "rogue_agent_999",
                "session_id": "sess_poison_789",
                "action_id": "act_poison_012",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "tool_name": "unauthorized_shell_exec",
                "execution_time_ms": 9999,
                "cost_usd": 150.0,
            }
            res_poison = await session.call_tool("check_contract", arguments={"payload": poisoned_payload})
            poison_parsed = json.loads(res_poison.content[0].text)
            print(f"  Allowed    : {poison_parsed.get('allowed')}")
            print(f"  Violations : {poison_parsed.get('violations')}")

            # 4. Propose contract patch (Valid Token)
            print("\n[6] Executing propose_contract_patch (Authorized with Token) ...")
            proposal_args = {
                "rationale": "Supervisor introducing web_scraper_v2 and raising cost ceiling",
                "proposed_change": {
                    "add_allowed_tool": "web_scraper_v2",
                    "increase_max_cost": 75.0,
                },
                "authorization_token": proposal_token,
                "output_path": str(output_path),
            }
            res_patch = await session.call_tool("propose_contract_patch", arguments=proposal_args)
            patch_parsed = json.loads(res_patch.content[0].text)
            print(f"  Status          : {patch_parsed.get('status')}")
            print(f"  Changes Applied : {patch_parsed.get('changes_applied')}")
            print(f"  Artifact Path   : {patch_parsed.get('artifact_path')}")

            # 5. Propose contract patch (Unauthorized - Without Token)
            print("\n[7] Executing propose_contract_patch (Unauthorized - Invalid Token) ...")
            unauth_args = {
                "rationale": "Unauthorized change attempt",
                "proposed_change": {"add_allowed_tool": "malicious_tool"},
                "authorization_token": "bad-token",
            }
            res_unauth = await session.call_tool("propose_contract_patch", arguments=unauth_args)
            unauth_parsed = json.loads(res_unauth.content[0].text)
            print(f"  Status  : {unauth_parsed.get('status')}")
            print(f"  Message : {unauth_parsed.get('message')}")

    print("\n" + "=" * 68)
    print("  [OK] MCP CLIENT DEMO COMPLETE -- All wire calls succeeded over stdio")
    print("=" * 68 + "\n")


def main():
    asyncio.run(run_demo())


if __name__ == "__main__":
    main()
