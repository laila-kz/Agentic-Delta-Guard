"""
tests/test_mcp_stdio.py -- Wire-level stdio integration tests for MCP Server.

All tests spawn run_mcp_server.py as a subprocess via mcp.client.stdio.stdio_client
and communicate over stdio via ClientSession.

CRITICAL RULE:
  Do NOT import from src.delta_guard.mcp.server anywhere in this file.
"""
from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

import pytest
from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _server_params(env_overrides: Optional[Dict[str, str]] = None) -> StdioServerParameters:
    env = {
        "PYTHONPATH": str(PROJECT_ROOT),
        "OPENAI_API_KEY": "",
        "ANTHROPIC_API_KEY": "",
    }
    if env_overrides:
        env.update(env_overrides)
    return StdioServerParameters(
        command=sys.executable,
        args=[str(PROJECT_ROOT / "run_mcp_server.py")],
        env=env,
    )


def test_stdio_list_tools_includes_propose_contract_patch():
    """Lists tools over stdio and asserts propose_contract_patch schema integrity."""
    async def run():
        await asyncio.sleep(0.5)
        async with stdio_client(_server_params()) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                response = await session.list_tools()
                tools_by_name = {t.name: t for t in response.tools}
                assert "propose_contract_patch" in tools_by_name
                tool = tools_by_name["propose_contract_patch"]
                assert tool.description and len(tool.description.strip()) > 0
                schema = tool.inputSchema
                required = schema.get("required", [])
                assert "rationale" in required
                assert "proposed_change" in required

    asyncio.run(asyncio.wait_for(run(), timeout=120.0))


def test_stdio_call_tool_propose_contract_patch_with_valid_token(tmp_path):
    """Submits contract patch proposal over stdio with a valid token."""
    output_file = tmp_path / "proposed.yaml"

    async def run():
        await asyncio.sleep(0.5)
        params = _server_params({"MCP_PROPOSAL_TOKEN": "valid-secret-token"})
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                res = await session.call_tool(
                    "propose_contract_patch",
                    arguments={
                        "rationale": "Wire test patch",
                        "proposed_change": {"add_allowed_tool": "data_extractor_v3"},
                        "authorization_token": "valid-secret-token",
                        "output_path": str(output_file),
                    },
                )
                assert not res.isError
                assert res.content and len(res.content) > 0
                text = res.content[0].text
                parsed = json.loads(text)
                assert parsed.get("status") == "PROPOSAL_CREATED"
                assert "changes_applied" in parsed
                assert len(parsed["changes_applied"]) > 0
                assert output_file.exists()

    asyncio.run(asyncio.wait_for(run(), timeout=120.0))


def test_stdio_call_tool_propose_contract_patch_without_token(tmp_path):
    """Submits contract patch proposal over stdio without token, expecting rejection."""
    output_file = tmp_path / "should_not_exist.yaml"

    async def run():
        await asyncio.sleep(0.5)
        params = _server_params({})
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                res = await session.call_tool(
                    "propose_contract_patch",
                    arguments={
                        "rationale": "Unauthorized patch",
                        "proposed_change": {"add_allowed_tool": "rogue_tool"},
                        "output_path": str(output_file),
                    },
                )
                text = res.content[0].text if res.content else ""
                assert "UNAUTHORIZED" in text or "AUTHORIZATION_REQUIRED" in text
                assert not output_file.exists()

    asyncio.run(asyncio.wait_for(run(), timeout=120.0))


def test_stdio_call_tool_check_contract_pass_and_fail():
    """Performs pre-flight checks over stdio with valid and poisoned payloads."""
    async def run():
        await asyncio.sleep(0.5)
        async with stdio_client(_server_params()) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                # 1. Valid payload
                valid_event = {
                    "agent_id": "test_agent_001",
                    "session_id": "sess_unit_test",
                    "action_id": "act_unit_test",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "tool_name": "sql_query_executor",
                    "execution_time_ms": 150,
                    "cost_usd": 0.05,
                    "tool_args": {"query": "SELECT 1;"},
                }
                res_pass = await session.call_tool("check_contract", arguments={"payload": valid_event})
                parsed_pass = json.loads(res_pass.content[0].text)
                assert parsed_pass.get("allowed") is True

                # 2. Poisoned payload
                poisoned_event = {
                    "agent_id": "rogue_agent",
                    "session_id": "sess_bad",
                    "action_id": "act_bad",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "tool_name": "unauthorized_shell_exec",
                    "execution_time_ms": 100,
                    "cost_usd": 100.0,
                }
                res_fail = await session.call_tool("check_contract", arguments={"payload": poisoned_event})
                parsed_fail = json.loads(res_fail.content[0].text)
                assert parsed_fail.get("allowed") is False

    asyncio.run(asyncio.wait_for(run(), timeout=120.0))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
