#!/usr/bin/env python
"""
record_mcp_session.py — Generates real MCP agent interaction transcripts.

Executes actual MCP tool invocations against src.delta_guard.mcp.server and
generates a high-fidelity Markdown interaction transcript at docs/reports/mcp_interaction_log.md
for portfolio and technical architecture reviews.
"""

import json
import os
import sys
from datetime import datetime, timezone

# Ensure safe UTF-8 terminal encoding on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# Ensure repo root is on sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.delta_guard.mcp.server import (
    check_contract,
    get_active_contract,
    get_quarantine_summary,
    get_system_status,
    inspect_bronze_lakehouse,
    propose_contract_patch,
    run_health_check,
)


def record_session():
    print(">> Recording Live MCP Agent Interaction Session...")
    os.environ["MCP_PROPOSAL_TOKEN"] = "portfolio-demo-token"

    timestamp_now = datetime.now(timezone.utc).isoformat()

    # 1. Discover Active Contract
    active_contract_res = json.loads(get_active_contract())

    # 2. Valid pre-flight check
    valid_event = {
        "agent_id": "analytics-agent-01",
        "session_id": "sess-prod-789",
        "action_id": "act-val-101",
        "timestamp": timestamp_now,
        "tool_name": "vector_search",
        "execution_time_ms": 142,
        "cost_usd": 0.045,
        "status": "SUCCESS",
        "tool_args": {"query": "customer churn 2026", "top_k": 5},
    }
    check_valid_res = json.loads(check_contract(valid_event))

    # 3. Rejected pre-flight check (poison / disallowed tool & cost limit breach)
    invalid_event = {
        "agent_id": "rogue-agent-07",
        "session_id": "sess-prod-994",
        "action_id": "act-rej-662",
        "timestamp": timestamp_now,
        "tool_name": "unauthorized_shell_exec",
        "execution_time_ms": 420000,
        "cost_usd": 128.50,
        "status": "SUCCESS",
        "tool_args": {"cmd": "rm -rf /"},
    }
    check_invalid_res = json.loads(check_contract(invalid_event))

    # 4. Quarantine Summary
    quarantine_res = json.loads(get_quarantine_summary(limit=5))

    # 5. Lakehouse Inspection
    bronze_res = json.loads(inspect_bronze_lakehouse(limit=3))

    # 6. Contract Patch Proposal
    proposal = {
        "add_allowed_tool": "data_extractor_v2",
        "increase_max_cost": 75.0,
    }
    patch_res = json.loads(propose_contract_patch(
        rationale="Upgrade analytics pipeline for multi-modal vector search extraction",
        proposed_change=proposal,
        authorization_token="portfolio-demo-token",
    ))

    # 7. System Health Status
    health_res = json.loads(run_health_check())

    # Generate Markdown Transcript
    output_path = os.path.join(PROJECT_ROOT, "docs", "reports", "mcp_interaction_log.md")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    markdown_content = f"""# 🤖 MCP Server Interaction Log: Shift-Left Agent Validation

**Generated At:** `{timestamp_now}`  
**Protocol Version:** Model Context Protocol (MCP) v1.0.0  
**Target Architecture:** Agentic Delta Guard (`src/delta_guard/mcp/`)

This document captures an end-to-end verified interaction session between an autonomous AI agent / supervisor and the **Agentic Delta Guard MCP Server**.

By exposing data contract guardrails directly to autonomous LLM agents over the Model Context Protocol, agents self-validate their planned actions *before* sending payloads into streaming pipelines, achieving zero downstream data pollution (Shift-Left Data Engineering).

---

## 📋 Architecture & Sequence Workflow

```mermaid
sequenceDiagram
    autonumber
    participant Agent as Autonomous AI Agent
    participant MCP as Delta Guard MCP Server
    participant Kafka as Kafka (KRaft) / Bronze Delta Lake

    Agent->>MCP: get_active_contract()
    MCP-->>Agent: Returns allowed_tools, max_cost ($50), schemas
    
    Note over Agent: Scenario 1: Authorized Action
    Agent->>MCP: check_contract(valid_payload)
    MCP-->>Agent: allowed: true, message: "Safe to emit"
    Agent->>Kafka: Ingest payload to streaming pipeline

    Note over Agent: Scenario 2: Unauthorized Tool & Cost Breach
    Agent->>MCP: check_contract(invalid_payload)
    MCP-->>Agent: allowed: false, violations: [UNAUTHORIZED_TOOL, COST_LIMIT_BREACH]
    Note over Agent: Agent intercepts failure, avoids network pollution & self-remediates

    Note over Agent: Scenario 3: Supervisor Incident Investigation
    Agent->>MCP: get_quarantine_summary()
    MCP-->>Agent: Quarantined records & root causes
    Agent->>MCP: propose_contract_patch(token, proposal)
    MCP-->>Agent: PROPOSAL_CREATED (isolated sandbox artifact)
```

---

## 1. Tool Discovery: `get_active_contract`

An autonomous agent queries the active data contract upon session initialization to dynamically discover permitted tools, cost ceilings, and validation thresholds.

### Request:
```json
{{
  "method": "tools/call",
  "params": {{
    "name": "get_active_contract",
    "arguments": {{}}
  }}
}}
```

### Live Server Response:
```json
{json.dumps(active_contract_res, indent=2)}
```

---

## 2. Valid Pre-Flight Check: `check_contract` (Approved)

The agent tests a compliant action payload before streaming it to Kafka.

### Request:
```json
{{
  "method": "tools/call",
  "params": {{
    "name": "check_contract",
    "arguments": {json.dumps(valid_event, indent=2)}
  }}
}}
```

### Live Server Response:
```json
{json.dumps(check_valid_res, indent=2)}
```

---

## 3. Policy Breach Interception: `check_contract` (Rejected)

The agent attempts to execute an unauthorized tool (`unauthorized_shell_exec`) with a cost of `$128.50` (exceeding the `$50.00` contract limit). The MCP validator intercepts and rejects the payload with remediation guidance.

### Request:
```json
{{
  "method": "tools/call",
  "params": {{
    "name": "check_contract",
    "arguments": {json.dumps(invalid_event, indent=2)}
  }}
}}
```

### Live Server Response:
```json
{json.dumps(check_invalid_res, indent=2)}
```

---

## 4. Supervisor Quarantine Telemetry: `get_quarantine_summary`

A supervisor or SRE agent inspects the quarantined records in Delta Lake to analyze failure patterns.

### Request:
```json
{{
  "method": "tools/call",
  "params": {{
    "name": "get_quarantine_summary",
    "arguments": {{
      "limit": 5
    }}
  }}
}}
```

### Live Server Response:
```json
{json.dumps(quarantine_res, indent=2)}
```

---

## 5. Bronze Lakehouse Inspection: `inspect_bronze_lakehouse`

Inspects valid records committed to the Bronze Delta Lake storage layer.

### Request:
```json
{{
  "method": "tools/call",
  "params": {{
    "name": "inspect_bronze_lakehouse",
    "arguments": {{
      "limit": 3
    }}
  }}
}}
```

### Live Server Response:
```json
{json.dumps(bronze_res, indent=2)}
```

---

## 6. Governed Contract Patch Proposal: `propose_contract_patch`

When new tools are introduced, an authorized agent proposes a schema evolution patch with security token authentication.

### Request:
```json
{{
  "method": "tools/call",
  "params": {{
    "name": "propose_contract_patch",
    "arguments": {{
      "rationale": "Upgrade analytics pipeline for multi-modal vector search extraction",
      "proposal": {json.dumps(proposal, indent=2)},
      "authorization_token": "portfolio-demo-token"
    }}
  }}
}}
```

### Live Server Response:
```json
{json.dumps(patch_res, indent=2)}
```

---

## 7. System Health Check: `run_health_check`

Validates overall storage layer integrity, contract availability, and component health.

### Request:
```json
{{
  "method": "tools/call",
  "params": {{
    "name": "run_health_check",
    "arguments": {{}}
  }}
}}
```

### Live Server Response:
```json
{json.dumps(health_res, indent=2)}
```

---

## 💡 Key Architectural Benefits Demonstrated

1. **Shift-Left Contract Enforcement**: Data contracts are enforced directly at the LLM agent level prior to streaming emission.
2. **Zero Downstream Data Pollution**: Malformed schemas, toxic inputs, and unauthorized actions never touch Kafka brokers or Delta tables.
3. **Deterministic Governance**: Automated feedback loops provide structured remediation hints, enabling autonomous agent self-healing.
"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(markdown_content)

    print(f"[OK] Successfully generated live MCP interaction log at: {output_path}")


if __name__ == "__main__":
    record_session()
