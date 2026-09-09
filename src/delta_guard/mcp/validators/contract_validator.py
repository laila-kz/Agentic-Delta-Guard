"""
src/delta_guard/mcp/validators/contract_validator.py
Synchronous, sub-5ms in-memory contract enforcement engine for MCP tool calls.
Provides pre-flight defense-in-depth validation against configs/agent_contract.yaml.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
import yaml


class ContractValidator:
    """
    Zero-overhead synchronous validator compiled from agent_contract.yaml.
    Optimized for sub-5ms execution.
    """

    def __init__(self, contract_path: str | Path = "configs/agent_contract.yaml"):
        raw_path = Path(contract_path)
        if not raw_path.is_absolute():
            # Resolve relative to project root
            project_root = Path(__file__).resolve().parents[4]
            self.contract_path = project_root / raw_path
        else:
            self.contract_path = raw_path

        self.contract: Dict[str, Any] = {}
        self.allowed_tools: Set[str] = set()
        self.required_fields: List[Dict[str, Any]] = []
        self.max_cost_usd: float = 50.0
        self.max_event_age_hours: int = 24
        self.max_future_skew_mins: int = 5
        self.max_payload_bytes: int = 32768

        self.reload()

    def _get_default_contract(self) -> dict[str, Any]:
        """Fallback contract if file doesn't exist."""
        return {
            "version": "1.0.0",
            "contract_id": "agent_events_v1",
            "schema": {
                "fields": [
                    {"name": "agent_id", "type": "string", "nullable": False},
                    {"name": "session_id", "type": "string", "nullable": False},
                    {"name": "action_id", "type": "string", "nullable": False},
                    {"name": "timestamp", "type": "timestamp", "nullable": False},
                    {
                        "name": "tool_name",
                        "type": "string",
                        "nullable": False,
                        "allowed_values": ["sql_query_executor", "vector_search", "web_scraper", "db_writer"],
                    },
                    {"name": "execution_time_ms", "type": "integer", "nullable": False},
                    {"name": "cost_usd", "type": "double", "nullable": False},
                ]
            },
            "allowed_tools": ["sql_query_executor", "vector_search", "web_scraper", "db_writer", "search_tool", "python_repl", "rag_retriever"],
            "semantic_rules": [
                {"id": "cost_non_negative", "rule": "cost_usd >= 0.0 AND cost_usd <= 50.0"},
                {"id": "timestamp_freshness", "rule": "INTERVAL 24 HOURS, INTERVAL 5 MINUTES"},
            ],
            "thresholds": {
                "max_cost_per_call": 50.0,
                "max_payload_bytes": 32768,
                "freshness_window_hours": 24,
                "future_tolerance_minutes": 5,
            },
        }

    def _load_contract(self) -> dict[str, Any]:
        """Load and parse the contract YAML."""
        if not self.contract_path.exists():
            return self._get_default_contract()

        try:
            with open(self.contract_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or self._get_default_contract()
        except Exception:
            return self._get_default_contract()

    def reload(self) -> None:
        """Hot-reload contract on file changes and recompile rules."""
        self.contract = self._load_contract()
        self._compile_rules()

    def _compile_rules(self) -> None:
        """Pre-compile validation rules for high-speed in-memory evaluation."""
        self.allowed_tools = set()
        self.required_fields = []
        
        # 1. Allowed tools extraction
        # Check top-level allowed_tools
        if "allowed_tools" in self.contract:
            self.allowed_tools.update(self.contract["allowed_tools"])

        # Check schema fields for tool_name allowed_values
        fields = self.contract.get("schema", {}).get("fields", [])
        for field in fields:
            fname = field.get("name")
            if not field.get("nullable", True):
                self.required_fields.append(field)
            if fname == "tool_name" and "allowed_values" in field:
                self.allowed_tools.update(field["allowed_values"])

        # 2. Thresholds & semantic rules compilation
        thresholds = self.contract.get("thresholds", {})
        self.max_cost_usd = float(thresholds.get("max_cost_per_call", 50.0))
        self.max_payload_bytes = int(thresholds.get("max_payload_bytes", 32768))
        self.max_event_age_hours = int(thresholds.get("freshness_window_hours", 24))
        self.max_future_skew_mins = int(thresholds.get("future_tolerance_minutes", 5))

        for rule in self.contract.get("semantic_rules", []):
            rule_text = str(rule.get("rule", ""))
            m_cost = re.search(r"cost_usd\s*<=\s*([\d.]+)", rule_text)
            if m_cost:
                self.max_cost_usd = float(m_cost.group(1))
            m_age = re.search(r"INTERVAL\s+(\d+)\s+HOURS", rule_text)
            if m_age:
                self.max_event_age_hours = int(m_age.group(1))
            m_skew = re.search(r"INTERVAL\s+(\d+)\s+MINUTES", rule_text)
            if m_skew:
                self.max_future_skew_mins = int(m_skew.group(1))

    def get_allowed_tools(self) -> List[str]:
        """Get list of allowed tool names."""
        return sorted(list(self.allowed_tools))

    def get_threshold(self, key: str, default: Any = None) -> Any:
        """Get a threshold value from the contract."""
        if key == "max_cost_per_call" or key == "max_cost_usd":
            return self.max_cost_usd
        if key == "freshness_window_hours" or key == "max_event_age_hours":
            return self.max_event_age_hours
        if key == "future_tolerance_minutes" or key == "max_future_skew_mins":
            return self.max_future_skew_mins
        if key == "max_payload_bytes":
            return self.max_payload_bytes
        return self.contract.get("thresholds", {}).get(key, default)

    def validate(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Sub-5ms evaluation returning structured validation results and self-healing hints.

        Returns:
            {
                "allowed": bool,
                "violations": List[str],
                "remediation_hints": List[str],
                "contract_version": str,
                "validation_timestamp": str
            }
        """
        violations: List[str] = []
        hints: List[str] = []

        # 1. Nullability & Schema Checks
        for field in self.required_fields:
            fname = field.get("name")
            if fname not in payload or payload[fname] is None:
                violations.append(f"MISSING_REQUIRED_FIELD: '{fname}' cannot be null or omitted.")
                hints.append(f"Populate the '{fname}' property in your event payload before emitting.")

        # 2. Tool Name Allowlist Check
        tool = payload.get("tool_name")
        if tool:
            if self.allowed_tools and tool not in self.allowed_tools:
                allowed_list_str = ", ".join(sorted(list(self.allowed_tools)))
                violations.append(f"UNAUTHORIZED_TOOL: '{tool}' is not in allowed_tools list.")
                hints.append(f"Use one of the authorized tools: [{allowed_list_str}] or request a contract patch.")

        # 3. Numeric Cost Boundaries Check
        cost = payload.get("cost_usd")
        if cost is not None:
            try:
                c_val = float(cost)
                if c_val < 0.0:
                    violations.append(f"NEGATIVE_COST: ${c_val:.4f} is below zero.")
                    hints.append("Cost must be non-negative (>= 0.0 USD).")
                elif c_val > self.max_cost_usd:
                    violations.append(f"COST_LIMIT_BREACH: ${c_val:.2f} exceeds ${self.max_cost_usd:.2f} ceiling.")
                    hints.append(f"Optimize model token parameters or cap invocation cost below ${self.max_cost_usd:.2f}.")
            except (ValueError, TypeError):
                violations.append(f"TYPE_ERROR: 'cost_usd' must be a valid numeric float, got '{cost}'.")
                hints.append("Ensure 'cost_usd' is formatted as a numeric float (e.g., 0.0042).")

        # 4. Execution Time Validation (if present)
        exec_time = payload.get("execution_time_ms")
        if exec_time is not None:
            try:
                t_val = int(exec_time)
                if t_val < 0:
                    violations.append(f"INVALID_EXECUTION_TIME: {t_val}ms cannot be negative.")
                    hints.append("Ensure execution_time_ms is a positive integer.")
            except (ValueError, TypeError):
                violations.append(f"TYPE_ERROR: 'execution_time_ms' must be an integer, got '{exec_time}'.")

        # 5. Timestamp Freshness Window Check
        ts_str = payload.get("timestamp")
        if ts_str:
            try:
                # Handle various ISO-8601 formats
                if isinstance(ts_str, str):
                    ts_clean = ts_str.replace("Z", "+00:00")
                    if " " in ts_clean and "T" not in ts_clean:
                        ts_clean = ts_clean.replace(" ", "T")
                    ts = datetime.fromisoformat(ts_clean)
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                elif isinstance(ts_str, (int, float)):
                    ts = datetime.fromtimestamp(ts_str, tz=timezone.utc)
                else:
                    ts = datetime.fromisoformat(str(ts_str))

                now = datetime.now(timezone.utc)
                if ts < (now - timedelta(hours=self.max_event_age_hours)):
                    violations.append(f"STALE_TIMESTAMP: event is older than {self.max_event_age_hours} hours.")
                    hints.append(f"Synchronize event generation to emit within the {self.max_event_age_hours}h freshness window.")
                elif ts > (now + timedelta(minutes=self.max_future_skew_mins)):
                    violations.append("FUTURE_TIMESTAMP: event timestamp is ahead of server clock.")
                    hints.append(f"Check system clock synchronization (future tolerance: {self.max_future_skew_mins} minutes).")
            except Exception as e:
                violations.append(f"INVALID_TIMESTAMP: Provide valid UTC ISO-8601 string (e.g. '{datetime.now(timezone.utc).isoformat()}'). Details: {str(e)}")
                hints.append("Use standard ISO-8601 formatting for timestamps (e.g., '2026-09-07T12:00:00Z').")

        # 6. Payload Byte Ceiling Check
        payload_body = payload.get("payload") or payload.get("tool_args") or payload
        payload_bytes = len(str(payload_body).encode("utf-8"))
        if payload_bytes > self.max_payload_bytes:
            violations.append(f"PAYLOAD_OVERFLOW: {payload_bytes} bytes exceeds {self.max_payload_bytes} byte ceiling.")
            hints.append("Compress or summarize intermediate tool output before logging.")

        # 7. Optional Suspicious SQL/Command Injection Pattern Checks in tool_args
        tool_args = payload.get("tool_args")
        if isinstance(tool_args, dict):
            suspicious_patterns = [r"DROP\s+TABLE", r"TRUNCATE\s+TABLE", r"DELETE\s+FROM\s+\w+\s*;"]
            for pattern in suspicious_patterns:
                for val in tool_args.values():
                    if re.search(pattern, str(val), re.IGNORECASE):
                        violations.append(f"SUSPICIOUS_PATTERN: Potentially destructive query '{pattern}' detected in tool_args.")
                        hints.append("Sanitize database tool arguments before executing.")

        return {
            "allowed": len(violations) == 0,
            "violations": violations,
            "remediation_hints": hints,
            "contract_version": self.contract.get("version", "1.0.0"),
            "validation_timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def validate_batch(self, payloads: List[dict]) -> List[dict]:
        """Validate multiple payloads in batch."""
        return [self.validate(p) for p in payloads]


# Alias for backwards compatibility with the guide
InFlightContractValidator = ContractValidator
