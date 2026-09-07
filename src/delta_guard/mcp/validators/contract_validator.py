"""
src/delta_guard/mcp/validators/contract_validator.py
Synchronous, sub-5ms in-memory contract enforcement engine for MCP tool calls.
"""

from __future__ import annotations
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
import json
import yaml
import re


class ContractValidator:
    """
    Zero-overhead synchronous validator compiled from agent_contract.yaml.
    Optimized for sub-5ms execution.
    """
    
    def __init__(self, contract_path: str | Path = "configs/agent_contract.yaml"):
        self.contract_path = Path(contract_path)
        # Create parent directories if they don't exist
        if not self.contract_path.is_absolute():
            self.contract_path = Path(__file__).parent.parent.parent.parent / self.contract_path
        self.contract = self._load_contract()
        self._cache = {}  # Simple cache for repeated validations
        
    def _load_contract(self) -> dict[str, Any]:
        """Load and parse the contract YAML"""
        if not self.contract_path.exists():
            return self._get_default_contract()
        
        with open(self.contract_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or self._get_default_contract()
    
    def _get_default_contract(self) -> dict[str, Any]:
        """Fallback contract if file doesn't exist"""
        return {
            "version": "1.0.0",
            "schema": {
                "fields": [
                    {"name": "agent_id", "type": "string", "nullable": False},
                    {"name": "tool_name", "type": "string", "nullable": False},
                    {"name": "cost_usd", "type": "float", "nullable": False},
                    {"name": "timestamp", "type": "string", "nullable": False},
                ]
            },
            "allowed_tools": ["search_tool", "python_repl", "database_writer", "rag_retriever"],
            "thresholds": {
                "max_cost_per_call": 50.0,
                "max_payload_bytes": 32768,
                "freshness_window_hours": 24,
                "future_tolerance_minutes": 5
            }
        }
    
    def reload(self) -> None:
        """Hot-reload contract on file changes"""
        self.contract = self._load_contract()
        self._cache.clear()
    
    def get_allowed_tools(self) -> List[str]:
        """Get list of allowed tool names"""
        return self.contract.get("allowed_tools", [])
    
    def get_threshold(self, key: str, default: Any) -> Any:
        """Get a threshold value from the contract"""
        return self.contract.get("thresholds", {}).get(key, default)
    
    def validate(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Sub-5ms evaluation returning structured validation results.
        
        Returns:
            {
                "allowed": bool,
                "violations": List[str],
                "remediation_hints": List[str],
                "contract_version": str
            }
        """
        violations = []
        hints = []
        
        # 1. Schema & Nullability Checks
        for field in self.contract.get("schema", {}).get("fields", []):
            fname = field.get("name")
            nullable = field.get("nullable", True)
            
            if not nullable and (fname not in payload or payload[fname] is None):
                violations.append(f"MISSING_REQUIRED_FIELD: '{fname}' cannot be null")
                hints.append(f"Populate the '{fname}' field in your event payload")
        
        # 2. Tool Name Allowlist
        allowed_tools = self.get_allowed_tools()
        tool = payload.get("tool_name")
        if allowed_tools and tool:
            if tool not in allowed_tools:
                violations.append(f"UNAUTHORIZED_TOOL: '{tool}' not in allowed list")
                hints.append(f"Use one of: {', '.join(allowed_tools[:5])}...")
        
        # 3. Cost Boundaries
        cost = payload.get("cost_usd")
        if cost is not None:
            try:
                c_val = float(cost)
                max_c = float(self.get_threshold("max_cost_per_call", 50.0))
                if c_val < 0.0:
                    violations.append(f"NEGATIVE_COST: ${c_val:.2f} is below zero")
                    hints.append("Cost must be non-negative")
                elif c_val > max_c:
                    violations.append(f"COST_LIMIT_BREACH: ${c_val:.2f} exceeds ${max_c:.2f} ceiling")
                    hints.append(f"Optimize token usage or request cost limit increase")
            except (ValueError, TypeError):
                violations.append("TYPE_ERROR: 'cost_usd' must be a valid number")
                hints.append("Ensure 'cost_usd' is a float (e.g., 0.0042)")
        
        # 4. Timestamp Freshness
        ts_str = payload.get("timestamp")
        if ts_str:
            try:
                # Parse timestamp (handle Z suffix)
                ts_str_clean = ts_str.replace("Z", "+00:00")
                ts = datetime.fromisoformat(ts_str_clean)
                now = datetime.now(timezone.utc)
                
                max_age_h = self.get_threshold("freshness_window_hours", 24)
                future_m = self.get_threshold("future_tolerance_minutes", 5)
                
                if ts < (now - timedelta(hours=max_age_h)):
                    violations.append(f"STALE_TIMESTAMP: event older than {max_age_h} hours")
                    hints.append(f"Ensure timestamp within last {max_age_h} hours")
                elif ts > (now + timedelta(minutes=future_m)):
                    violations.append(f"FUTURE_TIMESTAMP: event from the future")
                    hints.append("Check system clock synchronization")
            except (ValueError, TypeError) as e:
                violations.append(f"INVALID_TIMESTAMP: {str(e)}")
                hints.append("Use ISO-8601 format (e.g., '2026-09-07T12:00:00Z')")
        
        # 5. Payload Size Check
        if "payload" in payload:
            payload_bytes = len(str(payload["payload"]).encode("utf-8"))
            max_bytes = self.get_threshold("max_payload_bytes", 32768)
            if payload_bytes > max_bytes:
                violations.append(f"PAYLOAD_OVERFLOW: {payload_bytes} bytes > {max_bytes} bytes")
                hints.append("Compress or summarize tool output before logging")
        
        # 6. Optional: Check for allowed tool arguments
        tool_args = payload.get("tool_args")
        if tool_args and isinstance(tool_args, dict):
            # Check for suspicious patterns (basic security)
            suspicious_patterns = [r'DROP\s+TABLE', r'DELETE\s+FROM', r'TRUNCATE\s+TABLE']
            for pattern in suspicious_patterns:
                if any(re.search(pattern, str(val), re.IGNORECASE) for val in tool_args.values()):
                    violations.append(f"SUSPICIOUS_PATTERN: '{pattern}' detected in tool_args")
                    hints.append("Review tool arguments for malicious patterns")
        
        return {
            "allowed": len(violations) == 0,
            "violations": violations,
            "remediation_hints": hints,
            "contract_version": self.contract.get("version", "1.0.0"),
            "validation_timestamp": datetime.now(timezone.utc).isoformat()
        }
    
    def validate_batch(self, payloads: List[dict]) -> List[dict]:
        """Validate multiple payloads in batch"""
        return [self.validate(p) for p in payloads]
