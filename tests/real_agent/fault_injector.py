"""
tests/real_agent/fault_injector.py
Controlled fault injection proxy and ground-truth manifest generator.

Sits between agent telemetry and Kafka to inject deterministic contract faults
and record the expected routing verdict for the Oracle.
"""

from __future__ import annotations

import copy
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("fault_injector")

# Scenario definitions and expectations
SCENARIOS = {
    "A": {
        "description": "Baseline valid agent execution",
        "expected_route": "BRONZE",
        "expected_signatures": [],
    },
    "B1": {
        "description": "Schema violation: missing required session_id",
        "expected_route": "QUARANTINE",
        "expected_signatures": ["missing_required_field:session_id"],
    },
    "B2": {
        "description": "Schema violation: missing required action_id",
        "expected_route": "QUARANTINE",
        "expected_signatures": ["missing_required_field:action_id"],
    },
    "B3": {
        "description": "Type mismatch: cost_usd is non-numeric string",
        "expected_route": "QUARANTINE",
        "expected_signatures": ["type_mismatch:cost_usd_not_double"],
    },
    "B4": {
        "description": "Semantic violation: cost_usd exceeds $50 ceiling",
        "expected_route": "QUARANTINE",
        "expected_signatures": ["semantic_rule:cost_out_of_bounds"],
    },
    "B5": {
        "description": "Freshness violation: timestamp is 48 hours stale",
        "expected_route": "QUARANTINE",
        "expected_signatures": ["freshness:stale_timestamp"],
    },
    "B6": {
        "description": "Freshness violation: timestamp is 1 hour in the future",
        "expected_route": "QUARANTINE",
        "expected_signatures": ["freshness:future_timestamp"],
    },
    "B7": {
        "description": "Tool allowlist: unauthorized tool (bash_shell_executor)",
        "expected_route": "QUARANTINE",
        "expected_signatures": ["semantic_rule:tool_not_allowed"],
    },
    "B8": {
        "description": "Transport error: truncated / malformed JSON payload",
        "expected_route": "QUARANTINE",
        "expected_signatures": ["parse_error:invalid_json"],
    },
    "B9": {
        "description": "Multi-fault: missing session_id AND negative cost",
        "expected_route": "QUARANTINE",
        "expected_signatures": ["missing_required_field:session_id", "semantic_rule:cost_out_of_bounds"],
    },
    "B10": {
        "description": "Late watermark: event older than 10-minute watermark window",
        "expected_route": "DROPPED",
        "expected_signatures": [],
    },
}


def inject_fault(event: Dict[str, Any], scenario_id: str) -> Tuple[Any, Dict[str, Any]]:
    """Applies scenario defect to an event and generates oracle manifest record.

    Returns:
        (mutated_payload_to_emit, manifest_entry)
    """
    mutated = copy.deepcopy(event)
    now = datetime.now(timezone.utc)
    scenario_meta = SCENARIOS.get(scenario_id, SCENARIOS["A"])

    expected_route = scenario_meta["expected_route"]
    expected_sigs = list(scenario_meta["expected_signatures"])

    if scenario_id == "B1":
        mutated["session_id"] = None
    elif scenario_id == "B2":
        mutated["action_id"] = None
    elif scenario_id == "B3":
        mutated["cost_usd"] = "UNMETERED_FREE_TIER"
    elif scenario_id == "B4":
        mutated["cost_usd"] = 999.0
    elif scenario_id == "B5":
        mutated["timestamp"] = (now - timedelta(hours=48)).isoformat()
    elif scenario_id == "B6":
        mutated["timestamp"] = (now + timedelta(hours=1)).isoformat()
    elif scenario_id == "B7":
        mutated["tool_name"] = "bash_shell_executor"
    elif scenario_id == "B8":
        # Truncated raw string
        raw_str = json.dumps(mutated)[:35]
        manifest_record = {
            "key": {
                "agent_id": event.get("agent_id"),
                "session_id": event.get("session_id"),
                "action_id": event.get("action_id"),
            },
            "raw_payload_snippet": raw_str,
            "scenario": scenario_id,
            "expected_route": expected_route,
            "expected_signatures": expected_sigs,
            "timestamp": now.isoformat(),
        }
        return raw_str, manifest_record
    elif scenario_id == "B9":
        mutated["session_id"] = None
        mutated["cost_usd"] = -5.0
    elif scenario_id == "B10":
        # Past 10-min watermark (e.g. 20 minutes ago)
        mutated["timestamp"] = (now - timedelta(minutes=20)).isoformat()

    manifest_record = {
        "key": {
            "agent_id": mutated.get("agent_id"),
            "session_id": mutated.get("session_id"),
            "action_id": mutated.get("action_id"),
        },
        "scenario": scenario_id,
        "expected_route": expected_route,
        "expected_signatures": expected_sigs,
        "timestamp": now.isoformat(),
    }

    return mutated, manifest_record


class FaultInjectingProducerProxy:
    """Proxy sitting between the agent and Kafka that applies fault injection and logs manifests."""

    def __init__(
        self,
        base_producer: Any,
        manifest_path: Path,
        scenario_id: str = "A",
    ):
        self.base_producer = base_producer
        self.manifest_path = manifest_path
        self.scenario_id = scenario_id
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        self._manifest_fh = open(self.manifest_path, "a", encoding="utf-8")
        self.manifest_records: List[Dict[str, Any]] = []

    @property
    def dropped_count(self) -> int:
        return getattr(self.base_producer, "dropped_count", 0)

    @property
    def recorded_events(self) -> List[Any]:
        return getattr(self.base_producer, "recorded_events", [])

    def send(self, event: Dict[str, Any]) -> None:
        mutated_payload, manifest_rec = inject_fault(event, self.scenario_id)
        self.manifest_records.append(manifest_rec)
        self._manifest_fh.write(json.dumps(manifest_rec) + "\n")
        self._manifest_fh.flush()

        # Send to base Kafka producer
        self.base_producer.send(mutated_payload)

    def flush(self, timeout: float = 5.0) -> None:
        if hasattr(self.base_producer, "flush"):
            try:
                self.base_producer.flush(timeout=timeout)
            except TypeError:
                self.base_producer.flush()

    def close(self) -> None:
        if self._manifest_fh:
            try:
                self._manifest_fh.close()
            except Exception:
                pass
        if hasattr(self.base_producer, "close"):
            self.base_producer.close()
