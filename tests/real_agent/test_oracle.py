"""
tests/real_agent/test_oracle.py
Unit tests for the Fault Injector and Automated Oracle.
Runs completely offline with synthetic inputs.
"""

from __future__ import annotations

import pandas as pd
import pytest

from tests.real_agent.fault_injector import SCENARIOS, inject_fault
from tests.real_agent.oracle import evaluate_routing


def test_fault_injector_scenarios():
    """Fault injector correctly mutates payloads and sets manifest expectations."""
    base_event = {
        "agent_id": "agent_alpha",
        "session_id": "sess_101",
        "action_id": "act_202",
        "timestamp": "2026-10-17T12:00:00Z",
        "tool_name": "sql_query_executor",
        "execution_time_ms": 120,
        "cost_usd": 0.05,
        "status": "SUCCESS",
        "tool_args": "{}",
    }

    # Scenario A (baseline)
    evt_a, man_a = inject_fault(base_event, "A")
    assert man_a["expected_route"] == "BRONZE"
    assert evt_a["session_id"] == "sess_101"

    # Scenario B1 (missing session_id)
    evt_b1, man_b1 = inject_fault(base_event, "B1")
    assert man_b1["expected_route"] == "QUARANTINE"
    assert evt_b1["session_id"] is None
    assert "missing_required_field:session_id" in man_b1["expected_signatures"]

    # Scenario B4 (cost anomaly)
    evt_b4, man_b4 = inject_fault(base_event, "B4")
    assert evt_b4["cost_usd"] == 999.0
    assert "semantic_rule:cost_out_of_bounds" in man_b4["expected_signatures"]

    # Scenario B7 (unauthorized tool)
    evt_b7, man_b7 = inject_fault(base_event, "B7")
    assert evt_b7["tool_name"] == "bash_shell_executor"
    assert "semantic_rule:tool_not_allowed" in man_b7["expected_signatures"]

    # Scenario B8 (malformed JSON)
    evt_b8, man_b8 = inject_fault(base_event, "B8")
    assert isinstance(evt_b8, str)
    assert man_b8["expected_route"] == "QUARANTINE"


def test_oracle_confusion_matrix_evaluation():
    """Oracle correctly calculates TP, TN, FP, FN, precision, recall."""
    manifest = [
        # 1. Expected Bronze, present in Bronze -> TP
        {
            "key": {"agent_id": "a1", "session_id": "s1", "action_id": "act_tp"},
            "expected_route": "BRONZE",
            "scenario": "A",
            "expected_signatures": [],
        },
        # 2. Expected Quarantine, present in Quarantine -> TN
        {
            "key": {"agent_id": "a2", "session_id": "s2", "action_id": "act_tn"},
            "expected_route": "QUARANTINE",
            "scenario": "B1",
            "expected_signatures": ["missing_required_field:session_id"],
        },
        # 3. Expected Bronze, but routed to Quarantine -> FP
        {
            "key": {"agent_id": "a3", "session_id": "s3", "action_id": "act_fp"},
            "expected_route": "BRONZE",
            "scenario": "A",
            "expected_signatures": [],
        },
        # 4. Expected Quarantine, but leaked into Bronze -> FN (Contract breach)
        {
            "key": {"agent_id": "a4", "session_id": "s4", "action_id": "act_fn"},
            "expected_route": "QUARANTINE",
            "scenario": "B4",
            "expected_signatures": ["semantic_rule:cost_out_of_bounds"],
        },
    ]

    bronze_data = pd.DataFrame([
        {"agent_id": "a1", "session_id": "s1", "action_id": "act_tp"},
        {"agent_id": "a4", "session_id": "s4", "action_id": "act_fn"},  # Leaked!
    ])

    quarantine_data = pd.DataFrame([
        {
            "agent_id": "a2",
            "session_id": "s2",
            "action_id": "act_tn",
            "error_summary": "missing_required_field:session_id",
        },
        {
            "agent_id": "a3",
            "session_id": "s3",
            "action_id": "act_fp",
            "error_summary": "unexpected_error",
        },
    ])

    report = evaluate_routing(manifest, bronze_data, quarantine_data)
    cm = report["confusion_matrix"]
    m = report["metrics"]

    assert cm["true_positives_tp"] == 1
    assert cm["true_negatives_tn"] == 1
    assert cm["false_positives_fp"] == 1
    assert cm["false_negatives_fn_breach"] == 1
    assert m["precision"] == 0.5  # TP / (TP + FP) = 1 / 2
    assert m["recall"] == 0.5     # TP / (TP + FN) = 1 / 2
    assert m["signature_match_accuracy"] == 1.0  # act_tn had matching signature
