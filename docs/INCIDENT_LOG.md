# Agent Events Incident Log

Generated: 2026-10-20T13:22:35.213003+00:00
Quarantined records: 7487
Diagnosis provider: deterministic

## Diagnosis

Found 7487 quarantined records across 11 error signatures. 2 contract change(s) proposed.

## Findings

- Observed 1896 record(s) with cost exceeding $50.0 (max observed: $999.00).
- Observed 10 record(s) with invalid negative costs (genuine defect, no contract relaxation).
- Observed 7313 stale record(s) (>24h) and 0 future record(s) (>5m skew) (isolated by contract freshness rule).
- Observed unseen tool name(s): bash_shell_executor, expensive_financial_calc, unauthorized_shell_exec. Safe candidates: ['expensive_financial_calc', 'unauthorized_shell_exec']. SECURITY-FLAGGED (never auto-proposed): ['bash_shell_executor'].
- SECURITY ALERT: 1 privileged tool(s) detected in quarantine: bash_shell_executor. These require explicit human security review and MUST NOT be added to the allowlist without proper vetting.
- Observed quarantine columns differ from the contract schema.

## Error Signatures

- `semantic_rule:cost_out_of_bounds`: 1894 record(s)
- `type_mismatch:cost_usd_not_double`: 1846 record(s)
- `missing_required_field:agent_id`: 1773 record(s)
- `freshness:stale_timestamp`: 961 record(s)
- `freshness:future_timestamp`: 938 record(s)
- `missing_required_field:session_id`: 20 record(s)
- `semantic_rule:tool_not_allowed`: 18 record(s)
- `missing_required_field:action_id`: 15 record(s)
- `missing_required_field:session_id; semantic_rule:cost_out_of_bounds`: 10 record(s)
- `parse_error:invalid_json`: 10 record(s)
- `semantic_rule:cost_out_of_bounds; semantic_rule:tool_not_allowed`: 2 record(s)

## Recommended Contract Patches

- `cost_non_negative`
- `tool_name_allowed_values`
