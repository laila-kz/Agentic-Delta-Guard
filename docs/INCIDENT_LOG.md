# Agent Events Incident Log

Generated: 2026-10-18T12:38:09.569486+00:00
Quarantined records: 5488
Diagnosis provider: deterministic

## Diagnosis

Found 5488 quarantined records across 10 error signatures. 2 contract change(s) proposed.

## Findings

- Observed 1381 record(s) with cost exceeding $50.0 (max observed: $999.00).
- Observed 10 record(s) with invalid negative costs (genuine defect, no contract relaxation).
- Observed 5372 stale record(s) (>24h) and 5 future record(s) (>5m skew) (isolated by contract freshness rule).
- Observed unseen tool name(s): bash_shell_executor.
- Observed quarantine columns differ from the contract schema.

## Error Signatures

- `semantic_rule:cost_out_of_bounds`: 1381 record(s)
- `type_mismatch:cost_usd_not_double`: 1343 record(s)
- `missing_required_field:agent_id`: 1282 record(s)
- `freshness:stale_timestamp`: 723 record(s)
- `freshness:future_timestamp`: 688 record(s)
- `missing_required_field:session_id`: 20 record(s)
- `semantic_rule:tool_not_allowed`: 16 record(s)
- `missing_required_field:action_id`: 15 record(s)
- `missing_required_field:session_id; semantic_rule:cost_out_of_bounds`: 10 record(s)
- `parse_error:invalid_json`: 10 record(s)

## Recommended Contract Patches

- `cost_non_negative`
- `tool_name_allowed_values`
