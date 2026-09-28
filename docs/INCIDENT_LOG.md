# Agent Events Incident Log

Generated: 2026-09-28T19:53:54.527141+00:00
Quarantined records: 221
Diagnosis provider: deterministic

## Diagnosis

Found 221 quarantined records across 6 error signatures. 1 contract change(s) proposed.

## Findings

- Observed 60 record(s) with cost exceeding $50.0 (max observed: $850.00).
- Observed 220 stale record(s) (>24h) and 0 future record(s) (>5m skew) (isolated by contract freshness rule).
- Observed quarantine columns differ from the contract schema.

## Error Signatures

- `semantic_rule:cost_out_of_bounds`: 60 record(s)
- `missing_required_field:agent_id`: 55 record(s)
- `type_mismatch:cost_usd_not_double`: 55 record(s)
- `freshness:future_timestamp`: 29 record(s)
- `freshness:stale_timestamp`: 21 record(s)
- `type_mismatch:cost_usd_not_double; missing_required_field:agent_id`: 1 record(s)

## Recommended Contract Patches

- `cost_non_negative`

The proposed upper bound 935.0 is computed as `max_observed_cost * 1.1` (10% headroom),
floored at 60.0 and rounded to 1 decimal: `round(max(850.0 * 1.1, 60.0), 1) = 935.0`
(`src/delta_guard/triage.py:145`). This is an auto-proposal, not an approved change.
