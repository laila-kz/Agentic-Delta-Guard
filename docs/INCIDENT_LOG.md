# Agent Events Incident Log

Generated: 2026-10-08T16:49:53.127863+00:00
Quarantined records: 5357
Diagnosis provider: deterministic

## Diagnosis

Found 5357 quarantined records across 5 error signatures. 1 contract change(s) proposed.

## Findings

- Observed 1366 record(s) with cost exceeding $50.0 (max observed: $850.00).
- Observed 4684 stale record(s) (>24h) and 0 future record(s) (>5m skew) (isolated by contract freshness rule).
- Observed quarantine columns differ from the contract schema.

## Error Signatures

- `semantic_rule:cost_out_of_bounds`: 1366 record(s)
- `type_mismatch:cost_usd_not_double`: 1328 record(s)
- `missing_required_field:agent_id`: 1282 record(s)
- `freshness:stale_timestamp`: 708 record(s)
- `freshness:future_timestamp`: 673 record(s)

## Recommended Contract Patches

- `cost_non_negative`
