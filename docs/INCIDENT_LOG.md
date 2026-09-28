# Agent Events Incident Log

Generated: 2026-09-28T17:44:09.240897+00:00
Quarantined records: 221
Diagnosis provider: deterministic

## Diagnosis

Found 221 quarantined records across 6 error signatures.

## Findings

- Cost values violate the contract range [0.0, 50.0].
- Timestamps violate the rolling freshness window.
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
- `timestamp_freshness`
