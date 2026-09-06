# Agent Events Incident Log

Generated: 2026-09-06T14:33:30.296324+00:00
Quarantined records: 1
Diagnosis provider: deterministic

## Diagnosis

Found 1 quarantined records across 1 error signatures.

## Findings

- Timestamps violate the rolling freshness window.
- Observed quarantine columns differ from the contract schema.

## Error Signatures

- `type_mismatch:cost_usd_not_double; missing_required_field:agent_id`: 1 record(s)

## Recommended Contract Patches

- `timestamp_freshness`
