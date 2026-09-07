# Triage Engine Sample Run

## Run Details

- **Date:** 2026-09-06
- **Mode:** Deterministic fallback
- **Quarantine records analyzed:** 1
- **Error signatures:** `type_mismatch:cost_usd_not_double; missing_required_field:agent_id` (1)

This artifact is a captured local run of the same deterministic path exercised by CI. It does not claim a live provider response. Live verification remains an explicit, manual test because it requires an external API request.

## Generated Artifacts

- `triage_deterministic_run.txt`: console output from `python src/delta_guard/triage.py`
- `INCIDENT_LOG.md`: generated incident report
- `agent_contract_proposed.yaml`: generated proposed contract

## Verification

- [x] Error-signature clustering works
- [x] Incident report generation works
- [x] Proposed contract generation works
- [x] Deterministic mode works without an API key
- [ ] Live provider response captured separately with a rotated key

## Observed Diagnosis

- **Provider:** deterministic
- **Finding:** timestamp violates the rolling freshness window
- **Metrics:** 1 stale timestamp, 0 cost anomalies
- **Proposed patch:** `timestamp_freshness`

## Live API Mode

After rotating any exposed key, run the live check intentionally:

```powershell
$env:OPENAI_API_KEY = "<rotated-key>"
$env:LIVE_LLM_TEST = "1"
python -m pytest tests/test_contract_validation.py -k llm_triage_with_openai -v
```