"""LLM-assisted and deterministic triage for quarantined agent events."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class LLMTriageEngine:
    def __init__(
        self,
        quarantine_path: str | Path = "data/quarantine/agent_events",
        contract_path: str | Path = "configs/agent_contract.yaml",
        incident_log_path: str | Path = "docs/INCIDENT_LOG.md",
        proposed_contract_path: str | Path = "configs/agent_contract_proposed.yaml",
    ) -> None:
        self.quarantine_path = self._resolve_path(quarantine_path)
        self.contract_path = self._resolve_path(contract_path)
        self.incident_log_path = self._resolve_path(incident_log_path)
        self.proposed_contract_path = self._resolve_path(proposed_contract_path)
        self.incident_log_path.parent.mkdir(parents=True, exist_ok=True)
        self.proposed_contract_path.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _resolve_path(path: str | Path) -> Path:
        resolved = Path(path)
        if not resolved.is_absolute():
            resolved = PROJECT_ROOT / resolved
        return resolved.resolve()

    def load_quarantine_records(self) -> pd.DataFrame:
        """Load quarantine records through DuckDB Delta or Parquet readers."""
        path = str(self.quarantine_path).replace("'", "''")
        connection = duckdb.connect()
        try:
            try:
                return connection.execute(
                    f"select * from delta_scan('{path}')"
                ).df()
            except Exception:
                parquet_path = str(self.quarantine_path / "**" / "*.parquet").replace(
                    "'", "''"
                )
                return connection.execute(
                    f"select * from read_parquet('{parquet_path}')"
                ).df()
        finally:
            connection.close()

    def _read_contract(self) -> dict[str, Any]:
        if not self.contract_path.exists():
            return {}
        with self.contract_path.open("r", encoding="utf-8") as contract_file:
            return yaml.safe_load(contract_file) or {}

    def cluster_error_signatures(
        self, records: pd.DataFrame | None = None
    ) -> list[dict[str, Any]]:
        """Group quarantined rows by their gatekeeper error signature."""
        records = self.load_quarantine_records() if records is None else records
        if records.empty:
            return []

        frame = records.copy()
        if "error_summary" not in frame:
            frame["error_summary"] = "unknown"
        else:
            frame["error_summary"] = frame["error_summary"].fillna("unknown")
        clusters = []
        for signature, group in frame.groupby("error_summary", dropna=False):
            samples = group.head(3).where(pd.notna(group.head(3)), None)
            clusters.append(
                {
                    "signature": str(signature),
                    "count": int(len(group)),
                    "affected_agents": sorted(
                        {str(value) for value in group.get("agent_id", pd.Series()).dropna()}
                    ),
                    "affected_tools": sorted(
                        {str(value) for value in group.get("tool_name", pd.Series()).dropna()}
                    ),
                    "sample_payloads": samples.to_dict(orient="records"),
                }
            )
        return sorted(clusters, key=lambda cluster: cluster["count"], reverse=True)

    def _deterministic_triage_engine(
        self, records: pd.DataFrame, clusters: list[dict[str, Any]], contract: dict[str, Any]
    ) -> dict[str, Any]:
        """Diagnose common quarantine causes without an external model and emit only novel rule diffs."""
        frame = records.copy()
        costs = pd.to_numeric(frame.get("cost_usd", pd.Series(dtype=float)), errors="coerce")
        timestamps = pd.to_datetime(
            frame.get("timestamp", pd.Series(dtype=str)), errors="coerce", utc=True
        )
        now = datetime.now(timezone.utc)
        stale = timestamps < pd.Timestamp(now - timedelta(hours=24))
        future = timestamps > pd.Timestamp(now + timedelta(minutes=5))

        schema_fields = {
            field.get("name")
            for field in contract.get("schema", {}).get("fields", [])
            if field.get("name")
        }
        observed_fields = set(frame.columns) - {"error_summary", "quarantined_at", "errors"}
        schema_drift = {
            "missing_fields": sorted(schema_fields - observed_fields),
            "unexpected_fields": sorted(observed_fields - schema_fields),
        }

        # Check existing active semantic rules
        active_rules = {r.get("id"): r.get("rule", "") for r in contract.get("semantic_rules", [])}

        # Check tool allowed values
        tool_field = next(
            (f for f in contract.get("schema", {}).get("fields", []) if f.get("name") == "tool_name"),
            {},
        )
        allowed_tools = set(tool_field.get("allowed_values", []))
        observed_tools = set(frame.get("tool_name", pd.Series(dtype=str)).dropna().unique())
        unseen_tools = sorted(observed_tools - allowed_tools)

        findings = []
        patches = []

        # 1. Cost analysis
        if (costs > 50.0).any():
            max_cost = float(costs.max())
            high_cost_count = int((costs > 50.0).sum())
            findings.append(
                f"Observed {high_cost_count} record(s) with cost exceeding $50.0 (max observed: ${max_cost:.2f})."
            )
            widened_bound = round(max(max_cost * 1.1, 60.0), 1)
            new_rule = f"cost_usd >= 0.0 AND cost_usd <= {widened_bound}"
            if active_rules.get("cost_non_negative") != new_rule:
                patches.append(
                    {
                        "rule_id": "cost_non_negative",
                        "rule": new_rule,
                        "message": f"cost_usd out of valid boundaries [0.0, {widened_bound}]",
                        "evidence_count": high_cost_count,
                        "max_observed_cost": max_cost,
                        "rationale": f"Widened upper cost bound from $50.0 to ${widened_bound} (max observed: ${max_cost:.2f} + 10% buffer) based on {high_cost_count} observed executions.",
                    }
                )
        if (costs < 0).any():
            neg_count = int((costs < 0).sum())
            findings.append(f"Observed {neg_count} record(s) with invalid negative costs (genuine defect, no contract relaxation).")

        # 2. Freshness analysis
        if stale.any() or future.any():
            stale_count = int(stale.fillna(False).sum())
            future_count = int(future.fillna(False).sum())
            findings.append(
                f"Observed {stale_count} stale record(s) (>24h) and {future_count} future record(s) (>5m skew) (isolated by contract freshness rule)."
            )

        # 3. Tool name evolution
        if unseen_tools:
            findings.append(f"Observed unseen tool name(s): {', '.join(unseen_tools)}.")
            patches.append(
                {
                    "rule_id": "tool_name_allowed_values",
                    "field": "tool_name",
                    "added_tools": unseen_tools,
                    "evidence_count": int(frame["tool_name"].isin(unseen_tools).sum()),
                    "rationale": f"Add newly observed tool(s) {unseen_tools} to allowed_values.",
                }
            )

        if schema_drift["missing_fields"] or schema_drift["unexpected_fields"]:
            findings.append("Observed quarantine columns differ from the contract schema.")

        summary = (
            f"Found {len(records)} quarantined records across {len(clusters)} error signatures. "
            f"{len(patches)} contract change(s) proposed."
            if patches
            else f"Found {len(records)} quarantined records across {len(clusters)} error signatures. No contract change recommended (violations represent genuine poison isolated by active rules)."
        )

        return {
            "provider": "deterministic",
            "summary": summary,
            "findings": findings or ["No known cost, freshness, or schema-drift anomaly detected."],
            "schema_drift": schema_drift,
            "metrics": {
                "cost_anomaly_rows": int(((costs > 50) | (costs < 0)).sum()),
                "stale_timestamp_rows": int(stale.fillna(False).sum()),
                "future_timestamp_rows": int(future.fillna(False).sum()),
            },
            "recommended_patches": patches,
        }

    def _call_llm(self, prompt: str, max_retries: int = 3) -> dict[str, Any] | None:
        """Call an available LLM provider with exponential-backoff retry.

        Retries up to `max_retries` times on transient network/rate-limit errors
        before giving up and returning None (caller falls back to deterministic
        triage). The backoff is 2^attempt seconds (1s, 2s, 4s).
        """
        import time

        if os.getenv("OPENAI_API_KEY"):
            url = "https://api.openai.com/v1/chat/completions"
            headers = {"Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}"}
            payload = {
                "model": os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
                "messages": [{"role": "user", "content": prompt}],
                "response_format": {"type": "json_object"},
            }
        elif os.getenv("ANTHROPIC_API_KEY"):
            url = "https://api.anthropic.com/v1/messages"
            headers = {
                "x-api-key": os.environ["ANTHROPIC_API_KEY"],
                "anthropic-version": "2023-06-01",
            }
            payload = {
                "model": os.getenv("ANTHROPIC_MODEL", "claude-3-5-haiku-latest"),
                "max_tokens": 2000,
                "messages": [{"role": "user", "content": prompt}],
            }
        else:
            return None

        request = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={**headers, "Content-Type": "application/json"},
            method="POST",
        )

        last_error: Exception | None = None
        for attempt in range(max_retries):
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    body = json.loads(response.read().decode("utf-8"))
                if "choices" in body:
                    content = body["choices"][0]["message"]["content"]
                else:
                    content = body["content"][0]["text"]
                return json.loads(content)
            except urllib.error.HTTPError as exc:
                # 429 = rate limit; 5xx = server error — both are retryable
                if exc.code in (429, 500, 502, 503, 504):
                    last_error = exc
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                    continue
                # 4xx that are not 429 are not retryable (bad key, bad request…)
                return None
            except (OSError, ValueError, KeyError, IndexError, urllib.error.URLError) as exc:
                last_error = exc
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)

        import logging
        logging.getLogger(__name__).warning(
            "LLM call failed after %d attempts, falling back to deterministic triage: %s",
            max_retries,
            last_error,
        )
        return None

    def generate_llm_diagnosis(
        self, records: pd.DataFrame, clusters: list[dict[str, Any]]
    ) -> dict[str, Any]:
        contract = self._read_contract()
        deterministic = self._deterministic_triage_engine(records, clusters, contract)
        prompt = (
            "Diagnose these quarantined data incidents. Return JSON with keys "
            "summary, findings, schema_drift, metrics, recommended_patches.\n"
            + json.dumps({"contract": contract, "clusters": clusters}, default=str)
        )
        llm_diagnosis = self._call_llm(prompt)
        if llm_diagnosis is None:
            return deterministic
        llm_diagnosis["provider"] = "openai_or_anthropic"
        llm_diagnosis.setdefault("recommended_patches", deterministic["recommended_patches"])
        return llm_diagnosis

    def run_triage(self) -> dict[str, Any]:
        records = self.load_quarantine_records()
        clusters = self.cluster_error_signatures(records)
        diagnosis = self.generate_llm_diagnosis(records, clusters)
        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "quarantine_path": str(self.quarantine_path),
            "record_count": len(records),
            "clusters": clusters,
            "diagnosis": diagnosis,
        }
        self._write_incident_report(report)
        self._write_proposed_contract(diagnosis.get("recommended_patches", []))
        self._update_status_with_triage(clusters, diagnosis)
        return report

    def _update_status_with_triage(
        self, clusters: list[dict[str, Any]], diagnosis: dict[str, Any]
    ) -> None:
        """Merge triage results into status.json so the console shows live signatures."""
        status_path = PROJECT_ROOT / "status.json"
        # Read existing status (written by gatekeeper) or start fresh
        status: dict = {}
        if status_path.exists():
            try:
                status = json.loads(status_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                status = {}
        # Merge signature counts from latest triage run
        sig_counts: dict[str, int] = {}
        for cluster in clusters:
            sig_counts[cluster["signature"]] = cluster["count"]
        status["per_signature_counts"] = sig_counts
        # Patch info
        patches = diagnosis.get("recommended_patches", [])
        status["patch_count"] = len(patches)
        if patches and self.proposed_contract_path.exists():
            try:
                rel = self.proposed_contract_path.relative_to(PROJECT_ROOT)
                status["last_patch"] = str(rel)
            except ValueError:
                status["last_patch"] = str(self.proposed_contract_path)

        # Atomic write
        tmp = str(status_path) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(status, fh)
        os.replace(tmp, str(status_path))

    def _write_incident_report(self, report: dict[str, Any]) -> None:
        diagnosis = report["diagnosis"]
        lines = [
            "# Agent Events Incident Log",
            "",
            f"Generated: {report['generated_at']}",
            f"Quarantined records: {report['record_count']}",
            f"Diagnosis provider: {diagnosis.get('provider', 'unknown')}",
            "",
            "## Diagnosis",
            "",
            diagnosis.get("summary", "No summary available."),
            "",
            "## Findings",
            "",
        ]
        lines.extend(f"- {finding}" for finding in diagnosis.get("findings", []))
        lines.extend(["", "## Error Signatures", ""])
        for cluster in report["clusters"]:
            lines.append(f"- `{cluster['signature']}`: {cluster['count']} record(s)")
        lines.extend(["", "## Recommended Contract Patches", ""])
        patches = diagnosis.get("recommended_patches", [])
        lines.extend(f"- `{patch.get('rule_id', 'unknown')}`" for patch in patches)
        if not patches:
            lines.append("- None")
        self.incident_log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    def _write_proposed_contract(self, patches: list[dict[str, Any]]) -> None:
        contract = self._read_contract()
        proposed = json.loads(json.dumps(contract))
        existing = {
            rule.get("id"): rule
            for rule in proposed.get("semantic_rules", [])
            if rule.get("id")
        }
        for patch in patches:
            rule_id = patch.get("rule_id")
            if rule_id == "tool_name_allowed_values":
                for field in proposed.get("schema", {}).get("fields", []):
                    if field.get("name") == "tool_name":
                        current = set(field.get("allowed_values", []))
                        field["allowed_values"] = sorted(list(current | set(patch.get("added_tools", []))))
            elif rule_id and rule_id in existing:
                existing[rule_id] = {
                    "id": rule_id,
                    "rule": patch.get("rule", ""),
                    "message": patch.get("message", ""),
                }
        proposed["semantic_rules"] = list(existing.values())
        proposed["proposed_at"] = datetime.now(timezone.utc).isoformat()
        with self.proposed_contract_path.open("w", encoding="utf-8") as contract_file:
            yaml.safe_dump(proposed, contract_file, sort_keys=False)


if __name__ == "__main__":
    result = LLMTriageEngine().run_triage()
    print(json.dumps(result["diagnosis"], indent=2, default=str))
