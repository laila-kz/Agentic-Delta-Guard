# PROJECT_EXPLAINED.md
## Agentic Delta Guard — Interview Revision Guide

_Written from the code, not the marketing copy. Every claim here is traceable to a file._

---

## 1. The Problem, in Plain Terms

Imagine you give an AI agent — a tool-calling LLM like a CrewAI or LangGraph worker — write access to your data lake. It fires off tool calls: SQL queries, vector searches, web scrapes, database writes. Each call produces a log event with fields like `agent_id`, `tool_name`, `cost_usd`, and a `timestamp`. Those events flow into your streaming pipeline.

Here is what breaks, concretely, without any guard:

**Schema hallucination.** The agent generates the payload dynamically. Sometimes it emits `cost_usd = "UNMETERED"` instead of a float. Sometimes it drops `agent_id` entirely because it hallucinated an alternate field name. A traditional Kafka consumer will either silently cast `null` into your table or crash the whole micro-batch job. Neither is acceptable — one poisons your data, the other stops all processing.

**Cost runaways.** An agent in a loop can fire thousands of tool calls per minute. If `cost_usd = 850.0` lands in Bronze with no check, your financial reconciliation query reports absurd numbers days later. There is no alarm at ingestion time.

**One bad record kills the batch.** Classic Spark streaming with `mode("failOnError")` crashes the entire micro-batch when it encounters a schema violation. You lose all the good records in that batch too, and the job stays paused until an engineer restarts it.

**No audit trail.** If a bad record does get into Bronze, you have no record of what was wrong with it, when it arrived, or which error signature it matched. Incident response starts from scratch every time.

The project addresses all four: it validates each event against a versioned contract before it touches Bronze, routes violations to a separate Quarantine table that preserves the full payload, clusters them by error signature, and proposes a contract patch. Bad records never touch Bronze, and the error trail is complete.

---

## 2. End-to-End Walkthrough

### 2a. Event emission — `src/delta_guard/producer.py`

The producer is a standalone Python process using `kafka-python`. It generates synthetic events with `Faker`, applies a 15% poison injection rate (randomly triggering one of five violation types), and sends them to the `agent-events` Kafka topic at roughly 5 events/second.

Why does this exist? It lets the pipeline demonstrate end-to-end behaviour without needing real agents. In production this component is replaced by actual agent frameworks publishing to Kafka.

The poison types injected:
- `cost_usd = 850.0` → semantic rule violation
- `cost_usd = "UNMETERED"` → type mismatch
- `agent_id = null` → missing required field
- `timestamp = now + 48h` → future timestamp
- `timestamp = now - 48h` → stale timestamp

### 2b. Message bus — Kafka (KRaft, single broker)

Events land in the `agent-events` topic, one partition, replication factor 1. Kafka decouples the producers (agents) from the consumer (gatekeeper), giving backpressure and replay. The gatekeeper reads from `startingOffsets = "earliest"` so it can process a backlog on startup.

The single-broker setup is a deliberate scope limitation — covered honestly in section 4.

### 2c. The gatekeeper — `src/delta_guard/gatekeeper.py`

This is the core of the project. It is a PySpark Structured Streaming job running in micro-batch mode with a 10-second trigger.

**Step 1: Read from Kafka.**
```python
spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
    .option("subscribe", "agent-events")
    .option("startingOffsets", "earliest")
    .load()
```
Each Kafka message arrives as a binary `value` column.

**Step 2: Parse JSON.** The `value` is cast to string and parsed with `from_json()` against a hard-coded `EVENT_SCHEMA`. The schema intentionally reads `cost_usd` as a `StringType` — not a double — so that malformed values like `"UNMETERED"` are captured rather than silently dropped or null-coerced by Spark.

**Step 3: Watermarking.** Before deduplication, a 10-minute watermark is applied on `event_timestamp`. This tells Spark's streaming engine: "drop state for events that arrive more than 10 minutes late relative to the latest event seen." Without this, Spark accumulates unbounded state for deduplication, which eventually causes an OOM crash.

**Step 4: Deduplication.** `dropDuplicates(["agent_id", "session_id", "action_id"])` within the watermark window. This is the streaming-side idempotency guard. If the same event is replayed (e.g. Kafka offset re-read after a gatekeeper crash), it is dropped here before reaching the Delta MERGE.

**Step 5: Validation — building the `errors` array.**
```python
F.filter(
    F.array(
        F.when(F.col("agent_id").isNull(), "missing_required_field:agent_id"),
        F.when(F.col("cost_usd_double").isNull(), "type_mismatch:cost_usd_not_double"),
        F.when((cost > 50) | (cost < 0), "semantic_rule:cost_out_of_bounds"),
        F.when(ts < now - 24h, "freshness:stale_timestamp"),
        F.when(ts > now + 5min, "freshness:future_timestamp"),
    ),
    lambda e: e.isNotNull()
)
```
This runs distributed on Spark workers — no Python UDFs, no driver bottleneck. Each row gets an `errors` array; empty array means valid.

**Step 6: Split and route.**
- `errors.size == 0` → `valid_df` → Bronze
- `errors.size > 0` → `quarantine_df` → Quarantine

**Step 7: Bronze write via MERGE INTO.**

If Bronze doesn't exist yet, the first write uses `overwrite`. Subsequent writes use Delta's `MERGE INTO` on the idempotency key `(agent_id, session_id, action_id)` — only inserting rows that do not already exist in the target. This is the Delta-side idempotency guard (the streaming watermark + dropDuplicates is the Spark-side guard; they work in tandem).

**Step 8: Quarantine write — append.**

Quarantine uses simple `append` mode, not MERGE. The full payload is written with two extra columns: `quarantined_at` (timestamp) and `error_summary` (semicolon-joined list of error codes). Quarantine rows are never updated — they are an immutable audit trail.

**Step 9: status.json write.**

After every batch commit, the gatekeeper atomically writes `status.json` via a `tmp → os.replace()` pattern. This is what the console polls.

### 2d. Triage — `src/delta_guard/triage.py`

After some data accumulates in Quarantine, `LLMTriageEngine.run_triage()` does three things:

1. **Loads quarantine records** from Delta Lake via `delta_scan()` in DuckDB.
2. **Clusters by `error_summary`** — groups rows by their gatekeeper-assigned error signature, counts them, identifies affected agents and tools.
3. **Diagnoses** — first runs a deterministic engine that checks for known patterns (cost out of bounds, stale timestamps, schema drift). If an LLM API key is available (OpenAI or Anthropic), it also calls the LLM and merges the results.
4. **Writes `docs/INCIDENT_LOG.md`** and **`configs/agent_contract_proposed.yaml`** — the proposed contract patches are new `semantic_rules` entries derived from the violations found.
5. **Merges signature counts into `status.json`** so the console triage bars are live.

### 2e. dbt — Silver and Gold layers

Three models, all using DuckDB:

- `stg_agent_events` (view) — reads Bronze via `delta_scan('../data/bronze/agent_events')`, deduplicates by the idempotency key using `ROW_NUMBER()`, extracts JSON fields from `tool_args`.
- `fct_agent_activity` (table) — adds cost tier bucketing (`FREE`, `LOW_COST`, `MEDIUM_COST`, `HIGH_COST`) and latency tier (`FAST`, `NOMINAL`, `SLOW`).
- `dim_tool_efficiency` (table) — aggregates per-tool: invocation count, success rate %, average cost, average latency.

26 data tests are defined — not_null, unique, accepted_values, custom assertions for `assert_cost_anomaly`, `assert_no_duplicates`, `assert_timestamp_freshness`.

The dbt variables (`max_cost_usd`, `max_event_age_hours`) are synced from `agent_contract.yaml` by `scripts/sync_contract_to_dbt_vars.py` — one source of truth for contract bounds shared by both PySpark and dbt.

---

## 3. Every Major Design Decision

### Decision 1: MERGE INTO for idempotency, not dedup-after-the-fact

**I chose:** Delta Lake `MERGE INTO` on `(agent_id, session_id, action_id)` with `whenNotMatchedInsertAll()`.

**Alternative:** Write all records with `append`, then run a periodic dedup query.

**Why:** MERGE prevents duplicates at write time — the Bronze table is always clean, even after a gatekeeper restart that replays Kafka offsets. Dedup-after-the-fact means your downstream queries see duplicates between write time and dedup time. In a streaming context where agents fire the same action_id repeatedly on retry, that window is wide enough to corrupt aggregations.

**Trade-off:** MERGE is more expensive than append — it reads the target table to find matching keys. For a single-partition local setup this is fine. At scale you'd partition Bronze by `(agent_id, date)` and restrict the MERGE scan to matching partitions using `WHEN MATCHED` predicates.

---

### Decision 2: Watermark on event_timestamp, not processing_time

**I chose:** `withWatermark("event_timestamp", "10 minutes")` — based on the timestamp inside the event payload.

**Alternative:** Use Kafka's offset/processing time, or no watermark at all.

**Why:** Event time is what matters for freshness validation. A record with `timestamp = now - 48h` should be caught by the freshness rule, and it is — because we watermark on event time. If we used processing time, a late event from 25 hours ago could sneak past the freshness check if the gatekeeper happened to process it quickly.

**What breaks without a watermark:** Spark accumulates state for `dropDuplicates` indefinitely. In a long-running job, the state store eventually exhausts memory. The watermark tells Spark when it's safe to evict old keys from the dedup state.

---

### Decision 3: Bronze/Quarantine as native Delta, Silver/Gold as DuckDB/dbt

**I chose:** PySpark writes Delta Lake for Bronze and Quarantine (ACID, MERGE support, time-travel), then dbt + DuckDB reads Delta for analytical models.

**Alternative:** Write everything from PySpark — Bronze, Silver, Gold all as Delta tables driven by Spark.

**Why:** The streaming layer (PySpark) is optimised for throughput and ACID correctness. The analytical layer (dbt + DuckDB) is optimised for developer iteration speed — `dbt run` takes 12 seconds locally, whereas a Spark job would take 60+ seconds and require a JVM. DuckDB's `delta_scan()` extension reads the Delta transaction log natively, so the handoff is clean — no format conversion, no ETL step.

The boundary is also a scope boundary: PySpark does ingestion and validation; dbt does business logic and test coverage. Each tool does what it's good at.

---

### Decision 4: `cost_usd` read as StringType in EVENT_SCHEMA

**I chose:** Read `cost_usd` from Kafka JSON as `StringType`, then attempt `cast(cost_usd as DoubleType)` explicitly.

**Alternative:** Read it directly as `DoubleType` in the schema.

**Why:** If `cost_usd = "UNMETERED"` arrives and the schema specifies `DoubleType`, Spark silently casts it to `null`. The record looks like a missing field, not a type mismatch. By reading as String and casting explicitly, the `null` result of the cast is caught by `F.when(F.col("cost_usd_double").isNull(), "type_mismatch:cost_usd_not_double")` and the record goes to Quarantine with the correct error signature.

---

### Decision 5: Deterministic triage alongside LLM triage

**I chose:** A `_deterministic_triage_engine()` that runs first and always, with LLM as an optional enrichment.

**Alternative:** LLM-only triage.

**Why:** LLM APIs have latency (1–5s), cost money, and can fail. The deterministic engine runs in-process in milliseconds and has no external dependencies. It catches the known patterns (cost out of bounds, staleness, schema drift) reliably. The LLM adds nuance — root cause reasoning, remediation advice in natural language — but is not on the critical path. If no API key is configured, `_call_llm()` returns `None` and the deterministic output is returned unchanged.

This also makes the triage engine fully testable without mocking an external API — `test_triage_deterministic_fallback` in the test suite verifies exactly this.

---

### Decision 6: Quarantine instead of dropping bad records

**I chose:** Write violating records to a separate `data/quarantine/agent_events` Delta table with `quarantined_at` and `error_summary` columns appended.

**Alternative:** Log the error and drop the record.

**Why:** Dropping is irreversible. You lose the ability to replay, re-validate after a contract fix, or diagnose incident patterns. The Quarantine table is the full payload — if the contract changes (e.g. raising the cost ceiling from $50 to $200), you can re-run validation against Quarantine and recover records that would now be valid. Triage is also impossible without the data — you can't cluster error signatures you've thrown away.

---

### Decision 7: Contract thresholds in YAML, single source of truth

**I chose:** `configs/agent_contract.yaml` as the single source for all validation thresholds. `sync_contract_to_dbt_vars.py` reads it and writes dbt variable overrides. `gatekeeper.py` reads it at startup with `_load_contract_thresholds()`.

**Alternative:** Hardcode thresholds in the gatekeeper and separately in dbt schema tests.

**Why:** With separate definitions, you can have a gatekeeper that allows `cost_usd <= 50` and a dbt test that asserts `cost_usd <= 40`. They drift. When you change the contract, you'd have to remember to update both places. The YAML file is the contract — the gatekeeper and dbt both derive from it. The `agent_contract_proposed.yaml` is a diff against it, written by triage.

---

## 4. What's Real vs. What's Scoped Out

### Actually implemented and load-bearing

- **Contract validation in PySpark** — all five validation checks run on every batch. This is in production code, not pseudocode.
- **MERGE INTO Bronze** — tested against real Delta Lake tables, confirmed idempotent across batch replays.
- **Quarantine routing** — 5 distinct error signatures confirmed in a real run: `type_mismatch:cost_usd_not_double` (73), `semantic_rule:cost_out_of_bounds` (72), `missing_required_field:agent_id` (67), `freshness:future_timestamp` (44), `freshness:stale_timestamp` (27).
- **Deterministic triage** — runs fully offline, tested by `test_triage_deterministic_fallback`.
- **dbt Silver/Gold models** — 3 models, 26 tests, confirmed passing locally.
- **35 pytest tests passing**, 2 skipped (the two that require a live Kafka broker and live LLM API).
- **Watermarking and deduplication** — implemented. Chaos tests in `test_chaos_infra.py` verify replay behaviour (`test_checkpoint_wipe_forces_full_replay_without_duplicates`).
- **MCP server** — `src/delta_guard/mcp_server.py` exposes 7 tools: `check_contract`, `get_active_contract`, `get_quarantine_summary`, `inspect_bronze_lakehouse`, `propose_contract_patch`, `require_authorization`, `run_health_check`. Tested by `test_mcp_server.py`.
- **Live console** — `console.html` polls `status.json` every 1.5s via FastAPI server on port 8888. Fully wired.

### Stated limitations — not softened

- **Single-broker Kafka** — no replication, no partition scaling. The throughput figure of "5 ev/s" is the producer rate, not a gatekeeper throughput benchmark. No benchmark of gatekeeper end-to-end latency under load exists.
- **No live Kafka-to-Delta benchmark.** The README previously had a hand-edited "Verified ✅" badge based on a single manual run. That badge has been removed. The CI badge now reads the actual GitHub Actions workflow status.
- **LLM triage is opt-in** — if `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` is not set, the LLM path is skipped silently. The `test_llm_triage_with_openai` test is skipped in CI for the same reason.
- **Watermark is configured but not stress-tested for OOM.** The 10-minute window is reasonable for a local demo; it has not been validated at volume.
- **Delta MERGE on Windows has a known fragility** — the JVM's `BlockManagerMasterEndpoint` throws a `NullPointerException` race condition during Spark startup on some Windows JVM versions. It is non-fatal and self-heals, but it causes the MERGE to fail mid-batch occasionally. The fix is running the gatekeeper inside Docker (Linux JVM) where this does not occur.
- **dbt reads Bronze at rest** — dbt and the gatekeeper do not run concurrently in a single command. dbt reads a snapshot of Bronze; it does not subscribe to a live stream.
- **No schema registry** — schema is hardcoded in `EVENT_SCHEMA` in `gatekeeper.py`. Schema evolution requires a code change and redeploy, not a registry update.

---

## 5. Likely Interview Questions

**Q: Why not just use a schema registry (Confluent, AWS Glue)?**

A schema registry enforces that producers serialize to a pre-registered Avro/Protobuf schema before the message even hits Kafka. That's a producer-side contract. This project enforces a consumer-side contract — it validates what the gatekeeper receives, including semantic rules (cost bounds, freshness) that a schema registry cannot express. The two are complementary, not alternatives. A registry would catch `"UNMETERED"` as a type error at publish time; this gatekeeper catches it at ingest time and routes it to Quarantine with a labelled error code. In a real deployment you'd want both layers.

---

**Q: How does this handle out-of-order events?**

The watermark on `event_timestamp` is the mechanism. `withWatermark("event_timestamp", "10 minutes")` tells Spark: the maximum amount of time an event can arrive late is 10 minutes. Events arriving later than that are dropped by the dedup state cleanup — they won't produce duplicates in Bronze even if their action_id was already processed. Events within the watermark window are deduplicated normally. The 24-hour freshness rule in the contract is a separate check — an event arriving "on time" to the gatekeeper but with a payload timestamp from yesterday still fails the freshness check and goes to Quarantine.

---

**Q: What happens if the gatekeeper crashes mid-batch?**

The streaming query uses `checkpointLocation = "checkpoints/gatekeeper"`. Spark's checkpoint mechanism stores the Kafka offset of the last successfully committed batch. On restart, the gatekeeper re-reads from that offset — replaying any events from the failed batch. Delta's MERGE INTO is idempotent on `(agent_id, session_id, action_id)`, so replayed events that were already written to Bronze before the crash produce no duplicates. The `dropDuplicates()` watermark catches them at the Spark layer before they even reach the MERGE. The chaos test `test_checkpoint_wipe_forces_full_replay_without_duplicates` verifies this behaviour explicitly.

---

**Q: How would this scale past one broker?**

Three things need to change. First, Kafka: increase partition count on `agent-events` and set `replication.factor > 1`. The gatekeeper already reads with `subscribe` (not `assign`), so it will automatically distribute partition reads across Spark workers when you increase parallelism. Second, Bronze: partition the table by `(date, agent_id[:2])` and restrict the MERGE scan to the matching partition using a generated column predicate — this avoids full-table scans on MERGE at scale. Third, the Spark cluster: currently `local[*]` — in production this becomes a YARN or k8s cluster with `spark.executor.instances` set appropriately. The gatekeeper code itself doesn't need to change; the Spark config and the Delta table layout are what scale.

---

**Q: Why quarantine instead of just dropping bad records?**

Dropping is irreversible. You lose three things: (1) the ability to replay records after a contract change — if the cost ceiling changes from $50 to $200, records that were quarantined under the old rule can be re-validated and recovered; (2) the ability to do triage — you cannot cluster error signatures or propose patches against data you've thrown away; (3) the audit trail — in a regulated environment, you need to prove that every record either made it to Bronze or was explicitly rejected with a documented reason. Quarantine preserves all three.

---

**Q: What's the MTTR if the gatekeeper produces wrong results (e.g. a bug in the validation logic)?**

Delta Lake time-travel. Bronze and Quarantine are both Delta tables with `_delta_log` transaction history. You can roll back Bronze to any prior version with `RESTORE TABLE bronze TO VERSION AS OF N`. Misrouted records in Quarantine can be replayed through a corrected gatekeeper by re-reading the raw Kafka offsets (if within the Kafka retention window) or by reading the Quarantine table directly and re-validating. The checkpoint can be wiped to force a full replay from Kafka offset 0.

---

**Q: How is the contract versioned and who can change it?**

Currently `configs/agent_contract.yaml` is in Git — changes go through a PR. The triage engine writes `configs/agent_contract_proposed.yaml` as a diff, not as a replacement; a human reviews and promotes it. The MCP server's `propose_contract_patch` tool requires an `authorized` flag to be true — in `test_mcp_propose_contract_patch_requires_authorization`, passing `authorized=false` is verified to raise an error. In production you'd add a human-approval step before the proposed contract becomes the active one.

---

**Q: Why PySpark and not Flink or Kafka Streams?**

PySpark Structured Streaming was chosen because: (1) Delta Lake is a Spark-native format — `DeltaTable.merge()` is a first-class Spark API; (2) the target audience for this portfolio project is data engineering roles where PySpark is the dominant tool; (3) the Python ecosystem (Faker, kafka-python, deltalake, duckdb) integrates naturally. Flink has lower latency but Delta Lake's native Flink connector is less mature, and the Python Flink API is more complex to test locally. Kafka Streams is JVM-only with no Python path, which would break the dbt/DuckDB integration.

---

**Q: The `errors` array is built from a `F.filter(F.array(...))` expression. Why not a UDF?**

UDFs execute in Python, which requires serializing each row from the JVM to the Python interpreter and back. For a streaming job processing thousands of rows per batch, this adds significant overhead. The `F.filter(F.array(...))` approach uses Spark's native Catalyst expressions — they run entirely within the JVM on the worker, with no Python serialization cost. This is the correct pattern for row-level validation in PySpark.

---

**Q: What does `sync_contract_to_dbt_vars.py` actually do?**

It reads `configs/agent_contract.yaml`, extracts the numeric bounds from the `semantic_rules` section (via regex, same logic as `_load_contract_thresholds()` in the gatekeeper), and writes them to `dbt_delta_guard/dbt_project.yml` as `vars`. This means `{{ var("max_cost_usd") }}` in a dbt test refers to the same value as `max_cost` in the gatekeeper's `process_batch()`. If you change the ceiling in the YAML, running `sync_contract_to_dbt_vars.py` propagates it to both layers automatically.

---

**Q: Why is there a second deduplication in dbt (`stg_agent_events` uses `ROW_NUMBER()`) if MERGE INTO already handles idempotency?**

MERGE INTO handles cross-batch idempotency — if the same `action_id` appears in two separate Spark batches, the second is rejected by the MERGE. But within a single batch, `dropDuplicates()` handles intra-batch duplicates. The `ROW_NUMBER()` in `stg_agent_events` is a dbt-layer safety net for the analytical model — it ensures that if any duplicates somehow survived (e.g. a Bronze table written by a different tool, or a schema evolution edge case), the staging view is still clean. It's defensive programming at the layer boundary, not a primary dedup mechanism.

---

## 6. Glossary

**Agent contract** — A YAML file (`configs/agent_contract.yaml`) that declares the expected schema (field names, types, nullability), allowed values, semantic rules (cost bounds, freshness window), and idempotency key for agent event writes. It is the single source of truth for all validation logic.

**Bronze layer** — The first Delta Lake table written by the gatekeeper. Contains only records that passed all contract checks. Named after the medallion architecture convention (Bronze → Silver → Gold). Immutable append/merge — records are never updated.

**Quarantine layer** — A parallel Delta table that receives every record that failed at least one contract check. Contains the original payload plus `quarantined_at` and `error_summary` columns. Append-only, never modified after write.

**Silver / Gold layers** — dbt models built on top of Bronze by DuckDB. Silver (`fct_agent_activity`) adds derived columns (cost tier, latency tier). Gold (`dim_tool_efficiency`) is an aggregation table. Neither uses Spark — they are DuckDB SQL queries.

**Watermark** — A Spark Structured Streaming mechanism that bounds how late an out-of-order event can arrive and still be processed. Set to 10 minutes on `event_timestamp`. Events arriving more than 10 minutes late are dropped; state for deduplication is evicted for events older than the watermark.

**Idempotent merge** — A write operation that produces the same result regardless of how many times it is applied. Here: `MERGE INTO Bronze ON (agent_id, session_id, action_id) WHEN NOT MATCHED INSERT`. Replaying the same event 100 times produces exactly one row in Bronze.

**Idempotency key** — The composite key `(agent_id, session_id, action_id)` declared in `agent_contract.yaml` under `idempotency.key_fields`. This uniquely identifies one agent action within one session — no two legitimate actions from the same agent in the same session should share all three values.

**Error signature** — A string produced by the gatekeeper that identifies the type of violation, e.g. `"type_mismatch:cost_usd_not_double"` or `"freshness:stale_timestamp"`. Stored in the `error_summary` column of the Quarantine table. Used by triage to cluster records by failure mode.

**Contract patch** — A new or updated `semantic_rules` entry in `configs/agent_contract_proposed.yaml` proposed by the triage engine. It represents a suggested change to the contract to either tighten validation (if the current rule is too loose) or to document an exception. Requires human review before becoming the active contract.

**Deterministic triage** — The branch of `LLMTriageEngine` that analyses Quarantine records using rule-based Python logic (checking cost ranges, timestamp freshness, schema drift) without calling an external LLM. Always runs, always produces a result. LLM triage is layered on top if an API key is available.

**MTTR (Mean Time to Recovery)** — How quickly the system can recover from a bad state. In this project, Delta time-travel (`RESTORE TABLE ... TO VERSION AS OF`) and Kafka offset replay are the primary MTTR mechanisms. Neither is automated — both require a human to identify the bad version and execute the rollback.

**delta_scan()** — A DuckDB SQL function from the DuckDB `delta` extension. Reads a Delta Lake table by parsing its `_delta_log` transaction log, then loading all committed Parquet files. Used in `stg_agent_events.sql` to read Bronze without a Spark session.

**Micro-batch trigger** — The `processingTime="10 seconds"` trigger in the gatekeeper's `writeStream`. Spark accumulates Kafka messages for 10 seconds, then processes the batch atomically. Lower values increase freshness but increase overhead per batch.

**MCP server** — Model Context Protocol server (`src/delta_guard/mcp_server.py`). Exposes the pipeline's validation and inspection capabilities as tools that an LLM agent framework can call before writing events — a pre-flight check mechanism. Separate from the gatekeeper; the gatekeeper is the enforcement layer, the MCP server is the advisory layer.

**`status.json`** — A small JSON file written atomically by the gatekeeper after every batch and updated by triage after every run. Contains `bronze_count`, `quarantine_count`, `throughput_eps`, `per_signature_counts`, `patch_count`, `kafka_online`, and `updated_at`. Polled by the browser console every 1.5 seconds.

**Shallow clone** — A Delta Lake operation that creates a new table pointing to the same underlying Parquet files as the source, without copying the data. Used in `src/delta_guard/sandbox_guard.py` and tested in `test_delta_clone_utils.py` for safe experimentation without duplicating storage.

**Kafka KRaft mode** — Kafka operating without ZooKeeper, using its own Raft-based consensus protocol for metadata management. Enabled in `docker-compose.yml` via `KAFKA_PROCESS_ROLES: "broker,controller"`. Simpler to run locally — no separate ZooKeeper container required.
