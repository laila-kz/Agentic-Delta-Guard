# Agentic Delta Guard Architecture

## Purpose

Agentic Delta Guard is a streaming data-quality boundary for AI-agent tool events. It accepts events from Kafka, validates them against the data contract in a distributed Spark job, and writes valid and invalid records to separate Delta Lake tables.

The design favors:

- **Early validation:** malformed or unsafe events are isolated before they reach analytical models.
- **Replayability:** Kafka offsets and Delta transaction logs provide durable processing state.
- **Operational visibility:** the terminal HUD reads live broker and Delta-table health without mutating pipeline data.
- **Deterministic recovery:** quarantined events are grouped and diagnosed locally, with optional LLM enrichment when credentials are available.

## Recommended Architecture Views

These three diagrams are the primary views for presenting the system: the container topology, the contract-validation lifecycle, and the sandbox promotion loop. They use the names and behavior implemented in this repository.

### 1. Container Diagram

```mermaid
flowchart TD
	subgraph Streaming[Streaming Layer]
		KAFKA[(Apache Kafka\nTopic: agent-events)]
	end

	subgraph Processing[Processing Layer]
		SPARK[PySpark Structured Streaming\nDataFrame validation\nMicro-batch: 10 seconds]
	end

	subgraph Storage[Storage Layer - Delta Lake]
		BRONZE[(Bronze\nContract-passed events)]
		QUARANTINE[(Quarantine\nPoison records + error_summary)]
		GOLD[(Gold\nAgent analytics marts)]
		SANDBOX[(Sandbox\nIndependent experimental table)]
	end

	subgraph Governance[Governance Layer]
		DBT[dbt-core\nStaging views + marts + tests]
		TRIAGE[Triage engine\nError clustering + diagnosis]
		ASSERT[SandboxGuard assertions\nInvariant checks]
	end

	subgraph Observability[Observability Layer]
		HUD[Terminal HUD\n2-second telemetry refresh]
		REPORTS[Quality and incident reports\nMarkdown / JSON]
	end

	KAFKA --> SPARK
	SPARK -->|Pass contract| BRONZE
	SPARK -->|Fail contract| QUARANTINE
	BRONZE --> DBT
	DBT --> GOLD
	GOLD -->|Create isolated snapshot| SANDBOX
	SANDBOX --> ASSERT
	ASSERT -->|Promote only when assertions pass| GOLD
	QUARANTINE --> TRIAGE
	TRIAGE --> REPORTS
	BRONZE -.read-only telemetry.-> HUD
	QUARANTINE -.read-only telemetry.-> HUD
	KAFKA -.broker probe.-> HUD
```

**Reading this view:** Kafka and Spark form the ingestion boundary; Bronze and Quarantine are the durable routing destinations; dbt owns analytical modeling; triage and sandbox assertions provide governance; the HUD and reports expose operational state.

### 2. Contract Validation Data Flow

```mermaid
sequenceDiagram
	participant Agent as AI agent or tool
	participant Kafka as Kafka: agent-events
	participant Gatekeeper as PySpark gatekeeper
	participant Bronze as Bronze Delta
	participant Quarantine as Quarantine Delta

	Agent->>Kafka: Send tool execution event

	loop Structured Streaming micro-batch (10 seconds)
		Kafka->>Gatekeeper: Read event batch
		Gatekeeper->>Gatekeeper: Parse JSON with EVENT_SCHEMA
		Gatekeeper->>Gatekeeper: Cast cost_usd and timestamp
		Gatekeeper->>Gatekeeper: Check required fields, bounds, and freshness

		alt All contract rules pass
			Gatekeeper->>Bronze: MERGE by agent_id + session_id + action_id
		else Any rule fails
			Gatekeeper->>Quarantine: Append event + error_summary
		end
	end
```

The gatekeeper routes records with Spark DataFrame expressions and keeps processing distributed. Valid records are merged idempotently by the agent/session/action key; invalid records retain their validation signature for triage.

### 3. Sandbox Assertion and Promotion Flow

```mermaid
sequenceDiagram
	participant Guard as SandboxGuard
	participant Gold as Gold analytics Delta
	participant Sandbox as Sandbox Delta table
	participant Assert as Assertion suite
	participant Operator as Operator / pipeline

	Guard->>Gold: Read current Gold snapshot
	Guard->>Sandbox: Write independent experimental snapshot
	Operator->>Sandbox: Apply agent experiment or mutation
	Guard->>Assert: Run post-execution assertions
	Assert->>Assert: Check required columns and non-empty rows
	Assert->>Assert: Check unique tool names and non-negative counts
	Assert->>Assert: Check cost bounds and success-rate bounds

	alt All assertions pass
		Assert-->>Operator: Promotion-ready result
		Operator->>Gold: Explicitly promote approved result
	else Any assertion fails
		Assert-->>Operator: Reject and report violations
		Operator->>Sandbox: Discard or repair experiment
	end
```

> **Implementation note:** the current `SandboxGuard.create_shallow_clone()` name is historical. It writes an independent Delta snapshot and does not use a storage-level zero-copy Delta shallow clone. Promotion is conditional in the guard workflow; the assertion code reports readiness and the caller controls the production action.

## System Context

```mermaid
flowchart LR
	A[Agent tools and services] -->|JSON events| P[Event producer]
	P -->|agent-events| K[(Kafka broker)]
	K --> G[PySpark gatekeeper]
	G -->|valid events| B[(Bronze Delta table)]
	G -->|invalid events + error_summary| Q[(Quarantine Delta table)]
	B --> D[dbt staging and marts]
	Q --> T[Triage engine]
	T --> I[Incident log]
	T --> C[Proposed contract]
	B --> H[Terminal HUD]
	Q --> H
	K --> H
	H --> O[Operator]
```

The producer and HUD target the host endpoint `localhost:9092`. The gatekeeper defaults to the Compose-network endpoint `kafka:29092`; when it runs on the host, set `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`. Docker Compose supplies Kafka, ZooKeeper, and Kafka UI for local development.

## Runtime Topology

```mermaid
flowchart TB
	subgraph Host[Developer host]
		Producer[src/delta_guard/producer.py]
		Gatekeeper[src/delta_guard/gatekeeper.py\nPySpark Structured Streaming]
		HUD[src/delta_guard/hud.py\nTextual terminal UI]
		Triage[src/delta_guard/triage.py]
		DBT[dbt_delta_guard\ndbt project]
	end

	subgraph Docker[Docker Compose]
		ZK[ZooKeeper :2181]
		Kafka[Kafka :9092 / :29092]
		KafkaUI[Kafka UI :8080]
		ZK --> Kafka
		Kafka --> KafkaUI
	end

	subgraph Storage[Local storage]
		Checkpoints[checkpoints/gatekeeper]
		Bronze[data/bronze/agent_events]
		Quarantine[data/quarantine/agent_events]
		Warehouse[data/bronze/dbt_delta_guard.duckdb]
		Reports[docs/reports and docs/INCIDENT_LOG.md]
	end

	Producer -->|localhost:9092| Kafka
	Kafka -->|localhost:9092| Gatekeeper
	Gatekeeper --> Checkpoints
	Gatekeeper --> Bronze
	Gatekeeper --> Quarantine
	Bronze --> DBT
	DBT --> Warehouse
	Quarantine --> Triage
	Triage --> Reports
	HUD -.read-only probes.-> Kafka
	HUD -.read-only reads.-> Bronze
	HUD -.read-only reads.-> Quarantine
```

### Network endpoints

| Context | Kafka endpoint | Purpose |
| --- | --- | --- |
| Host process | `localhost:9092` | Producer, gatekeeper, and HUD access |
| Compose network | `kafka:29092` | Container-to-container access |
| Kafka UI | `http://localhost:8080` | Topic and broker inspection |

## Event Lifecycle

```mermaid
sequenceDiagram
	participant Producer
	participant Kafka
	participant Gatekeeper
	participant Bronze as Bronze Delta
	participant Quarantine as Quarantine Delta
	participant Triage

	Producer->>Kafka: Publish JSON to agent-events
	Kafka-->>Gatekeeper: Read event batch
	Gatekeeper->>Gatekeeper: Parse event schema
	Gatekeeper->>Gatekeeper: Cast cost and timestamp
	Gatekeeper->>Gatekeeper: Evaluate contract rules
	alt No validation errors
		Gatekeeper->>Bronze: Merge by agent/session/action key
	else One or more validation errors
		Gatekeeper->>Quarantine: Append event and error_summary
		Triage->>Quarantine: Read quarantined records
		Triage->>Triage: Cluster signatures and diagnose
	end
```

## Validation Boundary

The gatekeeper performs validation in `process_batch` using Spark DataFrame expressions. It does not collect the streaming batch into driver memory.

```mermaid
flowchart TD
	Raw[Kafka value] --> Parse[Parse JSON with EVENT_SCHEMA]
	Parse --> Cast[Cast cost_usd and timestamp]
	Cast --> Rules{Contract checks}
	Rules --> Required[Required identifiers present]
	Rules --> Types[cost_usd is numeric]
	Rules --> Bounds[0.0 <= cost_usd <= 50.0]
	Rules --> Fresh[Timestamp within rolling window]
	Required --> Errors[errors array]
	Types --> Errors
	Bounds --> Errors
	Fresh --> Errors
	Errors -->|size = 0| Valid[Valid DataFrame]
	Errors -->|size > 0| Invalid[Quarantine DataFrame]
	Valid --> Merge[Delta merge on agent_id + session_id + action_id]
	Invalid --> Append[Delta append with error_summary]
```

The current contract boundary catches:

- Missing `agent_id`, `session_id`, or `action_id`.
- Non-numeric `cost_usd` values.
- Costs outside the inclusive range `[0.0, 50.0]`.
- Invalid, stale, or future timestamps.

## Storage and Modeling

```mermaid
flowchart LR
	Bronze[(Bronze Delta)] --> STG[stg_agent_events\nview]
	STG --> INT[Intermediate models\nephemeral]
	INT --> FCT[fct_agent_activity\ntable]
	INT --> DIM[dim_tool_efficiency\ntable]
	FCT --> Tests[dbt data tests]
	DIM --> Tests
	Quarantine[(Quarantine Delta)] --> Triage[Deterministic triage]
	Triage --> Audit[Incident and quality reports]
```

The dbt project uses DuckDB for local analytical storage. Its configured materializations are:

| Layer | Materialization | Role |
| --- | --- | --- |
| Staging | View | Normalize and expose source events |
| Intermediate | Ephemeral | Reusable transformation logic |
| Marts | Table | Consumer-facing activity and efficiency models |

## Observability HUD

The terminal HUD is intentionally read-only. Every two seconds it probes the Kafka TCP endpoint and reads the current Delta tables using `deltalake`, then renders:

- Kafka broker status.
- Bronze row count and storage size.
- Quarantine row count and percentage of observed records.
- Total Bronze plus quarantine footprint.
- Host CPU and memory utilization.
- Incident state based on the quarantine population.

```mermaid
flowchart LR
	Timer[2-second refresh] --> Snapshot[collect_snapshot]
	Snapshot --> KafkaProbe[TCP probe localhost:9092]
	Snapshot --> DeltaRead[Read DeltaTable metadata and rows]
	Snapshot --> HostRead[Read psutil CPU and memory]
	KafkaProbe --> Render[Textual render]
	DeltaRead --> Render
	HostRead --> Render
	Render --> Operator[Operator terminal]
```

The HUD is an operational signal, not a replacement for the Delta transaction log, dbt tests, or incident report.

## Failure Handling

| Failure | Detection | Result | Operator action |
| --- | --- | --- | --- |
| Kafka unavailable | Producer/gatekeeper connection failure; HUD shows `OFFLINE` | No new events are processed | Start Compose services and inspect broker logs |
| Contract violation | Gatekeeper `errors` array | Event is appended to quarantine | Run triage and inspect `docs/INCIDENT_LOG.md` |
| Duplicate event | Bronze Delta merge key | Existing key is not inserted again | Review source identifiers if duplicates persist |
| dbt model/test failure | dbt command exit code | CI/local check fails | Inspect dbt logs and model test output |
| Triage provider unavailable | HTTP/API failure | Deterministic diagnosis is retained | Review the generated report without LLM enrichment |
| Missing Delta table | HUD shows zero rows/footprint | Telemetry remains available but incomplete | Check gatekeeper output and storage paths |

## Development and Verification Commands

```powershell
# Start Kafka, ZooKeeper, and Kafka UI
docker compose up -d

# Emit sample agent events
python src/delta_guard/producer.py

# Start the streaming gatekeeper
python src/delta_guard/gatekeeper.py

# Open the real-time terminal HUD
python src/delta_guard/hud.py

# Compile and test dbt models
cd dbt_delta_guard
dbt deps --profiles-dir .
dbt compile --profiles-dir .
dbt test --profiles-dir .

# Run the focused chaos suite
pytest tests/test_chaos_suite.py -v
```

The local CI helper also validates the contract, compiles dbt, runs the chaos suite, checks sandbox isolation, runs deterministic triage, and generates the storage benchmark report.

## Design Boundaries

- Kafka provides transport and replayable offsets; Delta Lake provides durable table state.
- The gatekeeper owns contract enforcement and routing, not analytical aggregation.
- dbt owns analytical transformations and assertions, not streaming ingestion.
- Triage owns quarantine diagnosis and proposed contract changes, not automatic production promotion.
- The HUD reports health and volume; it does not acknowledge, delete, or repair events.
