Step-by-Step Demo Recording Guide
This guide gives you a cinematic, structured script to record a 2 to 3 minute portfolio demo video.

🖥️ Recommended Screen Setup
Left Half of Screen: Split into 2 or 3 terminal panes (e.g., Windows Terminal or VS Code).
Right Half of Screen: Web Browser open to:
http://localhost:8080 (Kafka UI)
The generated benchmark/incident reports in your code editor.
📋 Demo Flow & Commands
mermaid
flowchart LR
    A[1. Infrastructure\nKafka KRaft] --> B[2. Streaming\nProducer + Gatekeeper + HUD]
    B --> C[3. Shift-Left\nMCP Pre-Flight Check]
    C --> D[4. Lakehouse & Quality\ndbt Run + Tests]
    D --> E[5. Automated Triage\nMTTR Benchmark]
Scene 1: Start Infrastructure (0:00 – 0:30)
Action: Show your terminal and spin up Kafka KRaft mode with Kafka UI.

powershell
# In Terminal 1:
docker compose up -d
Open browser to http://localhost:8080 (Kafka UI).
Show that Kafka KRaft mode started in < 3 seconds without ZooKeeper.
Scene 2: Live Streaming, Poison Injection & Quarantine HUD (0:30 – 1:15)
Action: Demonstrate the streaming gatekeeper separating valid records from poisoned records in real time.

Terminal 1 — Streaming Gatekeeper:
powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
python src/delta_guard/gatekeeper.py
Terminal 2 — Real-Time HUD (Dashboard):
powershell
python src/delta_guard/hud.py
Terminal 3 — Synthetic Producer (with 15% poison payloads):
powershell
python src/delta_guard/producer.py
What to highlight on camera:

Show the HUD updating live: valid events flowing to data/bronze and poisoned events (disallowed tools, negative cost, stale timestamps) routed into data/quarantine.
Explain: "The pipeline never crashed or halted on poison rows—valid rows were merged while invalid rows were safely quarantined."
Scene 3: Shift-Left MCP Contract Validation (1:15 – 1:50)
Action: Show that LLM agents can validate their payloads before sending them to Kafka.

powershell
# Run the live MCP interaction recording script:
python scripts/record_mcp_session.py
Open 

docs/reports/mcp_interaction_log.md
 in your editor.
Explain: "By implementing the Model Context Protocol (MCP), upstream autonomous agents query our schema contract and self-heal before producing bad data to the broker."
Scene 4: dbt Data Quality & Lakehouse Analytics (1:50 – 2:20)
Action: Build the Gold Layer marts and run all automated data quality checks.

powershell
cd dbt_delta_guard
dbt build --profiles-dir .
cd ..
What to highlight on camera:

All 3 models (stg_agent_events, fct_agent_activity, dim_tool_efficiency) created.
26/26 data tests passed with 0 errors and 0 warnings.
Scene 5: Single-Command End-to-End Test & Benchmarks (2:20 – 3:00)
Action: Run the automated master harness to show total test pass & engineering rigor.

powershell
python scripts/run_e2e_verification.py
What to highlight on camera:

Show the colored output passing all 5 stages in seconds:
Contract Sync ([OK])
Pytest Unit & Chaos Suite — 35 passed, 0 failures ([OK])
dbt Gold Models & Tests — 29/29 passed ([OK])
Quarantine Triage Benchmark — 100% MTTR Reduction ([OK])
Storage & Throughput Benchmark — 40,000+ events/sec ([OK])