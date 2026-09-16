# Project Status

**Version:** 2026-09-09 portfolio-readiness pass

This project is a rigorously tested local prototype for contract validation and quarantine of AI-agent event data. It is not a deployed production service, and the end-to-end Kafka-to-Delta demo is currently blocked by unstable local Kafka startup and Spark runtime constraints.

## Evidence Matrix

| Claim or capability | Current evidence | Status | Boundary |
| --- | --- | --- | --- |
| Contract validation rejects malformed and unsafe events | `tests/test_mcp_server.py`; 4 poison cases manually exercised | Demonstrated | MCP pre-flight path, not yet proof of live streaming routing |
| Bronze and Quarantine are Delta tables | Existing Delta transaction logs; DuckDB inspection | Demonstrated locally | Current live gatekeeper run did not add new rows |
| Kafka receives producer events | Broker offset advanced during producer runs | Demonstrated locally | Kafka KRaft mode active (ZooKeeper eliminated, zero startup collisions) |
| Spark consumes Kafka offsets | Gatekeeper checkpoint advanced | Demonstrated locally | Output writes were not confirmed in the latest run |
| Valid/invalid live routing | `process_batch` implementation and synthetic tests | Partial | Needs a stable Docker broker and a bounded end-to-end proof |
| Replay idempotency | `tests/test_chaos_infra.py` | Demonstrated by test | Synthetic Delta fixtures, not a completed live replay |
| Late-event handling | Watermark boundary tests | Demonstrated by test | Synthetic fixtures |
| dbt models and quality checks | `dbt run`: 3 models; `dbt test`: 25 passed | Demonstrated locally | DuckDB analytical layer |
| Sandbox isolation | `tests/test_contract_validation.py`; clone tests | Demonstrated locally | Windows uses an independent Delta-copy fallback; native zero-copy requires supported Spark runtime |
| Deterministic triage | Captured sample run and current quarantine data | Demonstrated locally | No live provider response is claimed |
| Live LLM triage | Manual test is opt-in and skipped without credentials | Not demonstrated | Keep out of automated pass counts and resume claims |
| MCP tool registration | Direct client harness and MCP tests | Demonstrated locally | No captured external agent transcript yet |
| MCP proposal authorization | `MCP_PROPOSAL_TOKEN` tests | Demonstrated locally | Local token gate, not production authentication |
| End-to-end throughput | No Kafka-to-Delta measurement | Not demonstrated | Existing benchmark measures local validation logic only |
| Deployment | No deployment target | Not started | This is a local project |

## Current Test Evidence

- Python tests: **35 passed, 2 skipped, 1 warning** in the latest full run.
- dbt models: **3 built successfully**.
- dbt data tests: **25 passed**.
- Skipped coverage includes Docker-gated broker failure testing, Windows Spark worker limitations, and opt-in live LLM testing.

## Next Gates

1. Stabilize Compose Kafka startup without recurring ZooKeeper broker-registration collisions.
2. Run a finite mixed batch through Docker and record Bronze/Quarantine counts before and after.
3. Capture an MCP client transcript with accepted, rejected, and authorized proposal calls.
4. Update resume claims only from this evidence matrix.
