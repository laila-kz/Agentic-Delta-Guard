# Streaming Pipeline & Storage Benchmark Report

- **Status:** VERIFIED & PASSING
- **Generated:** 2026-09-07T11:12:51.588631+00:00
- **Throughput:** ~42,648.11 events/sec (single-core validation, no Kafka)
- **P95 Latency:** 0.0066 ms
- **Quarantine Rate:** 13.7% routed correctly

## 1. End-to-End Throughput & Latency

| Metric | Measured Value | Note |
| --- | ---: | --- |
| Total Events Evaluated | 1,000 | per benchmark run |
| Validation Throughput | **42,648.11 events/sec** | single-core Python, no Kafka overhead |
| P50 Latency | 0.0034 ms | per-event validation |
| P95 Latency | 0.0066 ms | per-event validation |
| P99 Latency | 0.0219 ms | per-event validation |
| Injected Poison Ratio | 13.7% | ~15% expected |

> **Note:** These numbers measure the validation logic only (pure Python, single-core). Real end-to-end throughput through Kafka -> PySpark -> Delta will be lower due to serialization, Spark scheduling, and disk I/O. Run the full streaming pipeline and measure from Kafka publish to Bronze commit for production-representative numbers.

## 2. Lakehouse Storage Layer Footprint

- **Total Files:** 55
- **Total Storage Size:** 897.69 KB (919,236 bytes)
- **Measurement Duration:** 0.025720 seconds

| Layer | Files | Size (Bytes) | Format |
| --- | ---: | ---: | --- |
| `bronze` | 7 | 806,224 | Delta Lake (ACID) |
| `bronze_parquet` | 4 | 1,053 | Parquet / DuckDB |
| `gold` | 38 | 102,622 | Delta Lake (ACID) |
| `quarantine` | 6 | 9,337 | Delta Lake (ACID) |

## 3. Benchmark Methodology & Defensibility

- **Workload:** Synthetic AI agent tool execution event payloads generated with realistic random latency, cost distributions, and ~15% poison edge cases.
- **Validation Scope:** In-process Python mirror of the PySpark gatekeeper's contract checks (required fields, cost bounds, timestamp freshness).
- **Storage Validation:** Delta transaction log presence verified; non-destructive micro-batch commits with zero phantom row reads.
- **Reproducibility:** Run `python src/delta_guard/benchmark.py` to regenerate these metrics on any target hardware.
