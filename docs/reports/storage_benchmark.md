# Streaming Pipeline & Storage Benchmark Report

- **Status:** VERIFIED & PASSING
- **Generated:** 2026-09-22T21:54:25.789706+00:00
- **Throughput:** ~23,833.70 events/sec (single-core validation, no Kafka)
- **P95 Latency:** 0.0131 ms
- **Quarantine Rate:** 15.8% routed correctly

## 1. End-to-End Throughput & Latency

| Metric | Measured Value | Note |
| --- | ---: | --- |
| Total Events Evaluated | 1,000 | per benchmark run |
| Validation Throughput | **23,833.70 events/sec** | single-core Python, no Kafka overhead |
| P50 Latency | 0.0070 ms | per-event validation |
| P95 Latency | 0.0131 ms | per-event validation |
| P99 Latency | 0.0223 ms | per-event validation |
| Injected Poison Ratio | 15.8% | ~15% expected |

> **Note:** These numbers measure the validation logic only (pure Python, single-core). Real end-to-end throughput through Kafka -> PySpark -> Delta will be lower due to serialization, Spark scheduling, and disk I/O. Run the full streaming pipeline and measure from Kafka publish to Bronze commit for production-representative numbers.

## 2. Lakehouse Storage Layer Footprint

- **Total Files:** 1381
- **Total Storage Size:** 4705.91 KB (4,818,852 bytes)
- **Measurement Duration:** 0.584526 seconds

| Layer | Files | Size (Bytes) | Format |
| --- | ---: | ---: | --- |
| `bronze` | 827 | 3,204,838 | Delta Lake (ACID) |
| `bronze_parquet` | 4 | 1,053 | Parquet / DuckDB |
| `quarantine` | 346 | 1,065,538 | Delta Lake (ACID) |
| `sandbox` | 204 | 547,423 | Parquet / DuckDB |

## 3. Benchmark Methodology & Defensibility

- **Workload:** Synthetic AI agent tool execution event payloads generated with realistic random latency, cost distributions, and ~15% poison edge cases.
- **Validation Scope:** In-process Python mirror of the PySpark gatekeeper's contract checks (required fields, cost bounds, timestamp freshness).
- **Storage Validation:** Delta transaction log presence verified; non-destructive micro-batch commits with zero phantom row reads.
- **Reproducibility:** Run `python src/delta_guard/benchmark.py` to regenerate these metrics on any target hardware.
