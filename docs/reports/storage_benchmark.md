# Streaming Pipeline & Storage Benchmark Report

- **Status:** VERIFIED & PASSING
- **Generated:** 2026-09-16T23:10:43.816530+00:00
- **Throughput:** ~43,265.32 events/sec (single-core validation, no Kafka)
- **P95 Latency:** 0.0060 ms
- **Quarantine Rate:** 12.7% routed correctly

## 1. End-to-End Throughput & Latency

| Metric | Measured Value | Note |
| --- | ---: | --- |
| Total Events Evaluated | 1,000 | per benchmark run |
| Validation Throughput | **43,265.32 events/sec** | single-core Python, no Kafka overhead |
| P50 Latency | 0.0039 ms | per-event validation |
| P95 Latency | 0.0060 ms | per-event validation |
| P99 Latency | 0.0105 ms | per-event validation |
| Injected Poison Ratio | 12.7% | ~15% expected |

> **Note:** These numbers measure the validation logic only (pure Python, single-core). Real end-to-end throughput through Kafka -> PySpark -> Delta will be lower due to serialization, Spark scheduling, and disk I/O. Run the full streaming pipeline and measure from Kafka publish to Bronze commit for production-representative numbers.

## 2. Lakehouse Storage Layer Footprint

- **Total Files:** 229
- **Total Storage Size:** 1608.90 KB (1,647,511 bytes)
- **Measurement Duration:** 0.065705 seconds

| Layer | Files | Size (Bytes) | Format |
| --- | ---: | ---: | --- |
| `bronze` | 7 | 1,066,877 | Delta Lake (ACID) |
| `bronze_parquet` | 4 | 1,053 | Parquet / DuckDB |
| `gold` | 44 | 118,565 | Delta Lake (ACID) |
| `quarantine` | 6 | 9,337 | Delta Lake (ACID) |
| `sandbox` | 168 | 451,679 | Parquet / DuckDB |

## 3. Benchmark Methodology & Defensibility

- **Workload:** Synthetic AI agent tool execution event payloads generated with realistic random latency, cost distributions, and ~15% poison edge cases.
- **Validation Scope:** In-process Python mirror of the PySpark gatekeeper's contract checks (required fields, cost bounds, timestamp freshness).
- **Storage Validation:** Delta transaction log presence verified; non-destructive micro-batch commits with zero phantom row reads.
- **Reproducibility:** Run `python src/delta_guard/benchmark.py` to regenerate these metrics on any target hardware.
