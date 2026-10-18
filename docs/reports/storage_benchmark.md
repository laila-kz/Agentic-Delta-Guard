# Streaming Pipeline & Storage Benchmark Report

- **Status:** VERIFIED & PASSING
- **Generated:** 2026-10-18T12:38:34.772595+00:00
- **Throughput:** ~25,754.93 events/sec (median of 5 runs of 100,000 events, single-core Python)
- **P95 Latency:** 0.0154 ms
- **Quarantine Rate:** 15.0% routed correctly

## 1. End-to-End Throughput & Latency

| Metric | Measured Value | Note |
| --- | ---: | --- |
| Total Events Evaluated | 100,000 | per benchmark run (5 runs + 1 warmup) |
| Validation Throughput | **25,754.93 events/sec** | median across 5 runs, single-core Python |
| P50 Latency | 0.0047 ms | per-event validation (median) |
| P95 Latency | 0.0154 ms | per-event validation (median) |
| P99 Latency | 0.0541 ms | per-event validation (median) |
| Injected Poison Ratio | 15.0% | ~15% expected |

> **Note:** These numbers measure the validation logic only (pure Python, single-core). Real end-to-end throughput through Kafka -> PySpark -> Delta will be lower due to serialization, Spark scheduling, and disk I/O. Run the full streaming pipeline and measure from Kafka publish to Bronze commit for production-representative numbers.

## 2. Lakehouse Storage Layer Footprint

- **Total Files:** 9529
- **Total Storage Size:** 62681.39 KB (64,185,745 bytes)
- **Measurement Duration:** 2.124833 seconds

| Layer | Files | Size (Bytes) | Format |
| --- | ---: | ---: | --- |
| `bronze` | 6828 | 46,305,066 | Delta Lake (ACID) |
| `gold` | 24 | 66,355 | Delta Lake (ACID) |
| `quarantine` | 2677 | 17,814,324 | Delta Lake (ACID) |

## 3. Benchmark Methodology & Defensibility

- **Workload:** Synthetic AI agent tool execution event payloads generated with realistic random latency, cost distributions, and ~15% poison edge cases.
- **Validation Scope:** In-process Python mirror of the PySpark gatekeeper's contract checks (required fields, cost bounds, timestamp freshness).
- **Storage Validation:** Delta transaction log presence verified; non-destructive micro-batch commits with zero phantom row reads.
- **Reproducibility:** Run `python src/delta_guard/benchmark.py` to regenerate these metrics on any target hardware.
