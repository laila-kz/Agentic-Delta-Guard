# Storage Benchmark

- **Status:** PASSED
- **Generated:** 2026-09-06T14:33:30.632777+00:00
- **Measurement duration:** 0.027649 seconds
- **Total files:** 49
- **Total bytes:** 1427584

## Data Layer Metrics

| Layer | Files | Bytes |
| --- | ---: | ---: |
| bronze | 7 | 1330512 |
| bronze_parquet | 4 | 1053 |
| gold | 32 | 86682 |
| quarantine | 6 | 9337 |

This CI benchmark records the current local storage footprint. It does not mutate source data or perform destructive compaction.
