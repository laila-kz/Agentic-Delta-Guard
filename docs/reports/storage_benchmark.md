# Storage Benchmark

- **Status:** PASSED
- **Generated:** 2026-09-06T13:57:06.367168+00:00
- **Measurement duration:** 0.030012 seconds
- **Total files:** 43
- **Total bytes:** 1411656

## Data Layer Metrics

| Layer | Files | Bytes |
| --- | ---: | ---: |
| bronze | 7 | 1330512 |
| bronze_parquet | 4 | 1053 |
| gold | 26 | 70754 |
| quarantine | 6 | 9337 |

This CI benchmark records the current local storage footprint. It does not mutate source data or perform destructive compaction.
