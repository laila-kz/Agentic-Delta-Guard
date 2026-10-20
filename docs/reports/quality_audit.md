# Week 2 Quality Audit

- **Status:** PASSED
- **Return code:** 0
- **Started:** 2026-10-20T01:34:04.145899+00:00
- **Finished:** 2026-10-20T01:35:15.222843+00:00
- **Duration:** 71.077 seconds

## Test Summary

No dbt test summary was found in the command output.

## dbt Output

```text
01:34:09  Running with dbt=1.8.10
01:34:09  This version of dbt is deprecated and no longer receives regular patches, including for known bugs. We recommend upgrading to a newer supported version of dbt: https://docs.getdbt.com/docs/dbt-versions
01:34:10  Registered adapter: duckdb=1.8.4
01:34:11  Unable to do partial parsing because of a version mismatch
01:34:14  Found 3 models, 26 data tests, 542 macros
01:34:14  
01:34:18  Concurrency: 4 threads (target='duckdb_dev')
01:34:18  
01:34:18  1 of 26 START test accepted_values_fct_agent_activity_cost_tier__FREE__LOW_COST_0_01___MEDIUM_COST_0_01_0_10___HIGH_COST_0_10_  [RUN]
01:34:18  2 of 26 START test accepted_values_fct_agent_activity_latency_tier__FAST_100ms___NOMINAL_100_500ms___SLOW_500ms_  [RUN]
01:34:18  3 of 26 START test accepted_values_stg_agent_events_tool_name__sql_query_executor__vector_search__web_scraper__db_writer  [RUN]
01:34:18  4 of 26 START test assert_cost_anomaly ......................................... [RUN]
01:34:18  2 of 26 PASS accepted_values_fct_agent_activity_latency_tier__FAST_100ms___NOMINAL_100_500ms___SLOW_500ms_  [PASS in 0.45s]
01:34:18  1 of 26 PASS accepted_values_fct_agent_activity_cost_tier__FREE__LOW_COST_0_01___MEDIUM_COST_0_01_0_10___HIGH_COST_0_10_  [PASS in 0.45s]
01:34:18  5 of 26 START test assert_no_duplicates ........................................ [RUN]
01:34:18  6 of 26 START test assert_non_negative_costs ................................... [RUN]
01:34:19  5 of 26 PASS assert_no_duplicates .............................................. [PASS in 0.12s]
01:34:19  7 of 26 START test assert_timestamp_freshness .................................. [RUN]
01:34:24  4 of 26 PASS assert_cost_anomaly ............................................... [PASS in 5.58s]
01:34:24  8 of 26 START test dbt_utils_accepted_range_dim_tool_efficiency_success_rate_pct__100__0  [RUN]
01:34:24  8 of 26 PASS dbt_utils_accepted_range_dim_tool_efficiency_success_rate_pct__100__0  [PASS in 0.13s]
01:34:24  9 of 26 START test dbt_utils_accepted_range_stg_agent_events_cost_usd___var_max_cost_usd___0_0  [RUN]
01:34:27  3 of 26 PASS accepted_values_stg_agent_events_tool_name__sql_query_executor__vector_search__web_scraper__db_writer  [PASS in 8.58s]
01:34:27  10 of 26 START test not_null_dim_tool_efficiency_success_rate_pct .............. [RUN]
01:34:27  10 of 26 PASS not_null_dim_tool_efficiency_success_rate_pct .................... [PASS in 0.57s]
01:34:27  11 of 26 START test not_null_dim_tool_efficiency_tool_name ..................... [RUN]
01:34:27  11 of 26 PASS not_null_dim_tool_efficiency_tool_name ........................... [PASS in 0.19s]
01:34:27  12 of 26 START test not_null_dim_tool_efficiency_total_invocations ............. [RUN]
01:34:28  12 of 26 PASS not_null_dim_tool_efficiency_total_invocations ................... [PASS in 0.32s]
01:34:28  13 of 26 START test not_null_fct_agent_activity_action_id ...................... [RUN]
01:34:28  13 of 26 PASS not_null_fct_agent_activity_action_id ............................ [PASS in 0.17s]
01:34:28  14 of 26 START test not_null_fct_agent_activity_agent_id ....................... [RUN]
01:34:28  14 of 26 PASS not_null_fct_agent_activity_agent_id ............................. [PASS in 0.13s]
01:34:28  15 of 26 START test not_null_stg_agent_events_action_id ........................ [RUN]
01:34:32  7 of 26 WARN 42570 assert_timestamp_freshness .................................. [WARN 42570 in 13.28s]
01:34:32  16 of 26 START test not_null_stg_agent_events_agent_id ......................... [RUN]
01:34:33  6 of 26 PASS assert_non_negative_costs ......................................... [PASS in 14.35s]
01:34:33  17 of 26 START test not_null_stg_agent_events_cost_usd ......................... [RUN]
01:34:33  9 of 26 PASS dbt_utils_accepted_range_stg_agent_events_cost_usd___var_max_cost_usd___0_0  [PASS in 9.13s]
01:34:33  18 of 26 START test not_null_stg_agent_events_execution_time_ms ................ [RUN]
01:34:36  16 of 26 PASS not_null_stg_agent_events_agent_id ............................... [PASS in 4.38s]
01:34:36  19 of 26 START test not_null_stg_agent_events_extracted_at ..................... [RUN]
01:34:37  15 of 26 PASS not_null_stg_agent_events_action_id .............................. [PASS in 8.50s]
01:34:37  20 of 26 START test not_null_stg_agent_events_session_id ....................... [RUN]
01:34:37  19 of 26 PASS not_null_stg_agent_events_extracted_at ........................... [PASS in 1.01s]
01:34:37  21 of 26 START test not_null_stg_agent_events_status ........................... [RUN]
01:34:47  18 of 26 PASS not_null_stg_agent_events_execution_time_ms ...................... [PASS in 13.79s]
01:34:47  22 of 26 START test not_null_stg_agent_events_timestamp ........................ [RUN]
01:34:49  17 of 26 PASS not_null_stg_agent_events_cost_usd ............................... [PASS in 15.82s]
01:34:49  23 of 26 START test not_null_stg_agent_events_tool_name ........................ [RUN]
01:34:54  20 of 26 PASS not_null_stg_agent_events_session_id ............................. [PASS in 17.25s]
01:34:54  24 of 26 START test unique_dim_tool_efficiency_tool_name ....................... [RUN]
01:34:54  24 of 26 PASS unique_dim_tool_efficiency_tool_name ............................. [PASS in 0.20s]
01:34:54  25 of 26 START test unique_fct_agent_activity_action_id ........................ [RUN]
01:34:54  25 of 26 PASS unique_fct_agent_activity_action_id .............................. [PASS in 0.33s]
01:34:55  26 of 26 START test unique_stg_agent_events_action_id .......................... [RUN]
01:34:59  22 of 26 PASS not_null_stg_agent_events_timestamp .............................. [PASS in 11.79s]
01:35:05  23 of 26 PASS not_null_stg_agent_events_tool_name .............................. [PASS in 16.29s]
01:35:07  26 of 26 PASS unique_stg_agent_events_action_id ................................ [PASS in 12.18s]
01:35:08  21 of 26 PASS not_null_stg_agent_events_status ................................. [PASS in 30.68s]
01:35:10  
01:35:10  Finished running 26 data tests in 0 hours 0 minutes and 55.78 seconds (55.78s).
01:35:10  
01:35:10  Completed with 1 warning:
01:35:10  
01:35:10  Warning in test assert_timestamp_freshness (tests\assert_timestamp_freshness.sql)
01:35:10  Got 42570 results, configured to warn if != 0
01:35:10  
01:35:10    compiled code at target\compiled\dbt_delta_guard\tests\assert_timestamp_freshness.sql
01:35:10  
01:35:10  Done. PASS=25 WARN=1 ERROR=0 SKIP=0 TOTAL=26

```
