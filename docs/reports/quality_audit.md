# Week 2 Quality Audit

- **Status:** PASSED
- **Return code:** 0
- **Started:** 2026-09-06T01:21:09.834647+00:00
- **Finished:** 2026-09-06T01:21:29.826442+00:00
- **Duration:** 19.992 seconds

## Test Summary

No dbt test summary was found in the command output.

## dbt Output

```text
01:21:15  Running with dbt=1.12.3
01:21:17  Registered adapter: duckdb=1.11.0
01:21:21  [WARNING]: Configuration paths exist in your dbt_project.yml file which do not apply to any resources.
There are 1 unused configuration paths:
- models.dbt_delta_guard.intermediate
01:21:22  Found 3 models, 25 data tests, 616 macros
01:21:22  
01:21:22  Concurrency: 4 threads (target='dev')
01:21:22  
01:21:23  1 of 25 START test accepted_values_fct_agent_activity_cost_tier__FREE__LOW_COST_0_01___MEDIUM_COST_0_01_0_10___HIGH_COST_0_10_  [RUN]
01:21:23  2 of 25 START test accepted_values_fct_agent_activity_latency_tier__FAST_100ms___NOMINAL_100_500ms___SLOW_500ms_  [RUN]
01:21:23  3 of 25 START test accepted_values_stg_agent_events_tool_name__sql_query_executor__vector_search__web_scraper__db_writer  [RUN]
01:21:23  4 of 25 START test assert_cost_anomaly ......................................... [RUN]
01:21:25  4 of 25 PASS assert_cost_anomaly ............................................... [PASS in 1.43s]
01:21:25  1 of 25 PASS accepted_values_fct_agent_activity_cost_tier__FREE__LOW_COST_0_01___MEDIUM_COST_0_01_0_10___HIGH_COST_0_10_  [PASS in 1.51s]
01:21:25  5 of 25 START test assert_no_duplicates ........................................ [RUN]
01:21:25  2 of 25 PASS accepted_values_fct_agent_activity_latency_tier__FAST_100ms___NOMINAL_100_500ms___SLOW_500ms_  [PASS in 1.53s]
01:21:25  3 of 25 PASS accepted_values_stg_agent_events_tool_name__sql_query_executor__vector_search__web_scraper__db_writer  [PASS in 1.53s]
01:21:25  6 of 25 START test assert_non_negative_costs ................................... [RUN]
01:21:25  7 of 25 START test assert_timestamp_freshness .................................. [RUN]
01:21:25  8 of 25 START test dbt_utils_accepted_range_dim_tool_efficiency_success_rate_pct__100__0  [RUN]
01:21:25  5 of 25 PASS assert_no_duplicates .............................................. [PASS in 0.39s]
01:21:25  6 of 25 PASS assert_non_negative_costs ......................................... [PASS in 0.39s]
01:21:25  9 of 25 START test dbt_utils_accepted_range_stg_agent_events_cost_usd__50_0__0_0  [RUN]
01:21:25  8 of 25 PASS dbt_utils_accepted_range_dim_tool_efficiency_success_rate_pct__100__0  [PASS in 0.38s]
01:21:25  7 of 25 PASS assert_timestamp_freshness ........................................ [PASS in 0.40s]
01:21:25  10 of 25 START test not_null_dim_tool_efficiency_success_rate_pct .............. [RUN]
01:21:25  11 of 25 START test not_null_dim_tool_efficiency_tool_name ..................... [RUN]
01:21:25  12 of 25 START test not_null_dim_tool_efficiency_total_invocations ............. [RUN]
01:21:26  9 of 25 PASS dbt_utils_accepted_range_stg_agent_events_cost_usd__50_0__0_0 ..... [PASS in 0.48s]
01:21:26  13 of 25 START test not_null_fct_agent_activity_action_id ...................... [RUN]
01:21:26  10 of 25 PASS not_null_dim_tool_efficiency_success_rate_pct .................... [PASS in 0.49s]
01:21:26  11 of 25 PASS not_null_dim_tool_efficiency_tool_name ........................... [PASS in 0.48s]
01:21:26  12 of 25 PASS not_null_dim_tool_efficiency_total_invocations ................... [PASS in 0.49s]
01:21:26  14 of 25 START test not_null_fct_agent_activity_agent_id ....................... [RUN]
01:21:26  15 of 25 START test not_null_stg_agent_events_action_id ........................ [RUN]
01:21:26  16 of 25 START test not_null_stg_agent_events_agent_id ......................... [RUN]
01:21:26  13 of 25 PASS not_null_fct_agent_activity_action_id ............................ [PASS in 0.39s]
01:21:26  17 of 25 START test not_null_stg_agent_events_cost_usd ......................... [RUN]
01:21:26  14 of 25 PASS not_null_fct_agent_activity_agent_id ............................. [PASS in 0.35s]
01:21:26  15 of 25 PASS not_null_stg_agent_events_action_id .............................. [PASS in 0.35s]
01:21:26  16 of 25 PASS not_null_stg_agent_events_agent_id ............................... [PASS in 0.35s]
01:21:26  18 of 25 START test not_null_stg_agent_events_execution_time_ms ................ [RUN]
01:21:26  19 of 25 START test not_null_stg_agent_events_session_id ....................... [RUN]
01:21:26  20 of 25 START test not_null_stg_agent_events_status ........................... [RUN]
01:21:26  17 of 25 PASS not_null_stg_agent_events_cost_usd ............................... [PASS in 0.31s]
01:21:27  21 of 25 START test not_null_stg_agent_events_timestamp ........................ [RUN]
01:21:27  18 of 25 PASS not_null_stg_agent_events_execution_time_ms ...................... [PASS in 0.31s]
01:21:27  19 of 25 PASS not_null_stg_agent_events_session_id ............................. [PASS in 0.29s]
01:21:27  20 of 25 PASS not_null_stg_agent_events_status ................................. [PASS in 0.33s]
01:21:27  22 of 25 START test not_null_stg_agent_events_tool_name ........................ [RUN]
01:21:27  23 of 25 START test unique_dim_tool_efficiency_tool_name ....................... [RUN]
01:21:27  24 of 25 START test unique_fct_agent_activity_action_id ........................ [RUN]
01:21:27  21 of 25 PASS not_null_stg_agent_events_timestamp .............................. [PASS in 0.49s]
01:21:27  25 of 25 START test unique_stg_agent_events_action_id .......................... [RUN]
01:21:27  22 of 25 PASS not_null_stg_agent_events_tool_name .............................. [PASS in 0.53s]
01:21:27  23 of 25 PASS unique_dim_tool_efficiency_tool_name ............................. [PASS in 0.53s]
01:21:27  24 of 25 PASS unique_fct_agent_activity_action_id .............................. [PASS in 0.58s]
01:21:27  25 of 25 PASS unique_stg_agent_events_action_id ................................ [PASS in 0.20s]
01:21:27  
01:21:27  Finished running 25 data tests in 0 hours 0 minutes and 5.13 seconds (5.13s).
01:21:28  
01:21:28  Completed successfully
01:21:28  
01:21:28  Done. PASS=25 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=25

```
