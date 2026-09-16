# Delta Lake Time Travel & Shallow Cloning Guide

## Overview

This guide demonstrates two powerful Delta Lake 3.x features:

1. **Time Travel** - Query historical versions of your data
2. **Shallow Cloning** - Runtime-dependent sandboxes for testing and development

Native Delta operations provide ACID guarantees. On Windows, clone tests use an independent Delta-copy fallback because the local Spark filesystem runtime cannot provide the native clone path reliably.

---

## Time Travel

### What is Time Travel?

Time travel allows you to query any historical version of your Delta table by timestamp or version number. This is enabled by Delta Lake's immutable transaction log (`_delta_log`).

### Prerequisites

- Delta table with commit history
- Version number OR timestamp of the version you want to read
- Read access to the table

### Example 1: Read at Specific Version

```python
from src.delta_guard.delta_clone_utils import DeltaCloneUtils
import logging

logging.basicConfig(level=logging.INFO)
clone_utils = DeltaCloneUtils()

# Read agent_analytics at version 5
df_v5 = clone_utils.read_at_version(
    table_path="data/gold/agent_analytics",
    version=5,
)
print(f"Version 5: {len(df_v5)} rows")
print(df_v5.head())
```

### Example 2: Read at Specific Timestamp

```python
from datetime import datetime, timedelta
import pytz

# Read as of 2 hours ago
two_hours_ago = datetime.now(pytz.UTC) - timedelta(hours=2)

df_past = clone_utils.read_at_timestamp(
    table_path="data/gold/agent_analytics",
    timestamp=two_hours_ago.isoformat(),
)
print(f"Data from 2 hours ago: {len(df_past)} rows")
```

### Example 3: Compare Current vs Historical

```python
import pandas as pd

# Get current state
df_current = clone_utils.read_at_timestamp(
    table_path="data/gold/agent_analytics",
    timestamp=datetime.now().isoformat(),
)

# Get state from yesterday
yesterday = datetime.now() - timedelta(days=1)
df_yesterday = clone_utils.read_at_timestamp(
    table_path="data/gold/agent_analytics",
    timestamp=yesterday.isoformat(),
)

# Compare
rows_added = len(df_current) - len(df_yesterday)
print(f"Rows added since yesterday: {rows_added}")

# Find new tools added
current_tools = set(df_current['tool_name'].unique())
yesterday_tools = set(df_yesterday['tool_name'].unique())
new_tools = current_tools - yesterday_tools
print(f"New tools: {new_tools}")
```

### Example 4: Audit Trail - View All Changes

```python
# Get the complete commit history
history_df = clone_utils.get_table_history(
    table_path="data/gold/agent_analytics",
    limit=20,
)

print(history_df[['version', 'timestamp', 'operation', 'operationParameters']])

# Output:
# version  timestamp                    operation  operationParameters
# 4        2026-09-06T20:15:30.123Z    WRITE      {...}
# 3        2026-09-06T20:10:15.456Z    DELETE     {...}
# 2        2026-09-06T20:05:42.789Z    WRITE      {...}
# 1        2026-09-06T20:00:00.000Z    WRITE      {...}
```

### Example 5: Audit Data Quality Over Time

```python
from datetime import datetime, timedelta

# Sample 5 versions back in time
versions_to_check = [0, 1, 2, 3, 4]
audit_results = []

for version in versions_to_check:
    df = clone_utils.read_at_version(
        table_path="data/gold/agent_analytics",
        version=version,
    )
    
    audit_results.append({
        'version': version,
        'row_count': len(df),
        'avg_cost': df['total_cost_usd'].mean(),
        'success_rate_avg': df['success_rate_pct'].mean(),
        'timestamp': datetime.now().isoformat(),
    })

audit_df = pd.DataFrame(audit_results)
print(audit_df.to_string())

# Check for cost anomalies
print("\nCost anomalies detected:")
print(audit_df[audit_df['avg_cost'] > audit_df['avg_cost'].mean() * 1.5])
```

### Example 6: SQL Time Travel (Direct SQL)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("time_travel_demo") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .getOrCreate()

# Read at version 3
df_v3 = spark.sql("""
    SELECT * FROM delta.`data/gold/agent_analytics` VERSION AS OF 3
""")

# Read at timestamp
df_yesterday = spark.sql("""
    SELECT * FROM delta.`data/gold/agent_analytics` TIMESTAMP AS OF '2026-09-05T12:00:00'
""")

# Show row count progression
spark.sql("""
    SELECT
        version,
        timestamp,
        operation,
        (SELECT COUNT(*) FROM delta.`data/gold/agent_analytics` VERSION AS OF version) as row_count
    FROM (
        SELECT version, timestamp, operation 
        FROM table_history("data/gold/agent_analytics")
        ORDER BY version DESC LIMIT 10
    )
""").show()
```

### Time Travel Use Cases

| Use Case | Query Type | Benefit |
|----------|-----------|---------|
| **Audit Trail** | Read at timestamp | Track who changed what and when |
| **Data Recovery** | Read at version | Restore accidentally deleted data |
| **Testing** | Read at version | Reproduce bugs from specific time |
| **SLA Compliance** | Read at timestamp | Prove data freshness/availability |
| **Trend Analysis** | Compare versions | Track metrics over time |
| **Rollback** | Restore to version | Undo accidental changes |

---

## Shallow Cloning

### What is Shallow Cloning?

A native shallow clone is a **zero-copy snapshot** of a Delta table. The project also supports an independent-copy fallback on Windows. Native clones:

- Shares underlying data files with the source table (uses no new disk space)
- Has independent transaction history (safe to modify)
- Maintains full ACID semantics
- Can be snapshotted at any version

### Shallow vs Deep Clone

| Aspect | Shallow Clone | Deep Clone |
|--------|---------------|-----------|
| **Disk Space** | Minimal (metadata only) | Full copy of all data |
| **Speed** | ~instant (< 1 sec) | Proportional to data size |
| **Independence** | Complete isolation | Complete isolation |
| **Use Case** | Sandboxing, testing | Backups, archives |
| **Cost** | Very low | High |

### Example 1: Create Sandbox Clone for Testing

```python
from src.delta_guard.delta_clone_utils import DeltaCloneUtils

clone_utils = DeltaCloneUtils()

# Create isolated test sandbox in <1 second
result = clone_utils.create_shallow_clone(
    source_table_path="data/gold/agent_analytics",
    target_table_path="data/sandbox/agent_analytics_test",
    replace=True,  # Overwrite if already exists
)

print(f"Clone created in {result['clone_duration_seconds']:.2f}s")
print(f"Rows: {result['source_row_count']} → {result['target_row_count']}")
# Output:
# Clone created in 0.15s
# Rows: 1500 → 1500
```

### Example 2: Sandbox Experimentation Workflow

```python
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
clone_utils = DeltaCloneUtils(spark)

# Step 1: Create isolated sandbox
clone_result = clone_utils.create_shallow_clone(
    source_table_path="data/gold/agent_analytics",
    target_table_path="data/sandbox/agent_analytics_experiment",
    replace=True,
)
print(f"✓ Sandbox created: {clone_result['target_path']}")

# Step 2: Apply experimental changes to sandbox only
spark.sql("""
    DELETE FROM delta.`data/sandbox/agent_analytics_experiment`
    WHERE success_rate_pct < 50.0
""")
print("✓ Applied experimental filters to sandbox")

# Step 3: Validate on sandbox data
result_df = spark.sql("""
    SELECT 
        tool_name,
        COUNT(*) as count,
        AVG(total_cost_usd) as avg_cost,
        AVG(success_rate_pct) as avg_success_rate
    FROM delta.`data/sandbox/agent_analytics_experiment`
    GROUP BY tool_name
    ORDER BY avg_success_rate DESC
""")
print("✓ Validation results:")
result_df.show()

# Step 4: Verify original table unchanged
original_count = spark.sql("""
    SELECT COUNT(*) FROM delta.`data/gold/agent_analytics`
""").collect()[0][0]
print(f"✓ Original table untouched: {original_count} rows")
```

### Example 3: Multi-Version Sandboxes

```python
from datetime import datetime

clone_utils = DeltaCloneUtils()

# Create sandboxes for different versions
versions_to_test = [0, 1, 2]

for version in versions_to_test:
    # Note: In a real Spark environment with Delta, you'd read from version,
    # then clone. For now, this demonstrates the API pattern:
    
    clone_result = clone_utils.create_shallow_clone(
        source_table_path="data/gold/agent_analytics",
        target_table_path=f"data/sandbox/agent_analytics_v{version}_test",
        replace=True,
    )
    print(f"✓ Created sandbox for version {version}: {clone_result['clone_duration_seconds']:.3f}s")

print("\nAll sandboxes created. Each is independent and safe to modify.")
```

### Example 4: Clone at Historical Version

```python
from datetime import datetime, timedelta

# Read agent_analytics as it was 2 hours ago
two_hours_ago = datetime.now() - timedelta(hours=2)

# Note: This would require reading at timestamp first, then cloning
# In practice with Spark + Delta:
history = clone_utils.get_table_history(
    table_path="data/gold/agent_analytics",
    limit=5,
)

# Find version from 2 hours ago
for row in history.collect():
    commit_time = row['timestamp']
    if commit_time < two_hours_ago:
        target_version = row['version']
        break

print(f"Creating sandbox from version {target_version} (from 2+ hours ago)")
# Then create clone from that version
```

### Example 5: Production Testing Workflow

```python
from datetime import datetime

def test_new_dbt_models_on_sandbox():
    """Test new dbt models against cloned production data without risk."""
    
    clone_utils = DeltaCloneUtils()
    
    # Step 1: Create sandbox from production Gold layer
    sandbox = clone_utils.create_shallow_clone(
        source_table_path="data/gold/agent_analytics",
        target_table_path="data/sandbox/agent_analytics_dbt_test",
        replace=True,
    )
    print(f"✓ Sandbox created: {sandbox['source_row_count']} rows")
    
    # Step 2: Run dbt with target pointing to sandbox
    import subprocess
    result = subprocess.run(
        ["dbt", "run", "--target", "sandbox_test"],
        cwd="dbt_delta_guard",
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print("✓ dbt models ran successfully on sandbox")
        
        # Step 3: Validate outputs
        validation_results = clone_utils.get_table_details(
            table_path="data/sandbox/agent_analytics_dbt_test"
        )
        print(f"✓ Validation: {validation_results['row_count']} rows, "
              f"schema valid")
        
        return True
    else:
        print(f"✗ dbt models failed: {result.stderr}")
        return False

# Run the workflow
if test_new_dbt_models_on_sandbox():
    print("\n✓ Ready to promote to production")
else:
    print("\n✗ Fix errors before promoting")
```

### Shallow Clone Use Cases

| Use Case | Benefit |
|----------|---------|
| **Sandbox Testing** | Test changes without affecting production |
| **Development Environments** | Each dev gets instant isolated clone |
| **CI/CD Pipelines** | Run tests on production-like data, zero storage cost |
| **Analytics Exploration** | Experiment without production impact |
| **Data Validation** | Audit queries on independent snapshot |
| **Blue-Green Deployments** | Test new schemas/transformations safely |

---

## Practical Workflows

### Workflow 1: Safe Data Mutation Testing

```python
def test_data_mutation_safely():
    """Test DELETE/UPDATE operations on sandbox, verify results."""
    
    clone_utils = DeltaCloneUtils()
    
    # Create sandbox
    sandbox_path = "data/sandbox/mutation_test"
    clone_utils.create_shallow_clone(
        source_table_path="data/gold/agent_analytics",
        target_table_path=sandbox_path,
        replace=True,
    )
    
    spark = SparkSession.getActiveSession()
    
    # Count before mutation
    before = spark.sql(f"SELECT COUNT(*) FROM delta.`{sandbox_path}`").collect()[0][0]
    
    # Test DELETE
    spark.sql(f"DELETE FROM delta.`{sandbox_path}` WHERE total_cost_usd > 10.0")
    
    after = spark.sql(f"SELECT COUNT(*) FROM delta.`{sandbox_path}`").collect()[0][0]
    
    print(f"Before: {before} rows | After: {after} rows | Deleted: {before - after}")
    
    # Original table unchanged
    original = spark.sql("SELECT COUNT(*) FROM delta.`data/gold/agent_analytics`").collect()[0][0]
    print(f"Original table: {original} rows (untouched ✓)")
```

### Workflow 2: Incident Response with Time Travel

```python
def investigate_incident():
    """Use time travel to investigate data quality incident."""
    
    clone_utils = DeltaCloneUtils()
    
    # Get current state
    current = clone_utils.read_at_timestamp(
        table_path="data/gold/agent_analytics",
        timestamp=datetime.now().isoformat(),
    )
    
    # Get state 1 hour before incident
    one_hour_ago = (datetime.now() - timedelta(hours=1)).isoformat()
    before_incident = clone_utils.read_at_timestamp(
        table_path="data/gold/agent_analytics",
        timestamp=one_hour_ago,
    )
    
    # Compare
    current_cost_sum = current['total_cost_usd'].sum()
    before_cost_sum = before_incident['total_cost_usd'].sum()
    cost_delta = current_cost_sum - before_cost_sum
    
    print(f"Cost changed: ${before_cost_sum:.2f} → ${current_cost_sum:.2f}")
    print(f"Delta: ${cost_delta:.2f} ({(cost_delta/before_cost_sum)*100:.1f}%)")
    
    # Find the culprit tool
    tools_before = set(before_incident['tool_name'].unique())
    tools_current = set(current['tool_name'].unique())
    new_tools = tools_current - tools_before
    
    if new_tools:
        print(f"\n⚠ New tools introduced: {new_tools}")
    
    # Check for anomalies
    high_cost_tools = current[current['total_cost_usd'] > current['total_cost_usd'].quantile(0.95)]
    print(f"\nHigh-cost outliers:")
    print(high_cost_tools[['tool_name', 'total_cost_usd', 'success_rate_pct']])
```

---

## Configuration

### Enable in dbt

Add to `dbt_project.yml`:

```yaml
models:
  dbt_delta_guard:
    staging:
      +materialized: view
    intermediate:
      +materialized: ephemeral
    marts:
      +materialized: table
      +file_format: delta  # Enables time travel & cloning
```

### Enable in Spark

```python
spark = SparkSession.builder \
    .appName("delta_demo") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```

---

## Limitations & Considerations

| Aspect | Limitation | Workaround |
|--------|-----------|-----------|
| **Time Travel Retention** | Default 30 days | Adjust with `delta.logRetentionDays` |
| **Clone Performance** | Shallow clone metadata can grow | Monitor `_delta_log` size |
| **Sandbox Isolation** | Sandboxes still use same storage backend | Separate cloud buckets for prod/dev |
| **Version Numbering** | Resets on deep clone | Use timestamps for long-term tracking |

---

## Testing Time Travel & Cloning

See `tests/test_delta_clone_utils.py` for:
- Shallow clone creation and verification
- Deep clone creation and verification  
- Time travel at version and timestamp
- Table history and restoration

Run tests:
```bash
pytest tests/test_delta_clone_utils.py -v
```

---

## References

- [Delta Lake Time Travel](https://docs.delta.io/latest/delta-utility-commands.html#-time-travel)
- [Delta Lake Clone Command](https://docs.delta.io/latest/delta-table-operations.html#cloning-a-delta-table)
- [Delta Lake API Documentation](https://delta.io/docs/api/python/index.html)
