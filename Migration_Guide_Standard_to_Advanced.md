# CockroachDB Cloud: Standard to Advanced Migration Guide

## Using Enterprise Changefeeds via S3

---

## 1. Overview

This document describes a method for migrating a CockroachDB Cloud **Standard** cluster to a CockroachDB Cloud **Advanced** cluster using **Enterprise Changefeeds** with an **S3 sink**. The approach provides a full initial data load followed by continuous change data capture (CDC), enabling a near-zero-downtime cutover.

### Why this approach?

CockroachDB Cloud Standard clusters do not support Physical Cluster Replication (PCR) or Logical Data Replication (LDR). However, Standard clusters **do** support Enterprise Changefeeds, which stream row-level changes (inserts, updates, deletes) to external sinks such as S3 or Kafka.

```
Source (Standard)  ──>  Changefeed  ──>  S3 Bucket  ──>  CDC Consumer  ──>  Target (Advanced)
```

### Scripts provided

Both scripts are **schema-agnostic** — they auto-discover table columns and primary keys from `information_schema` at startup. No hardcoded table or column mappings are needed regardless of the customer's schema.

| Script | Purpose |
|--------|---------|
| `s3_to_advanced_loader.py` | One-time initial bulk load from S3 changefeed files into the target cluster |
| `cdc_consumer.py` | Long-running process that polls S3 for new changefeed files and applies ongoing UPSERT/DELETE operations to the target |
| `migration_monitor.py` | Monitoring dashboard: compares source/target row counts, shows changefeed and CDC consumer status |

---

## 2. Architecture

```
+---------------------+       +------------------+       +----------------------+
|  CockroachDB        |       |                  |       |  CockroachDB         |
|  Standard Cluster   |──────>|  AWS S3 Bucket   |──────>|  Advanced Cluster    |
|  (Source)            |       |                  |       |  (Target)            |
+---------------------+       +------------------+       +----------------------+
        |                              |                           ^
        |  Enterprise Changefeed       |  ndjson files             |
        |  (all tables)                |  + RESOLVED timestamps    |
        |  initial_scan = yes          |                           |
        +─────────────────────────────>+   s3_to_advanced_loader   |
                                       |   (initial load)          |
                                       |                           |
                                       |   cdc_consumer.py         |
                                       |   (ongoing replication)   |
                                       +───────────────────────────+
```

### How the changefeed JSON format works

The changefeed emits each row change as a single-line JSON object (ndjson) with the `wrapped` envelope:

```json
{"after": {"col1": "val1", "col2": "val2", ...}, "key": ["pk_value"]}
```

- **INSERT / UPDATE**: `after` contains the complete row. The consumer uses `UPSERT` which handles both.
- **DELETE**: `after` is `null`. Only `key` is present. The consumer executes `DELETE WHERE pk = key`.

The scripts never need to distinguish between INSERT and UPDATE because CockroachDB changefeeds emit both as full-row replacements.

---

## 3. Prerequisites

- Python 3.8+
- `pip install boto3 psycopg2-binary`
- AWS CLI configured with access to the S3 bucket
- `cockroach` CLI (for schema export and ad-hoc SQL)
- Source and target cluster connection strings
- S3 bucket in the same AWS region as both clusters

---

## 4. Step-by-Step Migration Procedure

### Step 1: Export schema from source

```bash
cockroach sql \
  --url "postgresql://<user>:<pass>@<source-host>:26257/<db>?sslmode=verify-full" \
  -e "SHOW CREATE ALL TABLES;" > source_schema.sql
```

**Also export custom enum types** (if any):

```sql
SELECT
  'CREATE TYPE ' || n.nspname || '."' || t.typname || '" AS ENUM (' ||
  string_agg('''' || e.enumlabel || '''', ', ' ORDER BY e.enumsortorder) || ');'
FROM pg_type t
JOIN pg_enum e ON t.oid = e.enumtypid
JOIN pg_namespace n ON t.typnamespace = n.oid
WHERE n.nspname = 'public'
GROUP BY n.nspname, t.typname;
```

### Step 2: Clean the schema for the target

The `SHOW CREATE ALL TABLES` output may contain syntax that doesn't apply to the target:

```bash
# Remove the TSV header line
sed -i '' '1d' source_schema.sql

# Fix double-escaped quotes (from TSV export format)
sed -i '' 's/""/"/g' source_schema.sql

# Remove LOCALITY clauses (if target is single-region)
sed -i '' 's/) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;/);/g' source_schema.sql

# Remove surrounding double-quote wrappers on each CREATE TABLE block
sed -i '' '/^"$/d' source_schema.sql
```

### Step 3: Apply enum types and schema to target

```bash
# 1. Create enum types first (they must exist before tables reference them)
cockroach sql --url "<target-dsn>" < enum_types.sql

# 2. Apply table schema
cockroach sql --url "<target-dsn>" < source_schema.sql
```

### Step 4: Create the S3 bucket

```bash
aws s3 mb s3://<bucket-name> --region <region>
```

Place the bucket in the **same AWS region** as both clusters to minimize latency and avoid cross-region data transfer costs.

### Step 5: Verify rangefeed is enabled

```sql
SHOW CLUSTER SETTING kv.rangefeed.enabled;
-- Must return 't'. On CockroachDB Cloud this is typically enabled by default.
```

### Step 6: Generate the changefeed CREATE statement

Rather than manually listing every table, generate it dynamically:

```sql
-- Generate the CREATE CHANGEFEED statement for all tables in the database
SELECT 'CREATE CHANGEFEED FOR ' ||
  string_agg('TABLE ' || table_name, ', ' ORDER BY table_name) ||
  E' INTO ''s3://<bucket>/cdc-stream' ||
  '?AWS_ACCESS_KEY_ID=<key>' ||
  '&AWS_SECRET_ACCESS_KEY=<secret>' ||
  '&AWS_REGION=<region>'' ' ||
  E'WITH initial_scan = ''yes'', resolved = ''30s'', ' ||
  E'format = ''json'', envelope = ''wrapped'', ' ||
  E'on_error = ''pause'', schema_change_policy = ''stop'';'
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE';
```

Then execute the generated statement. See [Best Practices](#5-best-practices-for-changefeed-cdc-jobs) for tuning the options.

### Step 7: Run the initial load

Wait for the changefeed initial scan to complete (ndjson files appear in S3), then:

```bash
export TARGET_DSN="postgresql://<user>:<pass>@<target-host>:26257/<db>?sslmode=verify-full"
export S3_BUCKET="<bucket-name>"
export S3_PREFIX="cdc-stream/"
export AWS_PROFILE="<profile>"
export BATCH_SIZE="200"          # Tune based on row size (see Best Practices)

python3 s3_to_advanced_loader.py
```

The script will:
1. Connect to the target and auto-discover all table schemas from `information_schema`
2. List ndjson files from S3, grouped by table name
3. For each table, parse the changefeed JSON and `UPSERT` rows in batches
4. Print row counts per table for validation

### Step 8: Seed the CDC consumer state and start it

After the initial load, seed the state file so the consumer skips already-loaded files:

```bash
python3 -c "
import boto3, json, os
session = boto3.Session(
    profile_name=os.environ.get('AWS_PROFILE', 'default'),
    region_name=os.environ.get('AWS_REGION', 'us-west-2')
)
s3 = session.client('s3')
bucket = os.environ['S3_BUCKET']
prefix = os.environ.get('S3_PREFIX', 'cdc-stream/')
files = []
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    for obj in page.get('Contents', []):
        if obj['Key'].endswith('.ndjson'):
            files.append(obj['Key'])
files.sort()
with open('cdc_consumer_state.json', 'w') as f:
    json.dump({'processed_files': files, 'last_resolved': None}, f)
print(f'Seeded state with {len(files)} file(s)')
"
```

Then start the consumer:

```bash
nohup python3 -u cdc_consumer.py > cdc_consumer.log 2>&1 &
echo "CDC Consumer PID: $!"
```

### Step 9: Validate

```sql
-- Run on both source and target, compare results
SELECT table_name, count(*) as row_count
FROM information_schema.tables t
JOIN LATERAL (SELECT count(*) FROM <table>) c ON true
WHERE table_schema = 'public';
```

Or use a simple script:

```bash
for table in $(cockroach sql --url "<source>" -e \
  "SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY 1;" \
  --format csv | tail -n +2); do
  src=$(cockroach sql --url "<source>" -e "SELECT count(*) FROM $table;" --format csv | tail -1)
  tgt=$(cockroach sql --url "<target>" -e "SELECT count(*) FROM $table;" --format csv | tail -1)
  match="YES"; [ "$src" != "$tgt" ] && match="MISMATCH"
  printf "%-45s source=%-10s target=%-10s %s\n" "$table" "$src" "$tgt" "$match"
done
```

### Step 10: Cutover

1. **Pause writes** on source (application maintenance mode)
2. **Wait for CDC consumer to drain** — monitor log for idle state
3. **Final row count validation** across all tables
4. **Cancel the changefeed**: `CANCEL JOB <job_id>;`
5. **Stop CDC consumer**: `kill <pid>`
6. **Switch application** connection strings to target
7. **Resume writes**

---

## 5. Best Practices for Changefeed / CDC Jobs

### 5.1 Changefeed options to protect the source cluster

| Option | Recommended Value | Purpose |
|--------|-------------------|---------|
| `resolved` | `'30s'` to `'60s'` | How often RESOLVED timestamp markers are emitted. Lower values = more S3 PUTs and higher overhead on the source. **Do not set below `'10s'` on production clusters.** |
| `min_checkpoint_frequency` | `'30s'` (default) | Minimum interval between changefeed checkpoints. Lower values increase write amplification on the source. Leave at default unless you need tighter RPO. |
| `on_error` | `'pause'` | Pauses the changefeed on transient errors instead of permanently failing. Allows you to investigate and `RESUME JOB`. |
| `schema_change_policy` | `'stop'` | Stops the changefeed if a DDL change occurs on a watched table. This prevents schema drift between source and target. |
| `protect_data_from_gc_on_pause` | `true` (default) | Prevents GC from collecting data the changefeed hasn't emitted yet. **Warning:** If a changefeed is paused for too long, this can cause unbounded storage growth. Monitor paused changefeeds. |
| `initial_scan` | `'yes'` | Performs a full scan of all rows before switching to streaming mode. Required for migration. |

### 5.2 Controlling initial scan pressure on the source

The changefeed initial scan can put significant read pressure on the source cluster. To throttle it:

```sql
-- Limit concurrent scan requests (default varies by cluster size)
-- Lower this if you see increased p99 latency on the source during the scan
SET CLUSTER SETTING changefeed.backfill.concurrent_scan_requests = 1;
```

For large databases, consider:
- Running the initial scan during **off-peak hours**
- Creating the changefeed for a **subset of tables** at a time rather than all at once
- Monitoring source cluster CPU, memory, and latency during the scan

### 5.3 Batch size tuning

The batch size determines how many rows are sent to the target per transaction. The goal is to balance throughput against transaction size limits and serialization conflicts.

| Row Size | Recommended `BATCH_SIZE` | Rationale |
|----------|--------------------------|-----------|
| Narrow (< 1 KB per row) | 500–1000 | Small rows; larger batches improve throughput |
| Medium (1–10 KB per row) | 100–200 | Moderate rows; balances throughput and conflict risk |
| Wide (> 10 KB per row, e.g. JSONB, TEXT, VECTOR) | 20–50 | Large rows can exceed transaction memory limits or cause contention |

**Rule of thumb:** Keep each batch under ~4 MB total payload. To estimate:
```
BATCH_SIZE = 4,000,000 / avg_row_size_bytes
```

Set via environment variable:
```bash
export BATCH_SIZE=200
```

### 5.4 Transaction retry and backoff strategy

CockroachDB uses serializable isolation. Under concurrent writes, transactions may receive `RETRY_SERIALIZABLE` errors. Both scripts implement automatic retry with **exponential backoff**:

```
Attempt 1: immediate
Attempt 2: wait 0.1s
Attempt 3: wait 0.2s
Attempt 4: wait 0.4s
Attempt 5: wait 0.8s
(then fail)
```

Tuning parameters:

| Parameter | Env Variable | Default | When to change |
|-----------|-------------|---------|----------------|
| Max retries | `MAX_RETRIES` | `5` | Increase if you see frequent retry exhaustion under heavy concurrent writes |
| Base delay | `RETRY_BASE_DELAY` | `0.1` (seconds) | Increase if the target cluster is under contention |

If retries are exhausted consistently, it usually indicates one of:
- Batch size is too large (reduce `BATCH_SIZE`)
- Target cluster is undersized for the write throughput
- Application writes to the target are conflicting with the migration load

### 5.5 Monitoring the changefeed

```sql
-- Check changefeed status and lag
SHOW CHANGEFEED JOB <job_id>;

-- Key columns to monitor:
--   status           = 'running' (good), 'paused' (needs attention), 'failed' (critical)
--   high_water_timestamp = latest fully-emitted timestamp
--   error            = error message if paused/failed
```

**Changefeed lag** = `now() - high_water_timestamp`. If lag grows continuously, the source is producing changes faster than the changefeed can emit them. Remedies:
- Increase cluster resources (CPU/memory)
- Reduce `resolved` interval overhead
- Check for hot ranges or write hotspots

```sql
-- Monitor all running changefeeds
SELECT job_id, status, high_water_timestamp, error
FROM [SHOW CHANGEFEED JOBS]
WHERE status != 'canceled';
```

### 5.6 S3 considerations

- **Bucket region**: Same region as both clusters. Cross-region transfer adds latency and cost.
- **S3 request costs**: Each ndjson file and RESOLVED file is an S3 PUT. With `resolved='10s'`, that is 6 PUTs/min just for timestamps. For very long migrations, this can add up. Use `resolved='30s'` or `'60s'` for cost-sensitive environments.
- **S3 lifecycle policy**: After migration is complete, set a lifecycle policy to expire changefeed files after a retention period (e.g., 7 days) to avoid indefinite storage costs.
- **IAM credentials**: Use long-lived IAM user credentials or IAM roles for production. AWS SSO temporary credentials expire (typically 1-12 hours) and will cause the changefeed to pause.

### 5.7 Handling schema changes during migration

The changefeed uses `schema_change_policy = 'stop'`. If a DDL change is needed during migration:

1. Apply the DDL to the **target** cluster first
2. Cancel the old changefeed
3. Apply the DDL to the **source** cluster
4. Create a new changefeed with `cursor` set to the last RESOLVED timestamp:
   ```sql
   CREATE CHANGEFEED FOR ... INTO '...'
   WITH cursor = '<resolved_timestamp>', initial_scan = 'no', ...;
   ```
5. The CDC consumer will automatically discover the updated schema on next reconnect

Since the scripts discover schema from `information_schema` at startup, new columns are picked up automatically after a restart.

### 5.8 GC TTL awareness

CockroachDB garbage-collects old MVCC versions based on `gc.ttlseconds` (default: 4 hours on Cloud). If the changefeed is paused for longer than the GC TTL, it cannot resume and must be recreated. Monitor paused changefeeds and resume them promptly.

```sql
-- Check GC TTL for a table
SHOW ZONE CONFIGURATION FOR TABLE <table_name>;
```

### 5.9 Consumer-side throttling

If the target cluster is also serving production traffic, throttle the CDC consumer to avoid overwhelming it:

```bash
# Increase poll interval to reduce write frequency
export POLL_INTERVAL=30

# Reduce batch size to lower per-transaction impact
export BATCH_SIZE=50

# Increase backoff to give the target breathing room
export RETRY_BASE_DELAY=0.5
```

---

## 6. Configuration Reference

All configuration is via environment variables. Both scripts fall back to built-in defaults if variables are not set.

| Variable | Used By | Default | Description |
|----------|---------|---------|-------------|
| `TARGET_DSN` | Both | (built-in) | PostgreSQL connection string to the target cluster |
| `S3_BUCKET` | Both | (built-in) | S3 bucket name for changefeed files |
| `S3_PREFIX` | Both | `cdc-stream/` | S3 key prefix for changefeed output |
| `AWS_PROFILE` | Both | (built-in) | AWS CLI profile for S3 access |
| `AWS_REGION` | Both | `us-west-2` | AWS region |
| `TARGET_SCHEMA` | Both | `public` | Database schema to discover tables from |
| `BATCH_SIZE` | Both | `200` | Rows per UPSERT batch |
| `MAX_RETRIES` | Both | `5` | Max transaction retries on serialization failure |
| `RETRY_BASE_DELAY` | Both | `0.1` | Base delay (seconds) for exponential backoff |
| `POLL_INTERVAL` | Consumer | `10` | S3 poll interval (seconds) |
| `STATE_FILE` | Consumer | `./cdc_consumer_state.json` | Path to processed-file tracking state |
| `STATUS_EVERY_N_POLLS` | Consumer | `6` | Print idle status every N polls |

---

## 7. How Schema Auto-Discovery Works

Both scripts query the target database at startup to build their table/column mappings dynamically:

**Table columns** (ordered by position for correct UPSERT alignment):
```sql
SELECT table_name, column_name
FROM information_schema.columns
WHERE table_schema = 'public'
ORDER BY table_name, ordinal_position;
```

**Primary keys** (for DELETE operations in the CDC consumer):
```sql
SELECT kcu.table_name, kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
 AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_schema = 'public'
ORDER BY kcu.table_name, kcu.ordinal_position;
```

**Foreign key load order** (initial loader uses topological sort to load parents before children):
```sql
SELECT DISTINCT
    kcu.table_name AS child,
    ccu.table_name AS parent
FROM information_schema.referential_constraints rc
JOIN information_schema.key_column_usage kcu
    ON rc.constraint_name = kcu.constraint_name
    AND rc.constraint_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage ccu
    ON rc.unique_constraint_name = ccu.constraint_name
    AND rc.unique_constraint_schema = ccu.table_schema
WHERE kcu.table_schema = 'public';
```

The initial loader applies Kahn's algorithm (topological sort) on these FK dependencies so parent tables always load before child tables, avoiding foreign key violations during bulk load.

**JSONB column handling**: Changefeed JSON output emits JSONB values as native JSON objects/arrays. Python's `json.loads()` converts these to dicts/lists, but psycopg2 requires JSON strings for JSONB columns. Both scripts automatically detect and convert these with `json.dumps()` before writing.

This means:
- **No code changes** are needed for different customer schemas
- **New tables** added to the changefeed are automatically handled after restarting the consumer
- **New columns** (after a schema change) are picked up on consumer restart
- **FK ordering** is handled automatically — no need to drop/re-add foreign keys

---

## 8. Adapting for a Customer Deployment

To use this for a different customer, the only changes needed are:

### 1. Set environment variables

```bash
export TARGET_DSN="postgresql://user:pass@target-host:26257/dbname?sslmode=verify-full"
export S3_BUCKET="customer-migration-bucket"
export S3_PREFIX="cdc-stream/"
export AWS_PROFILE="customer-aws-profile"
export AWS_REGION="us-east-1"        # Match the cluster region
export BATCH_SIZE="200"              # Tune based on row size
```

### 2. Export and apply the customer's schema

Follow Steps 1–3 in [Section 4](#4-step-by-step-migration-procedure). The enum type export query and schema cleaning commands work for any CockroachDB database.

### 3. Create the changefeed

Use the dynamic generation query from Step 6 — it automatically includes all tables in the `public` schema.

### 4. Run the scripts

```bash
# Initial load
python3 s3_to_advanced_loader.py

# Seed state
python3 seed_state.py  # (or inline script from Step 8)

# Start consumer
nohup python3 -u cdc_consumer.py > cdc_consumer.log 2>&1 &
```

No code modifications needed. The scripts will auto-discover the customer's tables, columns, and primary keys.

---

## 9. Monitoring the Migration

### Migration Monitor (`migration_monitor.py`)

A standalone monitoring script that provides a comprehensive migration status report:

```bash
# One-shot report
python3 migration_monitor.py

# Auto-refresh every 30 seconds (watch mode)
python3 migration_monitor.py --watch

# Custom refresh interval (10 seconds)
python3 migration_monitor.py --watch 10
```

The report includes four sections:

**1. Changefeed Status** — Queries the source cluster's `SHOW CHANGEFEED JOBS` to display the changefeed job ID, status (running/paused/failed), last resolved timestamp, and creation time.

**2. CDC Consumer Status** — Reads the consumer's state file (`cdc_consumer_state.json`) to show:
- When the consumer started and last processed a file
- Total files processed
- Last resolved timestamp from S3
- Per-table CDC statistics (upserts, deletes, files processed, errors)
- Last 5 errors with timestamps

**3. Row Count Comparison** — Connects to both source and target clusters and compares row counts for every table. Each table is marked as:
- `OK` — source and target match
- `+N` / `-N` — target has more/fewer rows (expected during active CDC)
- `NO SRC` / `NO TGT` — table exists on one cluster but not the other

**4. Migration Summary** — Reports whether all tables are in sync or how many have differences.

### CDC Consumer State File

The CDC consumer writes a JSON state file (`cdc_consumer_state.json`) with per-table tracking:

```json
{
  "processed_files": ["cdc-stream/...file1.ndjson", "..."],
  "last_resolved": "cdc-stream/2026-02-12/...RESOLVED",
  "started_at": "2026-02-12T00:36:19Z",
  "last_activity": "2026-02-12T00:45:47Z",
  "table_stats": {
    "login": {"upserts": 502, "deletes": 0, "files": 1, "errors": 0},
    "items": {"upserts": 14440, "deletes": 4, "files": 20, "errors": 0}
  },
  "errors": []
}
```

### Manual SQL validation

You can also check migration progress directly via SQL:

```sql
-- On source: check changefeed status
SHOW CHANGEFEED JOBS;

-- On target: get row counts for all tables
SELECT table_name,
       (SELECT count(*) FROM <table_name>) as row_count
FROM information_schema.tables
WHERE table_schema = 'public' AND table_type = 'BASE TABLE';

-- On source: check changefeed is emitting resolved timestamps
-- (look at the running_status column for the resolved= value)
SELECT job_id, status, running_status FROM [SHOW CHANGEFEED JOBS];
```

---

## 10. Limitations and Caveats

| Limitation | Detail |
|------------|--------|
| **No computed columns** | Changefeeds do not emit computed/virtual columns. These are recomputed on the target from their expressions as defined in the schema. |
| **JSONB column ordering** | JSON key ordering in changefeed output may differ from the original insertion order. This does not affect correctness since JSONB is unordered. |
| **Sequence values** | `unique_rowid()` and `gen_random_uuid()` defaults generate new values on the target. Since the changefeed provides the actual values, this is not an issue — the UPSERT uses the source values directly. |
| **Schema changes** | DDL changes on the source will stop the changefeed (`schema_change_policy = 'stop'`). Manual coordination is required. |
| **Foreign key ordering (initial load)** | The `s3_to_advanced_loader.py` uses topological sort to automatically load parent tables before children, avoiding FK violations. Circular FK references are appended at the end and may require temporarily dropping one FK. |
| **Foreign key ordering (CDC consumer)** | During ongoing replication, changefeed files arrive in S3 key order (alphabetical), not FK order. If a child row arrives before its parent, the consumer logs an error, reconnects, and retries on the next poll — by which time the parent row is usually present. This is self-healing. |
| **Changefeed covers only DML** | DDL changes (ALTER TABLE, CREATE INDEX) are **not** replicated. Schema changes must be applied manually to both clusters. |
| **S3 eventual consistency** | AWS S3 provides strong read-after-write consistency for PUTs, so newly written changefeed files are immediately visible to the consumer. |

---

## Appendix A: Proof-of-Concept Results

The following results were obtained from a live migration test.

### Environment

| Attribute | Value |
|-----------|-------|
| Source | CockroachDB Cloud Standard, us-west-2 |
| Target | CockroachDB Cloud Advanced, us-west-2 |
| Tables | 23 (10 custom enum types, 20 foreign keys) |
| Total rows | 21,289 across all 23 tables |
| Largest table | `items` — 14,431 rows (~96 MB with VECTOR(1536) columns) |

### Initial load (all 23 tables)

```
Batch size: 200
FK-aware load order: api_client -> items -> item_vectors -> login -> access_token ->
  device -> device_token -> device_token_authentication_factor -> login_create ->
  login_dependency -> login_force_set_credentials -> login_invitation -> login_role ->
  login_username_freeze -> login_username_log -> okta_user_sync_job ->
  previous_password -> registrable_authentication_factor -> reset_log ->
  role_privilege -> supersede_login_authorization -> username_change_request ->
  webauthn_credential

INITIAL LOAD COMPLETE
Total rows: 22,098 | Time: 288.7s | Avg: 77 rows/s
```

All 23 tables loaded successfully with FK ordering — zero FK violations during bulk load.

### Multi-table DML propagation test

Operations performed on the **source** cluster across 7 tables simultaneously:

| Operation | Table | Detail | Propagated |
|-----------|-------|--------|------------|
| INSERT | `login` | id=9999, `cdc_test_user@example.com` | Yes |
| INSERT | `api_client` | id=9999, `cdc_test_client` | Yes |
| INSERT | `login_role` | id=9999, role=USER_ROLE | Yes |
| INSERT | `device` | id=9999, `CDC Test Device` (IOS) | Yes |
| UPDATE | `login` | id=1, status→LOCKED, failed_auth_cnt→99 | Yes |
| UPDATE | `api_client` | id=1, enabled→false | Yes |
| UPDATE | `login_role` | id=1, role→ADMIN_ROLE | Yes |
| DELETE | `webauthn_credential` | id=150 removed | Yes |
| DELETE | `previous_password` | id=600 removed | Yes |
| DELETE | `login_dependency` | id=400 removed | Yes |

All 10 operations across 7 tables propagated correctly. The CDC consumer detected and applied all changes within one poll cycle (~10s after changefeed emission).

### CDC consumer resilience

During multi-table CDC testing, the `device` insert arrived before the `login` insert (alphabetical S3 key ordering), causing a temporary FK violation. The consumer:
1. Logged the error
2. Reconnected and re-discovered schema
3. Processed the `login` file (which arrived in the same batch)
4. Successfully retried the `device` file on the next poll

This demonstrates the consumer's self-healing behavior for FK ordering during ongoing replication.

### Row count validation (all 23 tables)

```
access_token                             800 rows  ✓
api_client                               50 rows   ✓
device                                   400 rows  ✓
device_token                             600 rows  ✓
device_token_authentication_factor       500 rows  ✓
item_vectors                             200 rows  ✓
items                                    14,431 rows ✓
login                                    500 rows  ✓
login_create                             500 rows  ✓
login_dependency                         400 rows  ✓
login_force_set_credentials              250 rows  ✓
login_invitation                         100 rows  ✓
login_role                               1,000 rows ✓
login_username_freeze                    200 rows  ✓
login_username_log                       500 rows  ✓
okta_user_sync_job                       5 rows    ✓
previous_password                        600 rows  ✓
registrable_authentication_factor        300 rows  ✓
reset_log                                300 rows  ✓
role_privilege                           3 rows    ✓
supersede_login_authorization            100 rows  ✓
username_change_request                  200 rows  ✓
webauthn_credential                      150 rows  ✓
```

All row counts match between source and target.
