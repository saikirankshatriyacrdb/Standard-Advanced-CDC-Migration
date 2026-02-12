# CockroachDB Standard-to-Advanced Migration: Live Demo Runbook

> **Audience**: Customer-facing demo. Walks through an end-to-end migration using Enterprise Changefeeds via S3.
>
> **Duration**: ~30 minutes live, or self-paced
>
> **What you need running**: Source (Standard) cluster, Target (Advanced) cluster, S3 bucket, AWS credentials

---

## Terminal Layout

Open **four** terminal tabs/panes before starting:

| Pane | Purpose | Label suggestion |
|------|---------|------------------|
| T1 | Main demo commands | `DEMO` |
| T2 | Source cluster SQL shell | `SOURCE` |
| T3 | Target cluster SQL shell | `TARGET` |
| T4 | CDC consumer / monitor | `MONITOR` |

---

## Pre-Demo Setup

### Set environment variables (all panes)

Paste this block into **every terminal pane**. Replace placeholders with your actual values.

```bash
# ── Cluster connections ──
export SOURCE_DSN="postgresql://<user>:<pass>@<source-host>:26257/<db>?sslmode=verify-full"
export TARGET_DSN="postgresql://<user>:<pass>@<target-host>:26257/<db>?sslmode=verify-full"

# ── S3 ──
export S3_BUCKET="<your-bucket>"
export S3_PREFIX="cdc-stream/"
export AWS_PROFILE="<your-aws-profile>"
export AWS_REGION="us-west-2"

# ── Script tuning ──
export BATCH_SIZE="200"
export POLL_INTERVAL="10"
export STATE_FILE="./cdc_consumer_state.json"
```

### Prerequisites check (T1)

```bash
python3 --version          # 3.8+
pip3 show boto3 psycopg2-binary | grep -E "^Name|^Version"
cockroach version
aws sts get-caller-identity --profile "$AWS_PROFILE"
```

**Expected**: Python 3.8+, boto3 and psycopg2-binary installed, cockroach CLI available, AWS identity confirmed.

### Open SQL shells (T2 and T3)

```bash
# T2 — Source
cockroach sql --url "$SOURCE_DSN"

# T3 — Target
cockroach sql --url "$TARGET_DSN"
```

### Remove stale state (T1)

```bash
rm -f cdc_consumer_state.json
```

---

## Act 1: Schema Discovery

> **Talking point**: "The first step in any migration is understanding what we're working with. Let's export the full schema from the source Standard cluster."

### 1.1 Export schema from source (T1)

```bash
cockroach sql --url "$SOURCE_DSN" \
  -e "SHOW CREATE ALL TABLES;" > source_schema.sql
```

### 1.2 Show the complexity (T1)

```bash
# Count tables
grep -c "^CREATE TABLE\|^\"CREATE TABLE" source_schema.sql

# Count foreign keys
grep -c "ADD CONSTRAINT.*FOREIGN KEY" source_schema.sql
```

**Expected output**:
```
23
20
```

> **Talking point**: "23 tables with 20 foreign key relationships. This is a real identity/auth schema — users, tokens, devices, credentials, WebAuthn, RBAC. Not a toy example."

### 1.3 Show enum types on the source (T2 — Source SQL shell)

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

**Expected**: 10 enum types including `UserStatus`, `UserType`, `Role`, `AuthenticationLevel`, `TokenStatus`, `TokenPurpose`, `AuthenticationFactorType`, `PlatformType`, `LoginInvitationStatus`, `UsernameChangeRequestStatus`.

> **Talking point**: "10 custom enum types. The migration tools handle all of these automatically — no manual column mapping required."

### 1.4 Highlight interesting data types (T2)

```sql
-- VECTOR columns (ML embeddings)
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE data_type LIKE '%VECTOR%' OR udt_name LIKE '%vector%';

-- JSONB columns
SELECT table_name, column_name
FROM information_schema.columns
WHERE data_type = 'jsonb'
ORDER BY table_name;
```

> **Talking point**: "Notice the VECTOR(1536) columns for ML embeddings and multiple JSONB columns. Our tools handle both natively — JSONB is serialized/deserialized automatically, and vectors flow through the changefeed as-is."

---

## Act 2: Schema Prep & Apply

> **Talking point**: "The exported schema has some artifacts from the Standard cluster format. We clean it in four sed commands."

### 2.1 Clean the schema (T1)

```bash
# Remove TSV header line
sed -i '' '1d' source_schema.sql

# Fix double-escaped quotes from TSV export
sed -i '' 's/""/"/g' source_schema.sql

# Remove LOCALITY clauses (Standard cluster artifact)
sed -i '' 's/) LOCALITY REGIONAL BY TABLE IN PRIMARY REGION;/);/g' source_schema.sql

# Remove surrounding double-quote wrappers
sed -i '' '/^"$/d' source_schema.sql
```

> **Talking point**: "Four one-liners. We remove the TSV header, fix escaped quotes, strip LOCALITY clauses that don't apply to the target, and remove wrapper quotes. That's it."

### 2.2 Show the clean result (T1)

```bash
head -5 source_schema.sql
```

**Expected**:
```
CREATE TABLE public.login (
	id INT8 NOT NULL DEFAULT unique_rowid(),
	username STRING NULL,
	username_search_hash STRING NULL,
	password_secret_hash STRING NULL,
```

### 2.3 Apply enum types to target (T3 — Target SQL shell)

> **Note**: You need to create enum types before the tables that reference them. Run the CREATE TYPE statements from Act 1.3 on the target, or use a pre-saved `enum_types.sql` file.

```sql
-- Example (run each CREATE TYPE from the export):
CREATE TYPE public."UserStatus" AS ENUM ('ACTIVE', 'DEACTIVATED', 'LOCKED');
CREATE TYPE public."UserType" AS ENUM ('CONSUMER', 'EMPLOYEE', 'SYSTEM', 'PARTNER');
-- ... (all 10 enum types)
```

### 2.4 Apply schema to target (T1)

```bash
cockroach sql --url "$TARGET_DSN" < source_schema.sql
```

> **Talking point**: "Schema applied — all 23 tables, 20 foreign keys, indexes, constraints. The target is now structurally identical to the source."

### 2.5 Verify on target (T3)

```sql
SELECT count(*) FROM information_schema.tables
WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
```

**Expected**: `23`

---

## Act 3: Changefeed Creation

> **Talking point**: "Instead of manually listing all 23 tables, we dynamically generate the CREATE CHANGEFEED statement from information_schema. This works for any customer schema without modification."

### 3.1 Generate the changefeed SQL dynamically (T2 — Source SQL shell)

```sql
SELECT 'CREATE CHANGEFEED FOR ' ||
  string_agg('TABLE ' || table_name, ', ' ORDER BY table_name) ||
  E' INTO ''s3://' || '<your-bucket>' || '/cdc-stream' ||
  '?AWS_ACCESS_KEY_ID=<key>' ||
  '&AWS_SECRET_ACCESS_KEY=<secret>' ||
  '&AWS_REGION=us-west-2'' ' ||
  E'WITH initial_scan = ''yes'', resolved = ''30s'', ' ||
  E'format = ''json'', envelope = ''wrapped'', ' ||
  E'on_error = ''pause'', schema_change_policy = ''stop'';'
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_type = 'BASE TABLE';
```

> **Talking point**: "This query auto-discovers every table and builds the full CREATE CHANGEFEED statement. Key options: `initial_scan = 'yes'` gives us the full data snapshot, `envelope = 'wrapped'` gives us structured JSON with `after` and `key` fields, and `on_error = 'pause'` so we can investigate and resume rather than losing the job."

### 3.2 Execute the generated changefeed (T2)

Copy and run the output from 3.1. It will return a job ID.

**Expected**:
```
        job_id
----------------------
  9xxxxxxxxxxxxxxxx
```

### 3.3 Monitor the changefeed (T2)

```sql
SELECT job_id, status, running_status
FROM [SHOW CHANGEFEED JOBS]
WHERE status != 'canceled'
ORDER BY created DESC LIMIT 1;
```

**Expected**:
```
  job_id             | status  | running_status
---------------------+---------+------------------------------------------
  9xxxxxxxxxxxxxxxx  | running | running: resolved=..., initial_scan=...
```

> **Talking point**: "The changefeed is running. It's performing the initial scan — streaming every existing row to S3 as ndjson files. Once that completes, it switches to streaming mode and captures every INSERT, UPDATE, and DELETE going forward."

### 3.4 Verify files are landing in S3 (T1)

```bash
aws s3 ls "s3://${S3_BUCKET}/${S3_PREFIX}" --recursive --profile "$AWS_PROFILE" | head -10
```

**Expected**: List of `.ndjson` files appearing, organized by date.

> **Talking point**: "Files are landing in S3. Each file contains changefeed records in ndjson format — one JSON object per line with the full row data."

---

## Act 4: Initial Data Load

> **Talking point**: "Now we run the initial loader. It reads all those S3 files and bulk-loads them into the target. The key feature: it auto-discovers the schema and loads tables in foreign key order using topological sort."

### 4.1 Run the initial loader (T1)

```bash
python3 s3_to_advanced_loader.py
```

**What to watch for** as it runs:
1. Schema discovery: "Discovered N tables from information_schema"
2. FK ordering: tables listed in dependency order (parents before children)
3. Per-table progress: file names, row counts
4. Final summary with totals

**Expected output (end)**:
```
INITIAL LOAD COMPLETE
Total rows: 22,098 | Time: 288.7s | Avg: 77 rows/s
Row count validation on target:
  api_client                               50 rows
  items                                    14,431 rows
  login                                    500 rows
  login_role                               1,000 rows
  device                                   400 rows
  device_token                             600 rows
  ...
```

> **Talking point**: "22,000+ rows across all 23 tables loaded with zero FK violations. Notice the load order — `login` before `login_role`, `device` before `device_token`, `items` before `item_vectors`. The topological sort (Kahn's algorithm) handled that automatically."

### 4.2 Spot-check on target (T3)

```sql
SELECT count(*) FROM login;
SELECT count(*) FROM items;
SELECT count(*) FROM device_token;
```

> **Talking point**: "Row counts match the source. The UPSERT approach means this is idempotent — you can safely re-run it if interrupted."

---

## Act 5: Start CDC Consumer

> **Talking point**: "The initial load is done. Now we need to start the CDC consumer so it picks up any changes that happened during the load and continues replicating going forward. First, we seed its state file."

### 5.1 Seed the consumer state (T1)

This tells the consumer to skip files already processed by the initial loader:

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

**Expected**:
```
Seeded state with 54 file(s)
```

> **Talking point**: "We seeded the state with 54 files. The consumer now knows to skip those and only process new files that arrive after this point."

### 5.2 Start the CDC consumer (T4 — Monitor pane)

```bash
python3 -u cdc_consumer.py
```

**Expected initial output**:
```
Connecting to target database...
Discovering schema from information_schema...
  Found 23 tables, 142 columns, 23 primary keys
Loading state from ./cdc_consumer_state.json
  Resuming: 54 files already processed
Polling S3 for new changefeed files...
[Poll #1] Waiting for changes... (total: 0 upserts, 0 deletes, 0 errors)
```

> **Talking point**: "The consumer auto-discovers the schema — 23 tables, all columns, all primary keys — directly from information_schema. No configuration files, no column mappings. It works for any customer schema out of the box."

---

## Act 6: Live Monitoring

> **Talking point**: "Now let's bring up the monitoring dashboard. This connects to both clusters and compares row counts table by table."

### 6.1 Start the migration monitor (open a 5th pane or use T1)

```bash
python3 migration_monitor.py --watch 10
```

**Expected output**:
```
==============================================================================
  Migration Monitor Report — 2026-02-12 00:45:00 UTC
==============================================================================

CHANGEFEED STATUS (Source Cluster)
------------------------------------------------------------------------------
  Job ID:         9xxxxxxxxxxxxxxxx
  Status:         RUNNING
  Running Status: running: resolved=...
  Created:        2026-02-11 23:04:41
  Last Modified:  2026-02-12 00:44:44

CDC CONSUMER STATUS
------------------------------------------------------------------------------
  Started at:       2026-02-12T00:36:19Z
  Last activity:    2026-02-12T00:45:47Z
  Files processed:  54
  Last resolved:    cdc-stream/2026-02-12/...RESOLVED
  Recent errors:    0

  Per-table CDC stats:
  Table                                      Upserts  Deletes  Files  Errors
  -----------------------------------------  -------- -------- ------ -------
  items                                         14440        4     20       0
  login                                           502        0      1       0
  access_token                                     800        0      1       0
  ...
  TOTAL                                         22098       10     54       0

ROW COUNT COMPARISON (Source vs Target)
------------------------------------------------------------------------------
  Table                                      Source   Target     Diff   Status
  -----------------------------------------  -------- -------- -------- --------
  access_token                                   800      800       +0       OK
  api_client                                      50       50       +0       OK
  device                                         400      400       +0       OK
  items                                        14431    14431       +0       OK
  login                                          500      500       +0       OK
  ...
  TOTAL                                        22098    22098       +0

MIGRATION SUMMARY
------------------------------------------------------------------------------
  Status: ALL TABLES IN SYNC
==============================================================================

  Auto-refreshing every 10s. Press Ctrl+C to stop.
```

> **Talking point**: "Four sections: changefeed job status, CDC consumer stats, table-by-table row comparison, and a summary. ALL TABLES IN SYNC — source and target match exactly across all 23 tables. This refreshes every 10 seconds."

---

## Act 7: Live DML Propagation

> **Talking point**: "This is the exciting part. Let's make live changes on the source and watch them appear on the target in real time."

### 7.1 INSERT on source (T2 — Source SQL shell)

```sql
-- Insert a new user
INSERT INTO login (id, username, username_search_hash, status, type,
  failed_auth_cnt, account_non_locked, phone_authentication_enabled,
  uuid, tfa_enabled, is_temporary)
VALUES (9999, 'cdc_test_user@example.com', md5('cdc_test_user'),
  'ACTIVE', 'CONSUMER', 0, true, false, gen_random_uuid(), false, false);

-- Insert a new API client
INSERT INTO api_client (id, client_id, client_secret_secret_hash, enabled,
  roles, attributes, access_token_expiry_time_seconds,
  refresh_token_expiry_time_seconds)
VALUES (9999, 'cdc_test_client', md5('secret'), true,
  '["USER_ROLE"]', '{"app": "demo"}', 3600, 86400);

-- Insert a child row (references login)
INSERT INTO login_role (id, login_id, "role", create_date)
VALUES (9999, 9999, 'USER_ROLE', now());

-- Insert a device (references login)
INSERT INTO device (id, login_id, name, type, platform_type)
VALUES (9999, 9999, 'CDC Test Device', 'mobile', 'IOS');
```

> **Talking point**: "Four inserts across four tables, including FK dependencies — login_role and device both reference the login we just created."

### 7.2 Watch the consumer (T4)

Within ~10 seconds (one poll cycle), the consumer should show:

```
[Poll #N] Found 4 new file(s)
  [login] ...ndjson
    Applied: 1 upserts, 0 deletes
  [api_client] ...ndjson
    Applied: 1 upserts, 0 deletes
  [login_role] ...ndjson
    Applied: 1 upserts, 0 deletes
  [device] ...ndjson
    Applied: 1 upserts, 0 deletes
```

### 7.3 Verify on target (T3 — Target SQL shell)

```sql
SELECT id, username, status FROM login WHERE id = 9999;
SELECT id, client_id FROM api_client WHERE id = 9999;
SELECT id, login_id, "role" FROM login_role WHERE id = 9999;
SELECT id, login_id, name FROM device WHERE id = 9999;
```

**Expected**: All four rows present on target.

### 7.4 UPDATE on source (T2)

```sql
UPDATE login SET status = 'LOCKED', failed_auth_cnt = 99 WHERE id = 1;
UPDATE api_client SET enabled = false WHERE id = 1;
```

### 7.5 Verify updates on target (T3, after ~10s)

```sql
SELECT id, status, failed_auth_cnt FROM login WHERE id = 1;
-- Expected: status = 'LOCKED', failed_auth_cnt = 99

SELECT id, enabled FROM api_client WHERE id = 1;
-- Expected: enabled = false
```

### 7.6 DELETE on source (T2)

```sql
DELETE FROM login_role WHERE id = 9999;
DELETE FROM device WHERE id = 9999;
```

### 7.7 Verify deletes on target (T3, after ~10s)

```sql
SELECT count(*) FROM login_role WHERE id = 9999;
-- Expected: 0

SELECT count(*) FROM device WHERE id = 9999;
-- Expected: 0
```

> **Talking point**: "Inserts, updates, deletes — all propagated automatically within one poll cycle. The changefeed captures every DML operation, the consumer applies it as UPSERT or DELETE. No application changes needed."

### 7.8 Check monitor (T1 or monitor pane)

The migration monitor should still show `ALL TABLES IN SYNC` once the consumer processes the changes.

---

## Act 8: Cutover Walkthrough

> **Talking point**: "Now let's walk through the cutover procedure. In a real migration, this is where we go from 'replicating' to 'fully migrated'. We won't execute this now, but here's the exact sequence."

### Cutover steps (explain, don't execute)

```
1. PAUSE WRITES     → Put the application in maintenance mode
                       (or drain connections on the source)

2. WAIT FOR DRAIN   → Watch the CDC consumer log for idle state:
                       "[Poll #N] Waiting for changes..."
                       This means all S3 files have been processed.

3. FINAL VALIDATION → Run the migration monitor one last time:
                       python3 migration_monitor.py
                       Confirm: "ALL TABLES IN SYNC"

4. CANCEL CHANGEFEED → On the source cluster:
                       CANCEL JOB <job_id>;

5. STOP CONSUMER    → Ctrl+C the cdc_consumer.py process
                       (or: kill $(cat cdc_consumer.pid))

6. SWITCH APP       → Update application connection strings
                       from SOURCE_DSN → TARGET_DSN

7. RESUME WRITES    → Take the application out of maintenance mode
                       Writes now go to the Advanced cluster
```

> **Talking point**: "Total cutover downtime is just steps 1 through 7 — the time to drain the last few changes and flip the connection string. For a schema this size, that's under a minute. The CDC consumer keeps the target within one poll interval (~10 seconds) of the source at all times."

### Rollback plan

```
If issues are found after cutover:
1. Switch connection strings back to SOURCE_DSN
2. Re-create the changefeed on the source (it's still running)
3. The CDC consumer can be restarted — it resumes from its state file
```

---

## Cleanup

> **Talking point**: "Let me clean up the demo resources."

### Stop the CDC consumer (T4)

Press `Ctrl+C` — the consumer handles SIGINT gracefully.

### Cancel the changefeed (T2 — Source SQL shell)

```sql
-- Find the job ID
SELECT job_id, status FROM [SHOW CHANGEFEED JOBS]
WHERE status = 'running';

-- Cancel it
CANCEL JOB <job_id>;
```

### Remove demo test data (T2 — Source SQL shell)

```sql
DELETE FROM device WHERE id = 9999;
DELETE FROM login_role WHERE id = 9999;
DELETE FROM api_client WHERE id = 9999;
DELETE FROM login WHERE id = 9999;

-- Revert updates
UPDATE login SET status = 'ACTIVE', failed_auth_cnt = 0 WHERE id = 1;
UPDATE api_client SET enabled = true WHERE id = 1;
```

### Clean up local files (T1)

```bash
rm -f cdc_consumer_state.json
rm -f cdc_consumer.log
```

### Optionally clean S3 (T1)

```bash
# List changefeed files
aws s3 ls "s3://${S3_BUCKET}/${S3_PREFIX}" --recursive --profile "$AWS_PROFILE" | wc -l

# Remove all changefeed files (irreversible)
# aws s3 rm "s3://${S3_BUCKET}/${S3_PREFIX}" --recursive --profile "$AWS_PROFILE"
```

---

## Quick Reference: Key Numbers

| Metric | Value |
|--------|-------|
| Tables | 23 |
| Custom enum types | 10 |
| Foreign key constraints | 20 |
| Total rows (test data) | ~22,098 |
| Largest table | `items` (14,431 rows, VECTOR(1536)) |
| JSONB columns | 8 across 4 tables |
| CDC poll interval | 10 seconds |
| Batch size | 200 rows per UPSERT |
| Scripts modified for this schema | 0 (fully schema-agnostic) |

## Quick Reference: Environment Variables

| Variable | Used By | Default |
|----------|---------|---------|
| `SOURCE_DSN` | monitor | (required) |
| `TARGET_DSN` | loader, consumer, monitor | (required) |
| `S3_BUCKET` | loader, consumer | (required) |
| `S3_PREFIX` | loader, consumer | `cdc-stream/` |
| `AWS_PROFILE` | loader, consumer | `default` |
| `AWS_REGION` | loader, consumer | `us-west-2` |
| `BATCH_SIZE` | loader, consumer | `200` |
| `POLL_INTERVAL` | consumer | `10` |
| `STATE_FILE` | consumer, monitor | `./cdc_consumer_state.json` |
| `TARGET_SCHEMA` | all 3 | `public` |

## Quick Reference: Scripts

| Script | Purpose | Invocation |
|--------|---------|------------|
| `s3_to_advanced_loader.py` | One-time bulk load | `python3 s3_to_advanced_loader.py` |
| `cdc_consumer.py` | Ongoing CDC replication | `python3 -u cdc_consumer.py` |
| `migration_monitor.py` | Status dashboard | `python3 migration_monitor.py --watch 10` |
