#!/usr/bin/env python3
"""
S3 Changefeed Initial Load Script (Schema-Agnostic)

Reads ndjson files from a CockroachDB changefeed S3 sink and loads them into
a target CockroachDB cluster. Automatically discovers table schemas from the
target database — no hardcoded table/column mappings required.

Configuration: Set via environment variables or edit the defaults below.

Usage:
  export TARGET_DSN="postgresql://user:pass@host:26257/db?sslmode=verify-full"
  export S3_BUCKET="my-migration-bucket"
  export S3_PREFIX="cdc-stream/"
  export AWS_PROFILE="my-profile"
  python3 s3_to_advanced_loader.py
"""

import boto3
import psycopg2
import psycopg2.extras
import psycopg2.errors
import json
import sys
import os
import time
import logging
from collections import defaultdict

# ---------------------------------------------------------------------------
# Configuration — override via environment variables
# ---------------------------------------------------------------------------
S3_BUCKET = os.environ.get("S3_BUCKET", "my-migration-bucket")
S3_PREFIX = os.environ.get("S3_PREFIX", "cdc-stream/")
AWS_PROFILE = os.environ.get("AWS_PROFILE", "default")
AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")

TARGET_DSN = os.environ.get(
    "TARGET_DSN",
    "postgresql://user:password@target-host:26257/dbname?sslmode=verify-full",
)

# --- Tuning knobs (see Best Practices in the migration guide) ---
# Rows per UPSERT batch. Lower for wide/large rows, higher for narrow rows.
#   Rule of thumb: keep each batch under ~4 MB total payload.
#   Narrow rows (< 1 KB):  500–1000
#   Medium rows (1–10 KB):  100–200
#   Wide rows (> 10 KB):     20–50
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))

# Max transaction retries on RETRY_SERIALIZABLE
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))

# Base delay (seconds) for exponential backoff.  Actual delay = BASE * 2^attempt
RETRY_BASE_DELAY = float(os.environ.get("RETRY_BASE_DELAY", "0.1"))

# Schema to load from (default: public)
TARGET_SCHEMA = os.environ.get("TARGET_SCHEMA", "public")

# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("loader")


# ---------------------------------------------------------------------------
# Schema auto-discovery
# ---------------------------------------------------------------------------
def discover_table_columns(conn, schema="public"):
    """
    Query information_schema to build {table_name: [col1, col2, ...]} mapping.
    Columns are ordered by ordinal_position so UPSERT values align correctly.
    """
    sql = """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = %s
        ORDER BY table_name, ordinal_position
    """
    table_columns = defaultdict(list)
    with conn.cursor() as cur:
        cur.execute(sql, (schema,))
        for table_name, column_name in cur.fetchall():
            table_columns[table_name].append(column_name)

    log.info(
        "Discovered %d table(s) with %d total column(s) from information_schema",
        len(table_columns),
        sum(len(cols) for cols in table_columns.values()),
    )
    return dict(table_columns)


def discover_fk_order(conn, schema="public"):
    """
    Topologically sort tables so parents load before children.
    Returns an ordered list of table names.
    """
    sql = """
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
        WHERE kcu.table_schema = %s
    """
    deps = defaultdict(set)  # child -> set of parents
    all_tables = set()

    with conn.cursor() as cur:
        cur.execute(sql, (schema,))
        for child, parent in cur.fetchall():
            if child != parent:  # skip self-references
                deps[child].add(parent)
            all_tables.add(child)
            all_tables.add(parent)

    # Add tables with no FK relationships
    sql2 = """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
    """
    with conn.cursor() as cur:
        cur.execute(sql2, (schema,))
        for (t,) in cur.fetchall():
            all_tables.add(t)

    # Kahn's algorithm for topological sort
    in_degree = {t: 0 for t in all_tables}
    for child, parents in deps.items():
        in_degree[child] = len(parents)

    queue = sorted([t for t in all_tables if in_degree[t] == 0])
    ordered = []
    while queue:
        t = queue.pop(0)
        ordered.append(t)
        for child, parents in deps.items():
            if t in parents:
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)
                    queue.sort()

    # Append any remaining (circular refs) at the end
    for t in sorted(all_tables):
        if t not in ordered:
            ordered.append(t)

    log.info("FK-aware load order: %s", " -> ".join(ordered))
    return ordered


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------
def get_s3_client():
    session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
    return session.client("s3")


def list_ndjson_files(s3_client):
    """List all ndjson files in the changefeed S3 prefix, grouped by table."""
    files_by_table = defaultdict(list)
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".ndjson"):
                table_name = extract_table_name(key)
                if table_name:
                    files_by_table[table_name].append(key)

    for table in files_by_table:
        files_by_table[table].sort()

    return files_by_table


def extract_table_name(s3_key):
    """
    Extract table name from changefeed ndjson filename.
    Format: <timestamp>-<nodeid>-<part>-<seq>-<hex>-<TABLE>-<N>.ndjson
    The table name is the second-to-last hyphen-delimited segment.
    """
    basename = s3_key.rsplit("/", 1)[-1]
    parts = basename.replace(".ndjson", "").rsplit("-", 2)
    if len(parts) >= 2:
        return parts[-2]
    return None


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def build_upsert_sql(table_name, columns):
    cols = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    return f"UPSERT INTO {table_name} ({cols}) VALUES ({placeholders})"


def execute_batch_with_retry(conn, sql, batch):
    """
    Execute a batch with CockroachDB transaction retry logic.

    CockroachDB uses serializable isolation and may return
    RETRY_SERIALIZABLE when transactions conflict. The recommended
    client-side pattern is exponential backoff with jitter.
    """
    for attempt in range(MAX_RETRIES):
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, sql, batch)
            conn.commit()
            return
        except psycopg2.errors.SerializationFailure:
            conn.rollback()
            if attempt < MAX_RETRIES - 1:
                sleep_time = RETRY_BASE_DELAY * (2 ** attempt)
                log.debug("Retry %d/%d after %.2fs", attempt + 1, MAX_RETRIES, sleep_time)
                time.sleep(sleep_time)
            else:
                log.error("Max retries (%d) exhausted for batch", MAX_RETRIES)
                raise
        except Exception:
            conn.rollback()
            raise


def load_table(s3_client, conn, table_name, columns, files):
    """Load all ndjson files for a single table into the target cluster."""
    upsert_sql = build_upsert_sql(table_name, columns)

    total_rows = 0
    skipped_deletes = 0

    for file_key in files:
        file_name = file_key.rsplit("/", 1)[-1]
        log.info("  %s", file_name)
        batch = []
        file_rows = 0

        response = s3_client.get_object(Bucket=S3_BUCKET, Key=file_key)
        for line in response["Body"].iter_lines():
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                log.warning("  Skipping malformed JSON: %s", e)
                continue

            after = record.get("after")
            if after is None:
                skipped_deletes += 1
                continue

            # Extract values in column order; use None for missing keys.
            # Convert dicts/lists to JSON strings (JSONB columns arrive
            # as native Python objects from json.loads but psycopg2 needs strings).
            values = []
            for c in columns:
                v = after.get(c)
                if isinstance(v, (dict, list)):
                    v = json.dumps(v)
                values.append(v)
            batch.append(values)
            file_rows += 1

            if len(batch) >= BATCH_SIZE:
                execute_batch_with_retry(conn, upsert_sql, batch)
                batch = []

        if batch:
            execute_batch_with_retry(conn, upsert_sql, batch)

        total_rows += file_rows
        log.info("    %d rows (cumulative: %d)", file_rows, total_rows)

    if skipped_deletes:
        log.info("  Skipped %d delete events (expected during initial load)", skipped_deletes)

    return total_rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 65)
    print("  CockroachDB S3 Changefeed → Initial Load (Schema-Agnostic)")
    print("=" * 65)

    log.info("Batch size: %d | Max retries: %d | Backoff base: %.2fs",
             BATCH_SIZE, MAX_RETRIES, RETRY_BASE_DELAY)

    # 1. Connect to target and auto-discover schema
    log.info("Connecting to target cluster...")
    conn = psycopg2.connect(TARGET_DSN)
    conn.autocommit = False
    log.info("Connected")

    table_columns = discover_table_columns(conn, TARGET_SCHEMA)
    fk_order = discover_fk_order(conn, TARGET_SCHEMA)

    # 2. List changefeed files from S3
    log.info("Connecting to S3 (bucket=%s, prefix=%s)...", S3_BUCKET, S3_PREFIX)
    s3_client = get_s3_client()
    files_by_table = list_ndjson_files(s3_client)

    if not files_by_table:
        log.error("No ndjson files found in s3://%s/%s", S3_BUCKET, S3_PREFIX)
        sys.exit(1)

    log.info("Found data files for %d table(s):", len(files_by_table))
    for t, f in sorted(files_by_table.items()):
        marker = "OK" if t in table_columns else "SKIP (not in target schema)"
        log.info("  %-40s %d file(s)  [%s]", t, len(f), marker)

    # 3. Load each table in FK-safe order (parents before children)
    grand_total = 0
    start_time = time.time()

    # Build ordered list: FK-ordered tables first, then any remaining
    load_order = [t for t in fk_order if t in files_by_table]
    for t in sorted(files_by_table.keys()):
        if t not in load_order:
            load_order.append(t)

    for table_name in load_order:
        files = files_by_table[table_name]
        if table_name not in table_columns:
            log.warning("Table '%s' not found in target schema, skipping", table_name)
            continue

        columns = table_columns[table_name]
        log.info("")
        log.info("--- %s (%d files, %d columns) ---", table_name, len(files), len(columns))

        table_start = time.time()
        count = load_table(s3_client, conn, table_name, columns, files)
        elapsed = time.time() - table_start
        grand_total += count
        log.info("  Done: %d rows in %.1fs (%.0f rows/s)",
                 count, elapsed, count / elapsed if elapsed > 0 else 0)

    total_elapsed = time.time() - start_time
    print()
    print("=" * 65)
    log.info("INITIAL LOAD COMPLETE")
    log.info("Total rows: %d | Time: %.1fs | Avg: %.0f rows/s",
             grand_total, total_elapsed,
             grand_total / total_elapsed if total_elapsed > 0 else 0)
    print("=" * 65)

    # 4. Validate row counts
    log.info("")
    log.info("Row count validation on target:")
    with conn.cursor() as cur:
        for table_name in sorted(files_by_table.keys()):
            if table_name in table_columns:
                cur.execute(f"SELECT count(*) FROM {table_name}")
                count = cur.fetchone()[0]
                log.info("  %-40s %d rows", table_name, count)

    conn.close()
    log.info("Done.")


if __name__ == "__main__":
    main()
