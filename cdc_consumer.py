#!/usr/bin/env python3
"""
CDC Consumer for CockroachDB S3 Changefeed (Schema-Agnostic)

Continuously polls S3 for new changefeed ndjson files and applies changes
(UPSERT/DELETE) to a target CockroachDB cluster. Automatically discovers
table schemas and primary keys from the target database — no hardcoded
table/column mappings required.

Configuration: Set via environment variables or edit the defaults below.

Usage:
  export TARGET_DSN="postgresql://user:pass@host:26257/db?sslmode=verify-full"
  export S3_BUCKET="my-migration-bucket"
  nohup python3 -u cdc_consumer.py > cdc_consumer.log 2>&1 &

Press Ctrl+C to stop gracefully.
"""

import boto3
import psycopg2
import psycopg2.extras
import psycopg2.errors
import json
import os
import time
import signal
import logging
from collections import defaultdict
from datetime import datetime

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

STATE_FILE = os.environ.get(
    "STATE_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "cdc_consumer_state.json"),
)

# Schema to load from (default: public)
TARGET_SCHEMA = os.environ.get("TARGET_SCHEMA", "public")

# --- Tuning knobs ---
# How often to poll S3 for new files (seconds)
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "10"))

# Rows per UPSERT batch (see guide for sizing recommendations)
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))

# Max retries on RETRY_SERIALIZABLE
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "5"))

# Base delay for exponential backoff (seconds)
RETRY_BASE_DELAY = float(os.environ.get("RETRY_BASE_DELAY", "0.1"))

# How often to print idle status (multiples of POLL_INTERVAL)
STATUS_EVERY_N_POLLS = int(os.environ.get("STATUS_EVERY_N_POLLS", "6"))

# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("cdc")

# Graceful shutdown
shutdown_requested = False


def signal_handler(sig, frame):
    global shutdown_requested
    log.info("Shutdown requested, finishing current batch...")
    shutdown_requested = True


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ---------------------------------------------------------------------------
# Schema auto-discovery
# ---------------------------------------------------------------------------
def discover_table_columns(conn, schema="public"):
    """
    Discover all table → [columns] from information_schema.
    Columns ordered by ordinal_position.
    """
    sql = """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = %s
        ORDER BY table_name, ordinal_position
    """
    result = defaultdict(list)
    with conn.cursor() as cur:
        cur.execute(sql, (schema,))
        for table_name, col_name in cur.fetchall():
            result[table_name].append(col_name)
    log.info("Discovered %d table(s) from information_schema", len(result))
    return dict(result)


def discover_primary_keys(conn, schema="public"):
    """
    Discover primary key columns for each table.
    Returns {table_name: [pk_col1, pk_col2, ...]}.
    """
    sql = """
        SELECT kcu.table_name, kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = %s
        ORDER BY kcu.table_name, kcu.ordinal_position
    """
    result = defaultdict(list)
    with conn.cursor() as cur:
        cur.execute(sql, (schema,))
        for table_name, col_name in cur.fetchall():
            result[table_name].append(col_name)
    return dict(result)


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------
def get_s3_client():
    session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
    return session.client("s3")


def extract_table_name(s3_key):
    """Extract table name from changefeed ndjson filename."""
    basename = s3_key.rsplit("/", 1)[-1]
    parts = basename.replace(".ndjson", "").rsplit("-", 2)
    if len(parts) >= 2:
        return parts[-2]
    return None


def list_new_ndjson_files(s3_client, processed_set):
    new_files = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".ndjson") and key not in processed_set:
                new_files.append(key)
    new_files.sort()
    return new_files


def get_latest_resolved(s3_client):
    resolved = None
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".RESOLVED"):
                if resolved is None or key > resolved:
                    resolved = key
    return resolved


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            data = json.load(f)
        # Ensure new fields exist for backward compat
        data.setdefault("table_stats", {})
        data.setdefault("errors", [])
        data.setdefault("started_at", None)
        data.setdefault("last_activity", None)
        return data
    return {
        "processed_files": [],
        "last_resolved": None,
        "table_stats": {},
        "errors": [],
        "started_at": None,
        "last_activity": None,
    }


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ---------------------------------------------------------------------------
# Retry helpers
# ---------------------------------------------------------------------------
def execute_batch_with_retry(conn, sql, batch):
    for attempt in range(MAX_RETRIES):
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, sql, batch)
            conn.commit()
            return
        except psycopg2.errors.SerializationFailure:
            conn.rollback()
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BASE_DELAY * (2 ** attempt))
            else:
                raise
        except Exception:
            conn.rollback()
            raise


def execute_delete_with_retry(conn, sql, params):
    for attempt in range(MAX_RETRIES):
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.commit()
            return
        except psycopg2.errors.SerializationFailure:
            conn.rollback()
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BASE_DELAY * (2 ** attempt))
            else:
                raise
        except Exception:
            conn.rollback()
            raise


# ---------------------------------------------------------------------------
# File processing
# ---------------------------------------------------------------------------
def process_file(s3_client, conn, file_key, table_columns, table_pks):
    """Process a single ndjson file, applying changes to the target."""
    table_name = extract_table_name(file_key)
    if not table_name or table_name not in table_columns:
        log.warning("  Skipping unknown table in: %s", file_key)
        return 0, 0

    columns = table_columns[table_name]
    pk_cols = table_pks.get(table_name, ["id"])

    upsert_sql = (
        f"UPSERT INTO {table_name} ("
        + ", ".join(f'"{c}"' for c in columns)
        + ") VALUES ("
        + ", ".join(["%s"] * len(columns))
        + ")"
    )
    delete_sql = (
        f"DELETE FROM {table_name} WHERE "
        + " AND ".join(f'"{c}" = %s' for c in pk_cols)
    )

    response = s3_client.get_object(Bucket=S3_BUCKET, Key=file_key)
    upsert_batch = []
    upserts = 0
    deletes = 0

    for line in response["Body"].iter_lines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        after = record.get("after")
        key_data = record.get("key")

        if after is None:
            if key_data:
                execute_delete_with_retry(conn, delete_sql, key_data)
                deletes += 1
        else:
            values = []
            for c in columns:
                v = after.get(c)
                if isinstance(v, (dict, list)):
                    v = json.dumps(v)
                values.append(v)
            upsert_batch.append(values)
            upserts += 1

            if len(upsert_batch) >= BATCH_SIZE:
                execute_batch_with_retry(conn, upsert_sql, upsert_batch)
                upsert_batch = []

    if upsert_batch:
        execute_batch_with_retry(conn, upsert_sql, upsert_batch)

    return upserts, deletes


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    print("=" * 65)
    print("  CockroachDB CDC Consumer (S3 Changefeed, Schema-Agnostic)")
    print("=" * 65)
    log.info("Poll interval: %ds | Batch size: %d | Max retries: %d",
             POLL_INTERVAL, BATCH_SIZE, MAX_RETRIES)
    log.info("State file: %s", STATE_FILE)

    # 1. Connect and discover schema
    log.info("Connecting to target cluster...")
    conn = psycopg2.connect(TARGET_DSN)
    conn.autocommit = False
    log.info("Connected")

    table_columns = discover_table_columns(conn, TARGET_SCHEMA)
    table_pks = discover_primary_keys(conn, TARGET_SCHEMA)

    for t in table_columns:
        pk = table_pks.get(t, ["id"])
        log.info("  %-40s %d cols, PK=(%s)", t, len(table_columns[t]), ", ".join(pk))

    # 2. Load state
    s3_client = get_s3_client()
    state = load_state()
    processed_set = set(state["processed_files"])
    log.info("Previously processed files: %d", len(processed_set))

    total_upserts = 0
    total_deletes = 0
    poll_count = 0
    error_count = 0

    if state["started_at"] is None:
        state["started_at"] = datetime.utcnow().isoformat() + "Z"

    # 3. Poll loop
    log.info("Entering poll loop (Ctrl+C to stop)...\n")
    while not shutdown_requested:
        poll_count += 1
        new_files = list_new_ndjson_files(s3_client, processed_set)

        if new_files:
            log.info("[Poll #%d] Found %d new file(s)", poll_count, len(new_files))

            for file_key in new_files:
                if shutdown_requested:
                    break

                table_name = extract_table_name(file_key) or "unknown"
                file_name = file_key.rsplit("/", 1)[-1]
                log.info("  [%s] %s", table_name, file_name)

                try:
                    upserts, deletes = process_file(
                        s3_client, conn, file_key, table_columns, table_pks
                    )
                    processed_set.add(file_key)
                    state["processed_files"].append(file_key)
                    total_upserts += upserts
                    total_deletes += deletes

                    # Track per-table stats
                    if table_name not in state["table_stats"]:
                        state["table_stats"][table_name] = {
                            "upserts": 0, "deletes": 0, "files": 0, "errors": 0
                        }
                    state["table_stats"][table_name]["upserts"] += upserts
                    state["table_stats"][table_name]["deletes"] += deletes
                    state["table_stats"][table_name]["files"] += 1
                    state["last_activity"] = datetime.utcnow().isoformat() + "Z"

                    if upserts or deletes:
                        log.info("    Applied: %d upserts, %d deletes", upserts, deletes)
                except Exception as e:
                    log.error("    ERROR: %s", e)
                    error_count += 1
                    if table_name not in state["table_stats"]:
                        state["table_stats"][table_name] = {
                            "upserts": 0, "deletes": 0, "files": 0, "errors": 0
                        }
                    state["table_stats"][table_name]["errors"] += 1
                    state["errors"].append({
                        "time": datetime.utcnow().isoformat() + "Z",
                        "file": file_key,
                        "table": table_name,
                        "error": str(e)[:200],
                    })
                    # Keep only last 50 errors
                    state["errors"] = state["errors"][-50:]
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = psycopg2.connect(TARGET_DSN)
                    conn.autocommit = False
                    # Re-discover schema in case of reconnect
                    table_columns = discover_table_columns(conn, TARGET_SCHEMA)
                    table_pks = discover_primary_keys(conn, TARGET_SCHEMA)

            resolved = get_latest_resolved(s3_client)
            if resolved:
                state["last_resolved"] = resolved
            save_state(state)

        else:
            if poll_count % STATUS_EVERY_N_POLLS == 0:
                log.info(
                    "Waiting for changes... (total: %d upserts, %d deletes, %d errors)",
                    total_upserts, total_deletes, error_count,
                )

        if not shutdown_requested:
            time.sleep(POLL_INTERVAL)

    # Shutdown
    log.info("")
    log.info("Shutting down.")
    log.info("Total: %d upserts, %d deletes across %d files",
             total_upserts, total_deletes, len(processed_set))
    save_state(state)
    conn.close()
    log.info("Done.")


if __name__ == "__main__":
    main()
