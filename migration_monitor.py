#!/usr/bin/env python3
"""
Migration Monitor — Row Count Comparison & CDC Status

Compares row counts between source and target CockroachDB clusters,
shows CDC consumer status, and displays per-table migration progress.

Configuration: Set via environment variables or edit the defaults below.

Usage:
  python3 migration_monitor.py              # One-shot report
  python3 migration_monitor.py --watch      # Refresh every 30s
  python3 migration_monitor.py --watch 10   # Refresh every 10s
"""

import psycopg2
import json
import os
import sys
import time
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Configuration — override via environment variables
# ---------------------------------------------------------------------------
SOURCE_DSN = os.environ.get(
    "SOURCE_DSN",
    "postgresql://user:password@source-host:26257/dbname?sslmode=verify-full",
)

TARGET_DSN = os.environ.get(
    "TARGET_DSN",
    "postgresql://user:password@target-host:26257/dbname?sslmode=verify-full",
)

STATE_FILE = os.environ.get(
    "STATE_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "cdc_consumer_state.json"),
)

TARGET_SCHEMA = os.environ.get("TARGET_SCHEMA", "public")


def get_table_counts(dsn, schema="public"):
    """Get row counts for all tables in a schema."""
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    counts = {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
            "ORDER BY table_name",
            (schema,),
        )
        tables = [row[0] for row in cur.fetchall()]

        for table in tables:
            cur.execute(f"SELECT count(*) FROM {table}")
            counts[table] = cur.fetchone()[0]

    conn.close()
    return counts


def load_cdc_state():
    """Load CDC consumer state file if it exists."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return None


def get_changefeed_status(dsn):
    """Get changefeed job status from the source cluster."""
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(
                "SELECT job_id, status, running_status, "
                "created::STRING, modified::STRING "
                "FROM [SHOW CHANGEFEED JOBS] "
                "ORDER BY created DESC LIMIT 1"
            )
            row = cur.fetchone()
        conn.close()
        if row:
            return {
                "job_id": str(row[0]),
                "status": row[1],
                "running_status": row[2],
                "created": row[3],
                "modified": row[4],
            }
    except Exception as e:
        return {"error": str(e)}
    return None


def print_report():
    """Print the full migration status report."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    print()
    print("=" * 78)
    print(f"  Migration Monitor Report — {now}")
    print("=" * 78)

    # --- Changefeed status ---
    print()
    print("CHANGEFEED STATUS (Source Cluster)")
    print("-" * 78)
    cf = get_changefeed_status(SOURCE_DSN)
    if cf and "error" not in cf:
        status_icon = "RUNNING" if cf["status"] == "running" else cf["status"].upper()
        print(f"  Job ID:         {cf['job_id']}")
        print(f"  Status:         {status_icon}")
        print(f"  Running Status: {cf.get('running_status', 'N/A')}")
        print(f"  Created:        {cf['created']}")
        print(f"  Last Modified:  {cf['modified']}")
    elif cf and "error" in cf:
        print(f"  Error querying changefeed: {cf['error']}")
    else:
        print("  No changefeed jobs found")

    # --- CDC consumer status ---
    print()
    print("CDC CONSUMER STATUS")
    print("-" * 78)
    state = load_cdc_state()
    if state:
        files_processed = len(state.get("processed_files", []))
        last_resolved = state.get("last_resolved", "N/A")
        started_at = state.get("started_at", "N/A")
        last_activity = state.get("last_activity", "N/A")
        errors = state.get("errors", [])
        table_stats = state.get("table_stats", {})

        print(f"  Started at:       {started_at}")
        print(f"  Last activity:    {last_activity}")
        print(f"  Files processed:  {files_processed}")
        print(f"  Last resolved:    {last_resolved}")
        print(f"  Recent errors:    {len(errors)}")

        if table_stats:
            print()
            print("  Per-table CDC stats:")
            print(f"  {'Table':<45} {'Upserts':>8} {'Deletes':>8} {'Files':>6} {'Errors':>7}")
            print(f"  {'-'*45} {'-'*8} {'-'*8} {'-'*6} {'-'*7}")
            total_u, total_d, total_f, total_e = 0, 0, 0, 0
            for t in sorted(table_stats.keys()):
                s = table_stats[t]
                u = s.get("upserts", 0)
                d = s.get("deletes", 0)
                f = s.get("files", 0)
                e = s.get("errors", 0)
                total_u += u
                total_d += d
                total_f += f
                total_e += e
                print(f"  {t:<45} {u:>8} {d:>8} {f:>6} {e:>7}")
            print(f"  {'-'*45} {'-'*8} {'-'*8} {'-'*6} {'-'*7}")
            print(f"  {'TOTAL':<45} {total_u:>8} {total_d:>8} {total_f:>6} {total_e:>7}")

        if errors:
            print()
            print("  Last 5 errors:")
            for err in errors[-5:]:
                print(f"    [{err.get('time', '?')}] {err.get('table', '?')}: {err.get('error', '?')[:80]}")
    else:
        print("  No state file found. CDC consumer has not run yet or state file was reset.")

    # --- Row count comparison ---
    print()
    print("ROW COUNT COMPARISON (Source vs Target)")
    print("-" * 78)
    print(f"  {'Table':<45} {'Source':>8} {'Target':>8} {'Diff':>8} {'Status':>8}")
    print(f"  {'-'*45} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")

    try:
        source_counts = get_table_counts(SOURCE_DSN, TARGET_SCHEMA)
    except Exception as e:
        print(f"  Error connecting to source: {e}")
        source_counts = {}

    try:
        target_counts = get_table_counts(TARGET_DSN, TARGET_SCHEMA)
    except Exception as e:
        print(f"  Error connecting to target: {e}")
        target_counts = {}

    all_tables = sorted(set(list(source_counts.keys()) + list(target_counts.keys())))
    total_source = 0
    total_target = 0
    all_match = True

    for table in all_tables:
        src = source_counts.get(table, 0)
        tgt = target_counts.get(table, 0)
        diff = tgt - src
        total_source += src
        total_target += tgt

        if diff == 0:
            status = "OK"
        elif diff > 0:
            status = f"+{diff}"
            all_match = False
        else:
            status = str(diff)
            all_match = False

        if table not in source_counts:
            status = "NO SRC"
            all_match = False
        elif table not in target_counts:
            status = "NO TGT"
            all_match = False

        print(f"  {table:<45} {src:>8} {tgt:>8} {diff:>+8} {status:>8}")

    print(f"  {'-'*45} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")
    total_diff = total_target - total_source
    print(f"  {'TOTAL':<45} {total_source:>8} {total_target:>8} {total_diff:>+8}")

    # --- Summary ---
    print()
    print("MIGRATION SUMMARY")
    print("-" * 78)
    if all_match:
        print("  Status: ALL TABLES IN SYNC")
    else:
        mismatched = sum(
            1 for t in all_tables
            if source_counts.get(t, -1) != target_counts.get(t, -2)
        )
        print(f"  Status: {mismatched} table(s) have row count differences")
        print("  Note: Small differences are expected while CDC is still processing")

    print("=" * 78)
    print()


def main():
    watch_mode = "--watch" in sys.argv
    interval = 30

    if watch_mode:
        # Check if a custom interval was provided
        idx = sys.argv.index("--watch")
        if idx + 1 < len(sys.argv):
            try:
                interval = int(sys.argv[idx + 1])
            except ValueError:
                pass

    if watch_mode:
        try:
            while True:
                # Clear screen
                print("\033[2J\033[H", end="")
                print_report()
                print(f"  Auto-refreshing every {interval}s. Press Ctrl+C to stop.")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\nStopped.")
    else:
        print_report()


if __name__ == "__main__":
    main()
