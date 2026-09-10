#!/usr/bin/env python3
"""
Garmin Data Query & Analytics CLI Tool.

Provides fast querying, statistical summaries, correlation analyses, and exports
from the local SQLite Garmin database (`garmin_db/garmin.db`).
"""

import sqlite3
import argparse
from pathlib import Path
from typing import List, Dict, Any

DB_PATH = Path("/Users/agrismarkus/ag/AG_VES/ag_ves_dati/garmin_db/garmin.db")


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found at {db_path}. Please run garmin_sync.py first.")
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def print_table(rows: List[sqlite3.Row], title: str = None, limit: int = 50):
    if not rows:
        print("No records found.")
        return

    if title:
        print(f"\n=== {title} ===")

    headers = rows[0].keys()
    col_widths = {h: len(h) for h in headers}

    data_rows = []
    for r in rows[:limit]:
        formatted = []
        for h in headers:
            val = r[h]
            if isinstance(val, float):
                val_str = f"{val:.2f}"
            elif val is None:
                val_str = "-"
            else:
                val_str = str(val)
            col_widths[h] = max(col_widths[h], len(val_str))
            formatted.append(val_str)
        data_rows.append(formatted)

    # Print Header
    header_line = " | ".join(h.ljust(col_widths[h]) for h in headers)
    sep_line = "-+-".join("-" * col_widths[h] for h in headers)
    print(header_line)
    print(sep_line)

    # Print Rows
    for r in data_rows:
        print(" | ".join(r[i].ljust(col_widths[headers[i]]) for i in range(len(headers))))

    if len(rows) > limit:
        print(f"... and {len(rows) - limit} more rows.")


def show_summary(conn: sqlite3.Connection):
    cursor = conn.cursor()
    print("\n==================================================")
    print("📊 GARMIN DATABASE OVERVIEW & RECORD COUNTS")
    print("==================================================")

    tables = [
        ("activities", "Total Activities"),
        ("body_composition", "Scale / Weigh-in Records"),
        ("daily_summaries", "Daily Tracking Days"),
        ("sleep_records", "Sleep Nights"),
        ("hrv_records", "HRV Records"),
        ("training_readiness", "Training Readiness Days"),
        ("training_status", "Training Status Records"),
        ("vo2_max_trend", "VO2 Max Trend Points"),
        ("personal_records", "Personal Records"),
        ("badges", "Earned Badges"),
        ("devices_and_gear", "Devices & Gear Items")
    ]

    for table, label in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) as cnt FROM {table}")
            cnt = cursor.fetchone()["cnt"]
            cursor.execute(f"SELECT MIN(calendar_date) as min_d, MAX(calendar_date) as max_d FROM {table}")
            d_row = cursor.fetchone()
            d_range = f"({d_row['min_d']} to {d_row['max_d']})" if d_row and d_row['min_d'] else ""
            print(f"  • {label:28s}: {cnt:5d} records {d_range}")
        except Exception:
            try:
                cursor.execute(f"SELECT COUNT(*) as cnt FROM {table}")
                cnt = cursor.fetchone()["cnt"]
                print(f"  • {label:28s}: {cnt:5d} records")
            except Exception:
                pass

    print("\n--- Activity Breakdown by Sport Type ---")
    cursor.execute("""
    SELECT 
        activity_type,
        COUNT(*) as total_count,
        ROUND(SUM(distance_m)/1000.0, 1) as total_km,
        ROUND(SUM(duration_s)/3600.0, 1) as total_hours,
        ROUND(AVG(avg_hr), 0) as avg_hr,
        ROUND(SUM(calories), 0) as total_kcal
    FROM activities
    GROUP BY activity_type
    ORDER BY total_count DESC
    """)
    print_table(cursor.fetchall())


def show_vo2max(conn: sqlite3.Connection):
    cursor = conn.cursor()
    print("\n=== VO2 MAX MONTHLY STATS & TREND ===")
    cursor.execute("""
    SELECT 
        substr(start_time_local, 1, 7) as month,
        COUNT(vo2_max) as measurements,
        ROUND(MIN(vo2_max), 1) as min_vo2,
        ROUND(MAX(vo2_max), 1) as max_vo2,
        ROUND(AVG(vo2_max), 1) as avg_vo2
    FROM activities
    WHERE vo2_max IS NOT NULL
    GROUP BY month
    ORDER BY month ASC
    """)
    print_table(cursor.fetchall())


def show_recent_runs(conn: sqlite3.Connection, limit: int = 15):
    cursor = conn.cursor()
    cursor.execute("""
    SELECT 
        substr(start_time_local, 1, 10) as date,
        activity_name,
        ROUND(distance_m / 1000.0, 2) as km,
        ROUND(duration_s / 60.0, 1) as min,
        printf('%d:%02d', CAST(duration_s / (distance_m / 1000.0) / 60 AS INT), CAST((duration_s / (distance_m / 1000.0)) % 60 AS INT)) as pace_min_km,
        ROUND(avg_hr, 0) as avg_hr,
        ROUND(max_hr, 0) as max_hr,
        ROUND(aerobic_training_effect, 1) as aer_te,
        ROUND(vo2_max, 1) as vo2_max
    FROM activities
    WHERE activity_type = 'running'
    ORDER BY start_time_local DESC
    LIMIT ?
    """, (limit,))
    print_table(cursor.fetchall(), title=f"Last {limit} Running Activities")


def execute_sql(conn: sqlite3.Connection, query: str):
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    print_table(rows, title=f"Query Results ({len(rows)} rows)")


def main():
    parser = argparse.ArgumentParser(description="Query Garmin Database")
    parser.add_argument("--summary", action="store_true", help="Show overview and record counts")
    parser.add_argument("--vo2max", action="store_true", help="Show VO2 max monthly progression")
    parser.add_argument("--runs", action="store_true", help="Show recent running activities")
    parser.add_argument("--sql", type=str, help="Run custom SQL query")
    args = parser.parse_args()

    conn = get_connection()

    if args.sql:
        execute_sql(conn, args.sql)
    elif args.vo2max:
        show_vo2max(conn)
    elif args.runs:
        show_recent_runs(conn)
    else:
        show_summary(conn)


if __name__ == "__main__":
    main()
