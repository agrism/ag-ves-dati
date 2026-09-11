#!/usr/bin/env python3
"""
Garmin Data Query & Analytics CLI Tool.

Provides fast querying, statistical summaries, correlation analyses, and exports
from either MySQL (Hetzner / remote production) or SQLite (local database).
"""

import sys
import argparse
from pathlib import Path
from typing import List, Dict, Any

# Import unified DB adapter
sys.path.insert(0, str(Path(__file__).resolve().parent))
from db_adapter import get_db, DBConnection, DB_TYPE


def print_table(rows: List[Any], title: str = None, limit: int = 50):
    if not rows:
        print("No records found.")
        return

    if title:
        print(f"\n=== {title} ===")

    # Handle both sqlite3.Row and pymysql dicts
    first_row = rows[0]
    if isinstance(first_row, dict):
        headers = list(first_row.keys())
    else:
        headers = first_row.keys()

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


def show_summary(db: DBConnection):
    print("\n==================================================")
    print(f"📊 GARMIN DATABASE OVERVIEW & RECORD COUNTS [{db.db_type.upper()}]")
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
            cur = db.execute(f"SELECT COUNT(*) as cnt FROM {table}")
            cnt_row = cur.fetchone()
            cnt = cnt_row["cnt"] if isinstance(cnt_row, dict) else cnt_row[0]

            cur = db.execute(f"SELECT MIN(calendar_date) as min_d, MAX(calendar_date) as max_d FROM {table}")
            d_row = cur.fetchone()
            if d_row and d_row["min_d"]:
                d_range = f"({d_row['min_d']} to {d_row['max_d']})"
            else:
                d_range = ""
            print(f"  • {label:28s}: {cnt:5d} records {d_range}")
        except Exception:
            try:
                cur = db.execute(f"SELECT COUNT(*) as cnt FROM {table}")
                cnt_row = cur.fetchone()
                cnt = cnt_row["cnt"] if isinstance(cnt_row, dict) else cnt_row[0]
                print(f"  • {label:28s}: {cnt:5d} records")
            except Exception:
                pass

    print("\n--- Activity Breakdown by Sport Type ---")
    cur = db.execute("""
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
    print_table(cur.fetchall())


def show_vo2max(db: DBConnection):
    print("\n=== VO2 MAX MONTHLY STATS & TREND ===")
    cur = db.execute("""
    SELECT 
        SUBSTRING(start_time_local, 1, 7) as month,
        COUNT(vo2_max) as measurements,
        ROUND(MIN(vo2_max), 1) as min_vo2,
        ROUND(MAX(vo2_max), 1) as max_vo2,
        ROUND(AVG(vo2_max), 1) as avg_vo2
    FROM activities
    WHERE vo2_max IS NOT NULL
    GROUP BY SUBSTRING(start_time_local, 1, 7)
    ORDER BY month ASC
    """)
    print_table(cur.fetchall())


def show_recent_runs(db: DBConnection, limit: int = 15):
    cur = db.execute("""
    SELECT 
        SUBSTRING(start_time_local, 1, 10) as date,
        activity_name,
        ROUND(distance_m / 1000.0, 2) as km,
        ROUND(duration_s / 60.0, 1) as min,
        ROUND(avg_hr, 0) as avg_hr,
        ROUND(max_hr, 0) as max_hr,
        ROUND(aerobic_training_effect, 1) as aer_te,
        ROUND(vo2_max, 1) as vo2_max
    FROM activities
    WHERE activity_type = 'running'
    ORDER BY start_time_local DESC
    LIMIT ?
    """, (limit,))
    print_table(cur.fetchall(), title=f"Last {limit} Running Activities")


def execute_sql(db: DBConnection, query: str):
    cur = db.execute(query)
    rows = cur.fetchall()
    print_table(rows, title=f"Query Results ({len(rows)} rows)")


def main():
    parser = argparse.ArgumentParser(description="Query Garmin Database (MySQL / SQLite)")
    parser.add_argument("--summary", action="store_true", help="Show overview and record counts")
    parser.add_argument("--vo2max", action="store_true", help="Show VO2 max monthly progression")
    parser.add_argument("--runs", action="store_true", help="Show recent running activities")
    parser.add_argument("--sql", type=str, help="Run custom SQL query")
    args = parser.parse_args()

    db = get_db()

    try:
        if args.sql:
            execute_sql(db, args.sql)
        elif args.vo2max:
            show_vo2max(db)
        elif args.runs:
            show_recent_runs(db)
        else:
            show_summary(db)
    finally:
        db.close()


if __name__ == "__main__":
    main()
