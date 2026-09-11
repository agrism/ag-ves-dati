#!/usr/bin/env python3
"""
Export SQLite Garmin Database to a Standard MySQL 8.0/5.7+ Dump File.

Features:
- Dumps schema with correct MySQL data types, indexes, and utf8mb4 encoding.
- Dumps data using multi-row INSERT statements (batched for performance).
- Automatically escapes text, quotes, backslashes, and JSON payloads.
- Generates both plain SQL (.sql) and compressed Gzip (.sql.gz) dump files.
"""

import os
import sys
import gzip
import sqlite3
from pathlib import Path
from typing import List, Dict, Any

WORKSPACE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = WORKSPACE_DIR / "garmin_db" / "garmin.db"
OUTPUT_SQL_PATH = WORKSPACE_DIR / "garmin_db" / "garmin_mysql_dump.sql"
OUTPUT_GZ_PATH = WORKSPACE_DIR / "garmin_db" / "garmin_mysql_dump.sql.gz"

# MySQL Table Definitions
MYSQL_SCHEMAS = {
    "activities": """
CREATE TABLE IF NOT EXISTS `activities` (
  `activity_id` BIGINT NOT NULL,
  `activity_name` VARCHAR(255) DEFAULT NULL,
  `activity_type` VARCHAR(100) DEFAULT NULL,
  `sport_type_id` INT DEFAULT NULL,
  `start_time_local` VARCHAR(50) DEFAULT NULL,
  `start_time_gmt` VARCHAR(50) DEFAULT NULL,
  `distance_m` DOUBLE DEFAULT NULL,
  `duration_s` DOUBLE DEFAULT NULL,
  `moving_duration_s` DOUBLE DEFAULT NULL,
  `elapsed_duration_s` DOUBLE DEFAULT NULL,
  `elevation_gain_m` DOUBLE DEFAULT NULL,
  `elevation_loss_m` DOUBLE DEFAULT NULL,
  `avg_speed_mps` DOUBLE DEFAULT NULL,
  `max_speed_mps` DOUBLE DEFAULT NULL,
  `avg_hr` DOUBLE DEFAULT NULL,
  `max_hr` DOUBLE DEFAULT NULL,
  `avg_cadence` DOUBLE DEFAULT NULL,
  `max_cadence` DOUBLE DEFAULT NULL,
  `calories` DOUBLE DEFAULT NULL,
  `bmr_calories` DOUBLE DEFAULT NULL,
  `aerobic_training_effect` DOUBLE DEFAULT NULL,
  `anaerobic_training_effect` DOUBLE DEFAULT NULL,
  `vo2_max` DOUBLE DEFAULT NULL,
  `avg_power` DOUBLE DEFAULT NULL,
  `max_power` DOUBLE DEFAULT NULL,
  `norm_power` DOUBLE DEFAULT NULL,
  `training_load` DOUBLE DEFAULT NULL,
  `location_name` VARCHAR(255) DEFAULT NULL,
  `gear_pk` BIGINT DEFAULT NULL,
  `raw_json` LONGTEXT DEFAULT NULL,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`activity_id`),
  KEY `idx_activities_start_time` (`start_time_local`),
  KEY `idx_activities_type` (`activity_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
""",

    "body_composition": """
CREATE TABLE IF NOT EXISTS `body_composition` (
  `sample_pk` BIGINT NOT NULL,
  `date_timestamp` BIGINT DEFAULT NULL,
  `calendar_date` VARCHAR(20) DEFAULT NULL,
  `weight_kg` DOUBLE DEFAULT NULL,
  `bmi` DOUBLE DEFAULT NULL,
  `body_fat_pct` DOUBLE DEFAULT NULL,
  `body_water_pct` DOUBLE DEFAULT NULL,
  `bone_mass_kg` DOUBLE DEFAULT NULL,
  `muscle_mass_kg` DOUBLE DEFAULT NULL,
  `physique_rating` DOUBLE DEFAULT NULL,
  `visceral_fat` DOUBLE DEFAULT NULL,
  `metabolic_age` DOUBLE DEFAULT NULL,
  `source_type` VARCHAR(50) DEFAULT NULL,
  `raw_json` LONGTEXT DEFAULT NULL,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`sample_pk`),
  KEY `idx_body_comp_date` (`calendar_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
""",

    "daily_summaries": """
CREATE TABLE IF NOT EXISTS `daily_summaries` (
  `calendar_date` VARCHAR(20) NOT NULL,
  `total_steps` INT DEFAULT NULL,
  `step_goal` INT DEFAULT NULL,
  `total_distance_m` DOUBLE DEFAULT NULL,
  `active_calories` DOUBLE DEFAULT NULL,
  `bmr_calories` DOUBLE DEFAULT NULL,
  `total_calories` DOUBLE DEFAULT NULL,
  `floors_climbed` INT DEFAULT NULL,
  `floors_goal` INT DEFAULT NULL,
  `resting_hr` INT DEFAULT NULL,
  `min_hr` INT DEFAULT NULL,
  `max_hr` INT DEFAULT NULL,
  `avg_stress` INT DEFAULT NULL,
  `max_stress` INT DEFAULT NULL,
  `stress_duration_s` INT DEFAULT NULL,
  `rest_stress_duration_s` INT DEFAULT NULL,
  `low_stress_duration_s` INT DEFAULT NULL,
  `med_stress_duration_s` INT DEFAULT NULL,
  `high_stress_duration_s` INT DEFAULT NULL,
  `body_battery_charged` INT DEFAULT NULL,
  `body_battery_drained` INT DEFAULT NULL,
  `body_battery_highest` INT DEFAULT NULL,
  `body_battery_lowest` INT DEFAULT NULL,
  `moderate_intensity_minutes` INT DEFAULT NULL,
  `vigorous_intensity_minutes` INT DEFAULT NULL,
  `raw_json` LONGTEXT DEFAULT NULL,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`calendar_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
""",

    "sleep_records": """
CREATE TABLE IF NOT EXISTS `sleep_records` (
  `calendar_date` VARCHAR(20) NOT NULL,
  `sleep_start_timestamp` BIGINT DEFAULT NULL,
  `sleep_end_timestamp` BIGINT DEFAULT NULL,
  `total_sleep_s` INT DEFAULT NULL,
  `deep_sleep_s` INT DEFAULT NULL,
  `light_sleep_s` INT DEFAULT NULL,
  `rem_sleep_s` INT DEFAULT NULL,
  `awake_s` INT DEFAULT NULL,
  `sleep_score` INT DEFAULT NULL,
  `sleep_score_qualifier` VARCHAR(50) DEFAULT NULL,
  `avg_sleep_hr` DOUBLE DEFAULT NULL,
  `min_sleep_hr` DOUBLE DEFAULT NULL,
  `avg_sleep_stress` DOUBLE DEFAULT NULL,
  `avg_sleep_respiration` DOUBLE DEFAULT NULL,
  `raw_json` LONGTEXT DEFAULT NULL,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`calendar_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
""",

    "hrv_records": """
CREATE TABLE IF NOT EXISTS `hrv_records` (
  `calendar_date` VARCHAR(20) NOT NULL,
  `weekly_avg_hrv` DOUBLE DEFAULT NULL,
  `last_night_avg_hrv` DOUBLE DEFAULT NULL,
  `last_night_5min_high_hrv` DOUBLE DEFAULT NULL,
  `baseline_low` DOUBLE DEFAULT NULL,
  `baseline_balanced_low` DOUBLE DEFAULT NULL,
  `baseline_balanced_upper` DOUBLE DEFAULT NULL,
  `status` VARCHAR(50) DEFAULT NULL,
  `raw_json` LONGTEXT DEFAULT NULL,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`calendar_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
""",

    "training_readiness": """
CREATE TABLE IF NOT EXISTS `training_readiness` (
  `calendar_date` VARCHAR(20) NOT NULL,
  `readiness_score` INT DEFAULT NULL,
  `readiness_level` VARCHAR(50) DEFAULT NULL,
  `sleep_score_feedback` VARCHAR(255) DEFAULT NULL,
  `recovery_time_s` INT DEFAULT NULL,
  `hrv_feedback` VARCHAR(255) DEFAULT NULL,
  `stress_history_feedback` VARCHAR(255) DEFAULT NULL,
  `raw_json` LONGTEXT DEFAULT NULL,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`calendar_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
""",

    "training_status": """
CREATE TABLE IF NOT EXISTS `training_status` (
  `calendar_date` VARCHAR(20) NOT NULL,
  `training_status_key` VARCHAR(100) DEFAULT NULL,
  `vo2_max_running` DOUBLE DEFAULT NULL,
  `vo2_max_cycling` DOUBLE DEFAULT NULL,
  `fitness_trend` VARCHAR(50) DEFAULT NULL,
  `load_status` VARCHAR(50) DEFAULT NULL,
  `acute_load` DOUBLE DEFAULT NULL,
  `optimal_load_min` DOUBLE DEFAULT NULL,
  `optimal_load_max` DOUBLE DEFAULT NULL,
  `raw_json` LONGTEXT DEFAULT NULL,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`calendar_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
""",

    "vo2_max_trend": """
CREATE TABLE IF NOT EXISTS `vo2_max_trend` (
  `calendar_date` VARCHAR(20) NOT NULL,
  `vo2_max` DOUBLE DEFAULT NULL,
  `sport` VARCHAR(50) DEFAULT NULL,
  `source` VARCHAR(100) DEFAULT NULL,
  `carried_forward` TINYINT(1) DEFAULT 0,
  `raw_json` LONGTEXT DEFAULT NULL,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`calendar_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
""",

    "personal_records": """
CREATE TABLE IF NOT EXISTS `personal_records` (
  `record_id` INT NOT NULL AUTO_INCREMENT,
  `type_key` VARCHAR(100) DEFAULT NULL,
  `activity_type` VARCHAR(100) DEFAULT NULL,
  `value` DOUBLE DEFAULT NULL,
  `formatted_value` VARCHAR(100) DEFAULT NULL,
  `activity_id` BIGINT DEFAULT NULL,
  `pr_start_time` VARCHAR(50) DEFAULT NULL,
  `raw_json` LONGTEXT DEFAULT NULL,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`record_id`),
  KEY `idx_pr_type` (`type_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
""",

    "badges": """
CREATE TABLE IF NOT EXISTS `badges` (
  `badge_id` BIGINT NOT NULL,
  `badge_key` VARCHAR(100) DEFAULT NULL,
  `badge_name` VARCHAR(255) DEFAULT NULL,
  `badge_points` INT DEFAULT NULL,
  `earned_date` VARCHAR(50) DEFAULT NULL,
  `raw_json` LONGTEXT DEFAULT NULL,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`badge_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
""",

    "devices_and_gear": """
CREATE TABLE IF NOT EXISTS `devices_and_gear` (
  `item_pk` VARCHAR(100) NOT NULL,
  `item_type` VARCHAR(50) DEFAULT NULL,
  `display_name` VARCHAR(255) DEFAULT NULL,
  `model_name` VARCHAR(255) DEFAULT NULL,
  `raw_json` LONGTEXT DEFAULT NULL,
  `updated_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`item_pk`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
""",

    "sync_log": """
CREATE TABLE IF NOT EXISTS `sync_log` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `sync_type` VARCHAR(50) DEFAULT NULL,
  `start_time` DATETIME DEFAULT NULL,
  `end_time` DATETIME DEFAULT NULL,
  `records_synced` INT DEFAULT NULL,
  `status` VARCHAR(50) DEFAULT NULL,
  `notes` TEXT DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""
}

TABLE_ORDER = [
    "activities",
    "body_composition",
    "daily_summaries",
    "sleep_records",
    "hrv_records",
    "training_readiness",
    "training_status",
    "vo2_max_trend",
    "personal_records",
    "badges",
    "devices_and_gear",
    "sync_log"
]


def escape_sql_val(val: Any) -> str:
    """Escapes Python value for MySQL SQL statement."""
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "1" if val else "0"
    if isinstance(val, (int, float)):
        return str(val)
    
    val_str = str(val)
    # MySQL string escaping: backslash, single quote, newline, etc.
    escaped = val_str.replace("\\", "\\\\").replace("'", "\\'").replace("\r", "\\r").replace("\n", "\\n").replace("\0", "\\0")
    return f"'{escaped}'"


def export_sqlite_to_mysql():
    if not DB_PATH.exists():
        print(f"Error: SQLite database not found at {DB_PATH}")
        sys.exit(1)

    print(f"Reading SQLite database from: {DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    OUTPUT_SQL_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_SQL_PATH, "w", encoding="utf-8") as out:
        # Header
        out.write("-- ========================================================\n")
        out.write("-- Garmin Connect MySQL Full Export\n")
        out.write(f"-- Generated on: {os.uname().sysname} {os.uname().release}\n")
        out.write("-- Target: MySQL 8.0 / MySQL 5.7+ / MariaDB\n")
        out.write("-- ========================================================\n\n")
        out.write("SET NAMES utf8mb4;\n")
        out.write("SET FOREIGN_KEY_CHECKS = 0;\n")
        out.write("SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO';\n")
        out.write("SET AUTOCOMMIT = 0;\n")
        out.write("START TRANSACTION;\n\n")

        total_exported_rows = 0

        for table in TABLE_ORDER:
            print(f"Exporting table: {table}...", end=" ", flush=True)
            schema_ddl = MYSQL_SCHEMAS.get(table)
            if not schema_ddl:
                print("Skipped (no DDL)")
                continue

            out.write(f"-- --------------------------------------------------------\n")
            out.write(f"-- Table structure for `{table}`\n")
            out.write(f"-- --------------------------------------------------------\n")
            out.write(schema_ddl.strip() + "\n\n")

            # Fetch rows
            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()
            row_count = len(rows)
            total_exported_rows += row_count

            if row_count > 0:
                col_names = [col[0] for col in cursor.description]
                cols_formatted = ", ".join([f"`{c}`" for c in col_names])
                
                out.write(f"-- Dumping data for table `{table}` ({row_count} rows)\n")
                
                # Batch inserts in chunks of 200
                chunk_size = 200
                for i in range(0, row_count, chunk_size):
                    chunk = rows[i:i + chunk_size]
                    out.write(f"INSERT INTO `{table}` ({cols_formatted}) VALUES\n")
                    val_lines = []
                    for row in chunk:
                        escaped_vals = [escape_sql_val(row[col]) for col in col_names]
                        val_lines.append(f"({', '.join(escaped_vals)})")
                    out.write(",\n".join(val_lines) + ";\n")
                out.write("\n")
            print(f"OK ({row_count} rows)")

        out.write("-- ========================================================\n")
        out.write("COMMIT;\n")
        out.write("SET FOREIGN_KEY_CHECKS = 1;\n")
        out.write("-- Dump Completed Successfully\n")

    sql_size = OUTPUT_SQL_PATH.stat().st_size / (1024 * 1024)
    print(f"\nGenerated MySQL SQL dump: {OUTPUT_SQL_PATH} ({sql_size:.2f} MB)")

    # Create GZIP version
    print(f"Compressing to GZIP: {OUTPUT_GZ_PATH}...", end=" ", flush=True)
    with open(OUTPUT_SQL_PATH, 'rb') as f_in, gzip.open(OUTPUT_GZ_PATH, 'wb') as f_out:
        f_out.writelines(f_in)
    gz_size = OUTPUT_GZ_PATH.stat().st_size / (1024 * 1024)
    print(f"OK ({gz_size:.2f} MB)")

    print(f"\nTotal rows exported: {total_exported_rows}")


if __name__ == "__main__":
    export_sqlite_to_mysql()
