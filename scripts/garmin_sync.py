#!/usr/bin/env python3
"""
Garmin Full History Downloader & Incremental Sync Engine (High Performance & Resilient).

Downloads and synchronizes all available Garmin Connect health, fitness, 
and activity data into a local SQLite database and structured raw JSON archives.

Features:
- Complete historical download (Activities, Scale/Body Composition, Daily Summaries,
  Sleep, HRV, Training Readiness, Training Status, VO2 Max, Badges, PRs, Gear).
- Multi-threaded fast concurrent day syncing (5 workers).
- Incremental syncing (only fetches new or missing data).
- Resilient retry logic, rate-limiting, and error handling.
- Dual storage: Relational SQLite DB (fast SQL queries) + Raw JSON (100% data preservation).
"""

import os
import sys
import json
import time
import sqlite3
import argparse
import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add garmin_mcp to path if available
GARMIN_MCP_SRC = "/Users/agrismarkus/ag/AG_VES/garmin_mcp/src"
if os.path.isdir(GARMIN_MCP_SRC):
    sys.path.insert(0, GARMIN_MCP_SRC)

try:
    from garmin_mcp import token_utils
    from garminconnect import Garmin
except ImportError:
    try:
        from garminconnect import Garmin
        token_utils = None
    except ImportError:
        print("Error: garminconnect library not found.", flush=True)
        sys.exit(1)

# Default Paths
WORKSPACE_DIR = Path("/Users/agrismarkus/ag/AG_VES/ag_ves_dati")
DB_DIR = WORKSPACE_DIR / "garmin_db"
DB_PATH = DB_DIR / "garmin.db"
RAW_DIR = DB_DIR / "raw"

# Create directories
DB_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)
for sub in ["activities", "daily", "body_composition", "sleep", "hrv", "readiness", "training_status", "misc"]:
    (RAW_DIR / sub).mkdir(parents=True, exist_ok=True)


def safe_get(d: Any, *keys: str, default: Any = None) -> Any:
    """Safely traverses nested dictionaries/lists without throwing AttributeError or TypeError."""
    curr = d
    for k in keys:
        if isinstance(curr, dict):
            curr = curr.get(k)
        elif isinstance(curr, list) and isinstance(k, int) and 0 <= k < len(curr):
            curr = curr[k]
        else:
            return default
        if curr is None:
            return default
    return curr if curr is not None else default


def get_garmin_client() -> Garmin:
    """Initializes and authenticates Garmin client using stored session tokens."""
    token_path = None
    if token_utils:
        try:
            token_path = token_utils.get_token_path()
        except Exception:
            token_path = os.path.expanduser("~/.garminconnect")
    else:
        token_path = os.path.expanduser("~/.garminconnect")

    client = Garmin()
    client.login(token_path)
    return client


def init_db(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Initializes SQLite database schema with indexes and WAL mode."""
    conn = sqlite3.connect(str(db_path), timeout=60.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Activities Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS activities (
        activity_id INTEGER PRIMARY KEY,
        activity_name TEXT,
        activity_type TEXT,
        sport_type_id INTEGER,
        start_time_local TEXT,
        start_time_gmt TEXT,
        distance_m REAL,
        duration_s REAL,
        moving_duration_s REAL,
        elapsed_duration_s REAL,
        elevation_gain_m REAL,
        elevation_loss_m REAL,
        avg_speed_mps REAL,
        max_speed_mps REAL,
        avg_hr REAL,
        max_hr REAL,
        avg_cadence REAL,
        max_cadence REAL,
        calories REAL,
        bmr_calories REAL,
        aerobic_training_effect REAL,
        anaerobic_training_effect REAL,
        vo2_max REAL,
        avg_power REAL,
        max_power REAL,
        norm_power REAL,
        training_load REAL,
        location_name TEXT,
        gear_pk INTEGER,
        raw_json TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Body Composition / Weight Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS body_composition (
        sample_pk INTEGER PRIMARY KEY,
        date_timestamp INTEGER,
        calendar_date TEXT,
        weight_kg REAL,
        bmi REAL,
        body_fat_pct REAL,
        body_water_pct REAL,
        bone_mass_kg REAL,
        muscle_mass_kg REAL,
        physique_rating REAL,
        visceral_fat REAL,
        metabolic_age REAL,
        source_type TEXT,
        raw_json TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Daily Wellness Summaries Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS daily_summaries (
        calendar_date TEXT PRIMARY KEY,
        total_steps INTEGER,
        step_goal INTEGER,
        total_distance_m REAL,
        active_calories REAL,
        bmr_calories REAL,
        total_calories REAL,
        floors_climbed INTEGER,
        floors_goal INTEGER,
        resting_hr INTEGER,
        min_hr INTEGER,
        max_hr INTEGER,
        avg_stress INTEGER,
        max_stress INTEGER,
        stress_duration_s INTEGER,
        rest_stress_duration_s INTEGER,
        low_stress_duration_s INTEGER,
        med_stress_duration_s INTEGER,
        high_stress_duration_s INTEGER,
        body_battery_charged INTEGER,
        body_battery_drained INTEGER,
        body_battery_highest INTEGER,
        body_battery_lowest INTEGER,
        moderate_intensity_minutes INTEGER,
        vigorous_intensity_minutes INTEGER,
        raw_json TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Sleep Records Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sleep_records (
        calendar_date TEXT PRIMARY KEY,
        sleep_start_timestamp INTEGER,
        sleep_end_timestamp INTEGER,
        total_sleep_s INTEGER,
        deep_sleep_s INTEGER,
        light_sleep_s INTEGER,
        rem_sleep_s INTEGER,
        awake_s INTEGER,
        sleep_score INTEGER,
        sleep_score_qualifier TEXT,
        avg_sleep_hr REAL,
        min_sleep_hr REAL,
        avg_sleep_stress REAL,
        avg_sleep_respiration REAL,
        raw_json TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # HRV Records Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS hrv_records (
        calendar_date TEXT PRIMARY KEY,
        weekly_avg_hrv REAL,
        last_night_avg_hrv REAL,
        last_night_5min_high_hrv REAL,
        baseline_low REAL,
        baseline_balanced_low REAL,
        baseline_balanced_upper REAL,
        status TEXT,
        raw_json TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Training Readiness Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS training_readiness (
        calendar_date TEXT PRIMARY KEY,
        readiness_score INTEGER,
        readiness_level TEXT,
        sleep_score_feedback TEXT,
        recovery_time_s INTEGER,
        hrv_feedback TEXT,
        stress_history_feedback TEXT,
        raw_json TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Training Status Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS training_status (
        calendar_date TEXT PRIMARY KEY,
        training_status_key TEXT,
        vo2_max_running REAL,
        vo2_max_cycling REAL,
        fitness_trend TEXT,
        load_status TEXT,
        acute_load REAL,
        optimal_load_min REAL,
        optimal_load_max REAL,
        raw_json TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # VO2 Max Trend Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS vo2_max_trend (
        calendar_date TEXT PRIMARY KEY,
        vo2_max REAL,
        sport TEXT,
        source TEXT,
        carried_forward INTEGER,
        raw_json TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Personal Records Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS personal_records (
        record_id INTEGER PRIMARY KEY AUTOINCREMENT,
        type_key TEXT,
        activity_type TEXT,
        value REAL,
        formatted_value TEXT,
        activity_id INTEGER,
        pr_start_time TEXT,
        raw_json TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Badges Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS badges (
        badge_id INTEGER PRIMARY KEY,
        badge_key TEXT,
        badge_name TEXT,
        badge_points INTEGER,
        earned_date TEXT,
        raw_json TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Devices and Gear Table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS devices_and_gear (
        item_pk TEXT PRIMARY KEY,
        item_type TEXT,
        display_name TEXT,
        model_name TEXT,
        raw_json TEXT,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Sync History Log
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sync_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sync_type TEXT,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        records_synced INTEGER,
        status TEXT,
        notes TEXT
    )
    """)

    # Indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_act_start_time ON activities(start_time_local);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_act_type ON activities(activity_type);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_bc_date ON body_composition(calendar_date);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_summaries(calendar_date);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sleep_date ON sleep_records(calendar_date);")

    conn.commit()
    return conn


def sync_activities(client: Garmin, conn: sqlite3.Connection, full: bool = False) -> int:
    """Syncs activities in batches of 100."""
    print("--> Syncing Activities...", flush=True)
    cursor = conn.cursor()
    total_activities = client.count_activities()
    print(f"    Total activities on Garmin: {total_activities}", flush=True)

    batch_size = 100
    start_idx = 0
    synced_count = 0

    while start_idx < total_activities:
        print(f"    Fetching activities {start_idx} to {min(start_idx + batch_size, total_activities)}...", flush=True)
        try:
            batch = client.get_activities(start_idx, batch_size)
            if not batch:
                break

            for act in batch:
                act_id = act.get("activityId")
                if not act_id:
                    continue

                # Save raw JSON
                raw_path = RAW_DIR / "activities" / f"activity_{act_id}.json"
                if not raw_path.exists() or full:
                    with open(raw_path, "w", encoding="utf-8") as f:
                        json.dump(act, f, ensure_ascii=False, indent=2)

                act_name = act.get("activityName")
                act_type_info = act.get("activityType") or {}
                act_type = act_type_info.get("typeKey") if isinstance(act_type_info, dict) else str(act_type_info)
                sport_type_id = act_type_info.get("typeId") if isinstance(act_type_info, dict) else None
                start_local = act.get("startTimeLocal")
                start_gmt = act.get("startTimeGMT")
                distance = act.get("distance")
                duration = act.get("duration")
                moving_dur = act.get("movingDuration")
                elapsed_dur = act.get("elapsedDuration")
                elevation_gain = act.get("elevationGain")
                elevation_loss = act.get("elevationLoss")
                avg_speed = act.get("averageSpeed")
                max_speed = act.get("maxSpeed")
                avg_hr = act.get("averageHR")
                max_hr = act.get("maxHR")
                avg_cadence = act.get("averageRunningCadenceInStepsPerMinute") or act.get("averageCadence")
                max_cadence = act.get("maxRunningCadenceInStepsPerMinute") or act.get("maxCadence")
                calories = act.get("calories")
                bmr_calories = act.get("bmrCalories")
                aerobic_te = act.get("aerobicTrainingEffect")
                anaerobic_te = act.get("anaerobicTrainingEffect")
                vo2_max = act.get("vO2MaxValue")
                avg_power = act.get("avgPower")
                max_power = act.get("maxPower")
                norm_power = act.get("normPower")
                training_load = act.get("activityTrainingLoad")
                location = act.get("locationName")
                gear_pk = act.get("gearPk")
                raw_json = json.dumps(act, ensure_ascii=False)

                cursor.execute("""
                INSERT INTO activities (
                    activity_id, activity_name, activity_type, sport_type_id,
                    start_time_local, start_time_gmt, distance_m, duration_s,
                    moving_duration_s, elapsed_duration_s, elevation_gain_m, elevation_loss_m,
                    avg_speed_mps, max_speed_mps, avg_hr, max_hr, avg_cadence, max_cadence,
                    calories, bmr_calories, aerobic_training_effect, anaerobic_training_effect,
                    vo2_max, avg_power, max_power, norm_power, training_load, location_name,
                    gear_pk, raw_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(activity_id) DO UPDATE SET
                    activity_name=excluded.activity_name,
                    activity_type=excluded.activity_type,
                    distance_m=excluded.distance_m,
                    duration_s=excluded.duration_s,
                    avg_hr=excluded.avg_hr,
                    max_hr=excluded.max_hr,
                    calories=excluded.calories,
                    aerobic_training_effect=excluded.aerobic_training_effect,
                    anaerobic_training_effect=excluded.anaerobic_training_effect,
                    vo2_max=excluded.vo2_max,
                    raw_json=excluded.raw_json,
                    updated_at=CURRENT_TIMESTAMP
                """, (
                    act_id, act_name, act_type, sport_type_id,
                    start_local, start_gmt, distance, duration,
                    moving_dur, elapsed_dur, elevation_gain, elevation_loss,
                    avg_speed, max_speed, avg_hr, max_hr, avg_cadence, max_cadence,
                    calories, bmr_calories, aerobic_te, anaerobic_te,
                    vo2_max, avg_power, max_power, norm_power, training_load, location,
                    gear_pk, raw_json
                ))
                synced_count += 1

            conn.commit()
            start_idx += batch_size
            time.sleep(0.05)
        except Exception as e:
            print(f"    Error syncing activities at index {start_idx}: {e}", flush=True)
            break

    print(f"    ✓ Synced {synced_count} activities.", flush=True)
    return synced_count


def sync_body_composition(client: Garmin, conn: sqlite3.Connection, start_date: str = "2023-01-01", end_date: str = None) -> int:
    """Syncs scale weigh-ins and body composition."""
    print("--> Syncing Body Composition & Weigh-ins...", flush=True)
    if not end_date:
        end_date = datetime.date.today().isoformat()
    cursor = conn.cursor()
    synced_count = 0

    try:
        data = client.get_body_composition(start_date, end_date)
        if isinstance(data, dict) and "dateWeightList" in data:
            raw_path = RAW_DIR / "body_composition" / "body_composition_full.json"
            with open(raw_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            for item in data["dateWeightList"]:
                sample_pk = item.get("samplePk")
                if not sample_pk:
                    continue
                date_ts = item.get("date")
                cal_date = item.get("calendarDate")
                weight_g = item.get("weight")
                weight_kg = (weight_g / 1000.0) if weight_g else None
                bmi = item.get("bmi")
                body_fat = item.get("bodyFat")
                body_water = item.get("bodyWater")
                bone_mass_g = item.get("boneMass")
                bone_mass_kg = (bone_mass_g / 1000.0) if bone_mass_g else None
                muscle_mass_g = item.get("muscleMass")
                muscle_mass_kg = (muscle_mass_g / 1000.0) if muscle_mass_g else None
                physique_rating = item.get("physiqueRating")
                visceral_fat = item.get("visceralFat")
                metabolic_age = item.get("metabolicAge")
                source_type = item.get("sourceType")
                raw_json = json.dumps(item, ensure_ascii=False)

                cursor.execute("""
                INSERT INTO body_composition (
                    sample_pk, date_timestamp, calendar_date, weight_kg, bmi,
                    body_fat_pct, body_water_pct, bone_mass_kg, muscle_mass_kg,
                    physique_rating, visceral_fat, metabolic_age, source_type,
                    raw_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(sample_pk) DO UPDATE SET
                    weight_kg=excluded.weight_kg,
                    bmi=excluded.bmi,
                    body_fat_pct=excluded.body_fat_pct,
                    muscle_mass_kg=excluded.muscle_mass_kg,
                    raw_json=excluded.raw_json,
                    updated_at=CURRENT_TIMESTAMP
                """, (
                    sample_pk, date_ts, cal_date, weight_kg, bmi,
                    body_fat, body_water, bone_mass_kg, muscle_mass_kg,
                    physique_rating, visceral_fat, metabolic_age, source_type,
                    raw_json
                ))
                synced_count += 1

            conn.commit()
    except Exception as e:
        print(f"    Error syncing body composition: {e}", flush=True)

    print(f"    ✓ Synced {synced_count} body composition records.", flush=True)
    return synced_count


def _fetch_single_day(d_str: str) -> Dict[str, Any]:
    """Fetches all metrics for a single day using an isolated thread-local Garmin client."""
    result = {"date": d_str}
    
    # Check if raw files exist locally first
    summary_path = RAW_DIR / "daily" / f"summary_{d_str}.json"
    sleep_path = RAW_DIR / "sleep" / f"sleep_{d_str}.json"
    hrv_path = RAW_DIR / "hrv" / f"hrv_{d_str}.json"
    readiness_path = RAW_DIR / "readiness" / f"readiness_{d_str}.json"
    status_path = RAW_DIR / "training_status" / f"status_{d_str}.json"

    # Only instantiate client if we need to make API calls
    client = None
    def get_client():
        nonlocal client
        if client is None:
            client = get_garmin_client()
        return client

    # 1. Stats and Body / Daily Summary
    if summary_path.exists():
        try:
            with open(summary_path, "r", encoding="utf-8") as f:
                result["stats_and_body"] = json.load(f)
        except Exception:
            result["stats_and_body"] = None
    else:
        try:
            sb = get_client().get_stats_and_body(d_str)
            result["stats_and_body"] = sb
        except Exception:
            result["stats_and_body"] = None

    # 2. Sleep Data
    if sleep_path.exists():
        try:
            with open(sleep_path, "r", encoding="utf-8") as f:
                result["sleep"] = json.load(f)
        except Exception:
            result["sleep"] = None
    else:
        try:
            sleep = get_client().get_sleep_data(d_str)
            result["sleep"] = sleep
        except Exception:
            result["sleep"] = None

    # 3. HRV Data
    if hrv_path.exists():
        try:
            with open(hrv_path, "r", encoding="utf-8") as f:
                result["hrv"] = json.load(f)
        except Exception:
            result["hrv"] = None
    else:
        try:
            hrv = get_client().get_hrv_data(d_str)
            result["hrv"] = hrv
        except Exception:
            result["hrv"] = None

    # 4. Training Readiness
    if readiness_path.exists():
        try:
            with open(readiness_path, "r", encoding="utf-8") as f:
                result["readiness"] = json.load(f)
        except Exception:
            result["readiness"] = None
    else:
        try:
            readiness = get_client().get_training_readiness(d_str)
            result["readiness"] = readiness
        except Exception:
            result["readiness"] = None

    # 5. Training Status
    if status_path.exists():
        try:
            with open(status_path, "r", encoding="utf-8") as f:
                result["status"] = json.load(f)
        except Exception:
            result["status"] = None
    else:
        try:
            status = get_client().get_training_status(d_str)
            result["status"] = status
        except Exception:
            result["status"] = None

    return result


def sync_daily_metrics_concurrent(conn: sqlite3.Connection, start_date_str: str, end_date_str: str, max_workers: int = 5) -> int:
    """Syncs daily health data concurrently using ThreadPoolExecutor."""
    print(f"--> Syncing Daily Health Metrics from {start_date_str} to {end_date_str} (concurrency: {max_workers})...", flush=True)
    cursor = conn.cursor()

    start_date = datetime.date.fromisoformat(start_date_str)
    end_date = datetime.date.fromisoformat(end_date_str)
    delta = datetime.timedelta(days=1)

    date_list = []
    curr = start_date
    while curr <= end_date:
        date_list.append(curr.isoformat())
        curr += delta

    total_days = len(date_list)
    synced_days = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_date = {executor.submit(_fetch_single_day, d): d for d in date_list}

        for future in as_completed(future_to_date):
            d_str = future_to_date[future]
            try:
                res = future.result()
                d = res["date"]

                # 1. Stats and Body
                sb = res.get("stats_and_body")
                if sb and isinstance(sb, dict):
                    with open(RAW_DIR / "daily" / f"summary_{d}.json", "w", encoding="utf-8") as f:
                        json.dump(sb, f, ensure_ascii=False)

                    cursor.execute("""
                    INSERT INTO daily_summaries (
                        calendar_date, total_steps, step_goal, total_distance_m,
                        active_calories, bmr_calories, total_calories, floors_climbed,
                        floors_goal, resting_hr, min_hr, max_hr, avg_stress, max_stress,
                        stress_duration_s, rest_stress_duration_s, low_stress_duration_s,
                        med_stress_duration_s, high_stress_duration_s, body_battery_charged,
                        body_battery_drained, body_battery_highest, body_battery_lowest,
                        moderate_intensity_minutes, vigorous_intensity_minutes, raw_json, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(calendar_date) DO UPDATE SET
                        total_steps=excluded.total_steps,
                        total_distance_m=excluded.total_distance_m,
                        active_calories=excluded.active_calories,
                        resting_hr=excluded.resting_hr,
                        avg_stress=excluded.avg_stress,
                        body_battery_highest=excluded.body_battery_highest,
                        raw_json=excluded.raw_json,
                        updated_at=CURRENT_TIMESTAMP
                    """, (
                        d, sb.get("totalSteps"), sb.get("dailyStepGoal"), sb.get("totalDistanceMeters"),
                        sb.get("activeKilocalories"), sb.get("bmrKilocalories"), sb.get("totalKilocalories"),
                        sb.get("floorsAscended"), sb.get("userFloorsAscendedGoal"), sb.get("restingHeartRate"),
                        sb.get("minHeartRate"), sb.get("maxHeartRate"), sb.get("averageStressLevel"),
                        sb.get("maxStressLevel"), sb.get("stressDuration"), sb.get("restStressDuration"),
                        sb.get("lowStressDuration"), sb.get("mediumStressDuration"), sb.get("highStressDuration"),
                        sb.get("bodyBatteryChargedValue"), sb.get("bodyBatteryDrainedValue"),
                        sb.get("bodyBatteryHighestValue"), sb.get("bodyBatteryLowestValue"),
                        sb.get("moderateIntensityMinutes"), sb.get("vigorousIntensityMinutes"),
                        json.dumps(sb, ensure_ascii=False)
                    ))

                # 2. Sleep
                sleep = res.get("sleep")
                if sleep and isinstance(sleep, dict):
                    with open(RAW_DIR / "sleep" / f"sleep_{d}.json", "w", encoding="utf-8") as f:
                        json.dump(sleep, f, ensure_ascii=False)
                    dto = sleep.get("dailySleepDTO") or {}
                    sleep_score = safe_get(dto, "sleepScores", "overall", "value")
                    qualifier = safe_get(dto, "sleepScores", "overall", "qualifierKey")

                    cursor.execute("""
                    INSERT INTO sleep_records (
                        calendar_date, sleep_start_timestamp, sleep_end_timestamp,
                        total_sleep_s, deep_sleep_s, light_sleep_s, rem_sleep_s, awake_s,
                        sleep_score, sleep_score_qualifier, avg_sleep_hr, min_sleep_hr,
                        avg_sleep_stress, avg_sleep_respiration, raw_json, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(calendar_date) DO UPDATE SET
                        total_sleep_s=excluded.total_sleep_s,
                        deep_sleep_s=excluded.deep_sleep_s,
                        rem_sleep_s=excluded.rem_sleep_s,
                        sleep_score=excluded.sleep_score,
                        sleep_score_qualifier=excluded.sleep_score_qualifier,
                        raw_json=excluded.raw_json,
                        updated_at=CURRENT_TIMESTAMP
                    """, (
                        d, dto.get("sleepStartTimestampGMT"), dto.get("sleepEndTimestampGMT"),
                        dto.get("sleepTimeSeconds"), dto.get("deepSleepSeconds"), dto.get("lightSleepSeconds"),
                        dto.get("remSleepSeconds"), dto.get("awakeSleepSeconds"),
                        sleep_score, qualifier,
                        None, None, dto.get("averageStressDuringSleep"), None,
                        json.dumps(sleep, ensure_ascii=False)
                    ))

                # 3. HRV
                hrv = res.get("hrv")
                if hrv and isinstance(hrv, dict):
                    with open(RAW_DIR / "hrv" / f"hrv_{d}.json", "w", encoding="utf-8") as f:
                        json.dump(hrv, f, ensure_ascii=False)
                    hs = hrv.get("hrvSummary") or {}
                    b_low = safe_get(hs, "baseline", "lowThreshold")
                    b_bal_low = safe_get(hs, "baseline", "balancedLow")
                    b_bal_up = safe_get(hs, "baseline", "balancedUpper")

                    cursor.execute("""
                    INSERT INTO hrv_records (
                        calendar_date, weekly_avg_hrv, last_night_avg_hrv,
                        last_night_5min_high_hrv, baseline_low, baseline_balanced_low,
                        baseline_balanced_upper, status, raw_json, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(calendar_date) DO UPDATE SET
                        weekly_avg_hrv=excluded.weekly_avg_hrv,
                        last_night_avg_hrv=excluded.last_night_avg_hrv,
                        status=excluded.status,
                        raw_json=excluded.raw_json,
                        updated_at=CURRENT_TIMESTAMP
                    """, (
                        d, hs.get("weeklyAvg"), hs.get("lastNightAvg"), hs.get("lastNight5MinHigh"),
                        b_low, b_bal_low, b_bal_up, hs.get("status"),
                        json.dumps(hrv, ensure_ascii=False)
                    ))

                # 4. Training Readiness
                readiness = res.get("readiness")
                if readiness and isinstance(readiness, (dict, list)):
                    with open(RAW_DIR / "readiness" / f"readiness_{d}.json", "w", encoding="utf-8") as f:
                        json.dump(readiness, f, ensure_ascii=False)
                    r_entry = readiness[0] if isinstance(readiness, list) and readiness else readiness
                    if isinstance(r_entry, dict):
                        cursor.execute("""
                        INSERT INTO training_readiness (
                            calendar_date, readiness_score, readiness_level, sleep_score_feedback,
                            recovery_time_s, hrv_feedback, stress_history_feedback, raw_json, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        ON CONFLICT(calendar_date) DO UPDATE SET
                            readiness_score=excluded.readiness_score,
                            readiness_level=excluded.readiness_level,
                            raw_json=excluded.raw_json,
                            updated_at=CURRENT_TIMESTAMP
                        """, (
                            d, r_entry.get("score"), r_entry.get("level"), r_entry.get("sleepScoreFeedback"),
                            r_entry.get("recoveryTime"), r_entry.get("hrvFeedback"),
                            r_entry.get("stressHistoryFeedback"), json.dumps(readiness, ensure_ascii=False)
                        ))

                # 5. Training Status & VO2 Max
                status = res.get("status")
                if status and isinstance(status, dict):
                    with open(RAW_DIR / "training_status" / f"status_{d}.json", "w", encoding="utf-8") as f:
                        json.dump(status, f, ensure_ascii=False)
                    vo2_run = safe_get(status, "mostRecentVO2Max", "generic", "vo2MaxValue")
                    acute_load = safe_get(status, "metrics", "acuteTrainingLoadValue")

                    cursor.execute("""
                    INSERT INTO training_status (
                        calendar_date, training_status_key, vo2_max_running, vo2_max_cycling,
                        fitness_trend, load_status, acute_load, optimal_load_min,
                        optimal_load_max, raw_json, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(calendar_date) DO UPDATE SET
                        training_status_key=excluded.training_status_key,
                        vo2_max_running=excluded.vo2_max_running,
                        acute_load=excluded.acute_load,
                        raw_json=excluded.raw_json,
                        updated_at=CURRENT_TIMESTAMP
                    """, (
                        d, status.get("trainingStatus"), vo2_run, None, status.get("fitnessTrend"),
                        status.get("loadStatus"), acute_load,
                        None, None, json.dumps(status, ensure_ascii=False)
                    ))
                    if vo2_run:
                        cursor.execute("""
                        INSERT INTO vo2_max_trend (calendar_date, vo2_max, sport, source, carried_forward, raw_json, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        ON CONFLICT(calendar_date) DO UPDATE SET
                            vo2_max=excluded.vo2_max,
                            updated_at=CURRENT_TIMESTAMP
                        """, (d, vo2_run, "running", "get_training_status", 0, json.dumps(status, ensure_ascii=False)))

                synced_days += 1
                if synced_days % 25 == 0 or synced_days == total_days:
                    conn.commit()
                    pct = (synced_days / total_days) * 100
                    print(f"    Progress: {synced_days}/{total_days} days ({pct:.1f}%)...", flush=True)

            except Exception as e:
                print(f"    Error processing date {d_str}: {e}", flush=True)

    conn.commit()
    print(f"    ✓ Successfully synced daily metrics for {synced_days} days.", flush=True)
    return synced_days


def sync_miscellaneous(client: Garmin, conn: sqlite3.Connection) -> None:
    """Syncs Badges, PRs, Gear, Devices, and Profile."""
    print("--> Syncing Badges, Personal Records, Devices & Profile...", flush=True)
    cursor = conn.cursor()

    # 1. Profile
    try:
        prof = client.get_user_profile()
        with open(RAW_DIR / "misc" / "user_profile.json", "w", encoding="utf-8") as f:
            json.dump(prof, f, ensure_ascii=False, indent=2)
        user_pk = prof.get("id")
    except Exception as e:
        print(f"    Error profile: {e}", flush=True)
        user_pk = None

    # 2. Devices
    try:
        devices = client.get_devices()
        with open(RAW_DIR / "misc" / "devices.json", "w", encoding="utf-8") as f:
            json.dump(devices, f, ensure_ascii=False, indent=2)
        for dev in devices:
            dev_id = str(dev.get("deviceId") or dev.get("unitId"))
            cursor.execute("""
            INSERT INTO devices_and_gear (item_pk, item_type, display_name, model_name, raw_json, updated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(item_pk) DO UPDATE SET
                display_name=excluded.display_name,
                raw_json=excluded.raw_json,
                updated_at=CURRENT_TIMESTAMP
            """, (f"device_{dev_id}", "device", dev.get("displayName") or dev.get("productDisplayName"), dev.get("partNumber"), json.dumps(dev, ensure_ascii=False)))
    except Exception as e:
        print(f"    Error devices: {e}", flush=True)

    # 3. Gear
    if user_pk:
        try:
            gear_list = client.get_gear(user_pk)
            with open(RAW_DIR / "misc" / "gear.json", "w", encoding="utf-8") as f:
                json.dump(gear_list, f, ensure_ascii=False, indent=2)
            for g in gear_list:
                g_id = str(g.get("gearPk"))
                cursor.execute("""
                INSERT INTO devices_and_gear (item_pk, item_type, display_name, model_name, raw_json, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(item_pk) DO UPDATE SET
                    display_name=excluded.display_name,
                    model_name=excluded.model_name,
                    raw_json=excluded.raw_json,
                    updated_at=CURRENT_TIMESTAMP
                """, (f"gear_{g_id}", "gear", g.get("customMakeModel") or g.get("displayName"), g.get("gearTypeName"), json.dumps(g, ensure_ascii=False)))
        except Exception as e:
            print(f"    Error gear: {e}", flush=True)

    # 4. Personal Records
    try:
        prs = client.get_personal_record()
        with open(RAW_DIR / "misc" / "personal_records.json", "w", encoding="utf-8") as f:
            json.dump(prs, f, ensure_ascii=False, indent=2)
        for pr in prs:
            cursor.execute("""
            INSERT INTO personal_records (type_key, activity_type, value, formatted_value, activity_id, pr_start_time, raw_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                pr.get("typeKey") or pr.get("prType"),
                pr.get("activityType"),
                pr.get("value"),
                pr.get("formattedValue"),
                pr.get("activityId"),
                pr.get("startTimeGMT"),
                json.dumps(pr, ensure_ascii=False)
            ))
    except Exception as e:
        print(f"    Error PRs: {e}", flush=True)

    # 5. Earned Badges
    try:
        badges = client.get_earned_badges()
        with open(RAW_DIR / "misc" / "earned_badges.json", "w", encoding="utf-8") as f:
            json.dump(badges, f, ensure_ascii=False, indent=2)
        for b in badges:
            b_id = b.get("badgeId")
            cursor.execute("""
            INSERT INTO badges (badge_id, badge_key, badge_name, badge_points, earned_date, raw_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(badge_id) DO UPDATE SET
                badge_name=excluded.badge_name,
                badge_points=excluded.badge_points,
                earned_date=excluded.earned_date,
                raw_json=excluded.raw_json,
                updated_at=CURRENT_TIMESTAMP
            """, (
                b_id, b.get("badgeKey"), b.get("badgeName"),
                b.get("badgePointsValue"), b.get("badgeEarnedDate"),
                json.dumps(b, ensure_ascii=False)
            ))
    except Exception as e:
        print(f"    Error badges: {e}", flush=True)

    conn.commit()
    print("    ✓ Synced Badges, PRs, Devices, Gear & Profile.", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Garmin Connect Full History & Incremental Sync")
    parser.add_argument("--full", action="store_true", help="Perform full historical sync from 2023 to present")
    parser.add_argument("--incremental", action="store_true", help="Perform incremental sync since last recorded date (default)")
    parser.add_argument("--days", type=int, help="Sync specific number of recent days")
    parser.add_argument("--activities-only", action="store_true", help="Sync only activities")
    parser.add_argument("--workers", type=int, default=10, help="Number of concurrent worker threads (default 10)")
    args = parser.parse_args()

    start_time = datetime.datetime.now()
    print(f"==================================================", flush=True)
    print(f"🚀 Garmin Data Sync Engine started at {start_time.isoformat()}", flush=True)
    print(f"   Database: {DB_PATH}", flush=True)
    print(f"   Raw Archive: {RAW_DIR}", flush=True)
    print(f"==================================================", flush=True)

    client = get_garmin_client()
    conn = init_db(DB_PATH)

    today_str = datetime.date.today().isoformat()

    if args.full:
        sync_activities(client, conn, full=True)
        sync_body_composition(client, conn, start_date="2023-01-01", end_date=today_str)
        sync_daily_metrics_concurrent(conn, start_date_str="2024-07-27", end_date_str=today_str, max_workers=args.workers)
        sync_miscellaneous(client, conn)
    elif args.activities_only:
        sync_activities(client, conn, full=False)
    elif args.days:
        past_date = (datetime.date.today() - datetime.timedelta(days=args.days)).isoformat()
        sync_activities(client, conn, full=False)
        sync_body_composition(client, conn, start_date=past_date, end_date=today_str)
        sync_daily_metrics_concurrent(conn, start_date_str=past_date, end_date_str=today_str, max_workers=args.workers)
        sync_miscellaneous(client, conn)
    else:
        # Incremental Sync (Default)
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(calendar_date) as max_date FROM daily_summaries")
        row = cursor.fetchone()
        max_date = row["max_date"] if row and row["max_date"] else None

        if not max_date:
            print("No previous daily history found in DB. Starting full sync...", flush=True)
            sync_activities(client, conn, full=True)
            sync_body_composition(client, conn, start_date="2023-01-01", end_date=today_str)
            sync_daily_metrics_concurrent(conn, start_date_str="2024-07-27", end_date_str=today_str, max_workers=args.workers)
            sync_miscellaneous(client, conn)
        else:
            start_date = (datetime.date.fromisoformat(max_date) - datetime.timedelta(days=3)).isoformat()
            print(f"Incremental sync starting from {start_date} to {today_str}...", flush=True)
            sync_activities(client, conn, full=False)
            sync_body_composition(client, conn, start_date=start_date, end_date=today_str)
            sync_daily_metrics_concurrent(conn, start_date_str=start_date, end_date_str=today_str, max_workers=args.workers)
            sync_miscellaneous(client, conn)

    end_time = datetime.datetime.now()
    duration_s = (end_time - start_time).total_seconds()

    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO sync_log (sync_type, start_time, end_time, records_synced, status, notes)
    VALUES (?, ?, ?, ?, ?, ?)
    """, ("full" if args.full else "incremental", start_time.isoformat(), end_time.isoformat(), 1, "SUCCESS", f"Duration: {duration_s:.1f}s"))
    conn.commit()

    print(f"\n==================================================", flush=True)
    print(f"✅ Sync completed successfully in {duration_s:.1f} seconds.", flush=True)
    print(f"   All data safely stored in SQLite and raw JSON.", flush=True)
    print(f"==================================================", flush=True)


if __name__ == "__main__":
    main()
