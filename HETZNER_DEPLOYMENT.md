# 🚀 Hetzner Server Deployment & DeepSeek Harness Guide

This guide provides step-by-step instructions for deploying the **Garmin Connect Database** and **DeepSeek AI Health Harness** to your Hetzner Linux server.

---

## 🏗️ Architecture Overview

```
                        ┌────────────────────────────────────────┐
                        │             Hetzner Server             │
                        │                                        │
┌────────────────┐      │  ┌──────────────────────────────────┐  │
│ Garmin Connect │ ───> │  │   Garmin Sync Engine (Python)    │  │
│      Cloud     │      │  └─────────────────┬────────────────┘  │
└────────────────┘      │                    │ (Auto Daily Sync) │
                        │                    ▼                   │
                        │  ┌──────────────────────────────────┐  │
                        │  │        MySQL 8.0 Database        │  │
                        │  │  (Docker Container or Baremetal) │  │
                        │  │   • 901+ Activities              │  │
                        │  │   • 777+ Days Health / Sleep     │  │
                        │  │   • Scale / HRV / VO2 Max        │  │
                        │  └─────────────────┬────────────────┘  │
                        │                    │                   │
                        │                    ▼                   │
                        │  ┌──────────────────────────────────┐  │
                        │  │      DeepSeek AI Harness         │  │
                        │  │  • Health Analysis               │  │
                        │  │  • Daily Training & Nutrition    │  │
                        │  │  • Automated Summary Markdown    │  │
                        │  └──────────────────────────────────┘  │
                        └────────────────────────────────────────┘
```

---

## 📋 Prerequisites

- **Hetzner Cloud / Dedicated Server** (Ubuntu 22.04 LTS / 24.04 LTS recommended).
- **Docker & Docker Compose** installed (`sudo apt install -y docker.io docker-compose-plugin`).
- **Python 3.10+** and Git (`sudo apt install -y python3-pip python3-venv git`).

---

## ⚡ Fast 5-Step Deployment

### 1. Clone Repository on Server
```bash
git clone https://github.com/agrism/ag-ves-dati.git
cd ag-ves-dati
```

### 2. Configure Environment Variables
Copy `.env.example` to `.env` and set your secure passwords:
```bash
cp .env.example .env
nano .env
```
Ensure the following settings are configured:
```ini
DB_TYPE=mysql
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_DATABASE=garmin_db
MYSQL_USER=garmin_user
MYSQL_PASSWORD=your_strong_password_here
MYSQL_ROOT_PASSWORD=your_root_password_here
DEEPSEEK_API_KEY=your_deepseek_api_key_here
```

### 3. Start MySQL Container
```bash
docker compose up -d
```
Verify the container is healthy:
```bash
docker compose ps
```

### 4. Import the Garmin MySQL Database Dump
Import the compressed SQL dump directly into MySQL:

```bash
# Option A: Import via Docker Compose (Recommended)
gunzip -c garmin_db/garmin_mysql_dump.sql.gz | docker compose exec -T garmin_mysql mysql -u garmin_user -p"$(grep MYSQL_PASSWORD .env | cut -d '=' -f2)" garmin_db

# Option B: Plain SQL import (if already extracted)
docker compose exec -T garmin_mysql mysql -u garmin_user -p"$(grep MYSQL_PASSWORD .env | cut -d '=' -f2)" garmin_db < garmin_db/garmin_mysql_dump.sql
```

### 5. Setup Python Virtual Environment & Verify Data
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pymysql python-dotenv garminconnect

# Test query CLI against MySQL
python3 scripts/garmin_query.py --summary
```

You should see all **901+ activities** and **777+ health days** displayed directly from your MySQL database!

---

## 🔄 Automated Daily Sync Setup (Cron)

To keep the MySQL database continuously up to date with new workouts, sleep, weight, and recovery metrics from Garmin:

1. Copy your Garmin authentication token to the server:
   ```bash
   mkdir -p ~/.garminconnect
   # Copy tokens from local machine ~/.garminconnect to server ~/.garminconnect
   ```

2. Add a daily cron job to run the incremental sync:
   ```bash
   crontab -e
   ```
   Add the following line (runs every morning at 06:30 and night at 23:30 Europe/Riga time):
   ```cron
   30 6,23 * * * cd /root/ag-ves-dati && /root/ag-ves-dati/.venv/bin/python scripts/garmin_sync.py --incremental >> /var/log/garmin_sync.log 2>&1
   ```

---

## 🤖 DeepSeek Harness Integration

DeepSeek Harness can query MySQL directly for context-rich health analytics, automated summaries, and training recommendations.

### Key MySQL Tables for DeepSeek Prompts:
| Table | Description | Primary Key |
|---|---|---|
| `daily_summaries` | Steps, calories, resting HR, stress, body battery | `calendar_date` |
| `sleep_records` | Sleep stages (deep, REM, light), sleep score, respiration | `calendar_date` |
| `body_composition` | Weight (kg), body fat %, muscle mass, bone mass, BMI | `sample_pk` |
| `hrv_records` | Nightly HRV, 7-day average, baseline ranges, status | `calendar_date` |
| `training_readiness` | Morning readiness score (1-100), recovery time | `calendar_date` |
| `training_status` | Acute load, optimal load ranges, VO2 max | `calendar_date` |
| `activities` | All GPS runs, cycling, strength, HR, cadence, power | `activity_id` |

### DeepSeek Python Runner Pattern:
```python
import os
import pymysql
from dotenv import load_dotenv

load_dotenv()

# Connect to MySQL
conn = pymysql.connect(
    host=os.getenv("MYSQL_HOST", "127.0.0.1"),
    user=os.getenv("MYSQL_USER", "garmin_user"),
    password=os.getenv("MYSQL_PASSWORD"),
    database=os.getenv("MYSQL_DATABASE", "garmin_db"),
    cursorclass=pymysql.cursors.DictCursor
)

with conn.cursor() as cur:
    # Fetch last 7 days of complete metrics for DeepSeek prompt context
    cur.execute("""
        SELECT 
            d.calendar_date,
            d.total_steps,
            d.resting_hr,
            d.avg_stress,
            d.body_battery_highest,
            d.body_battery_lowest,
            s.sleep_score,
            s.total_sleep_s / 3600.0 AS sleep_hours,
            h.last_night_avg_hrv,
            h.weekly_avg_hrv,
            r.readiness_score,
            b.weight_kg
        FROM daily_summaries d
        LEFT JOIN sleep_records s ON d.calendar_date = s.calendar_date
        LEFT JOIN hrv_records h ON d.calendar_date = h.calendar_date
        LEFT JOIN training_readiness r ON d.calendar_date = r.calendar_date
        LEFT JOIN (
            SELECT calendar_date, weight_kg 
            FROM body_composition 
            GROUP BY calendar_date
        ) b ON d.calendar_date = b.calendar_date
        ORDER BY d.calendar_date DESC
        LIMIT 7;
    """)
    recent_health_context = cur.fetchall()

print(f"Loaded {len(recent_health_context)} days of health context for DeepSeek prompt.")
```

---

## 📦 File Inventory

- `garmin_db/garmin_mysql_dump.sql` (38.9 MB) – Uncompressed MySQL dump.
- `garmin_db/garmin_mysql_dump.sql.gz` (3.2 MB) – Compressed MySQL dump for fast server transfer.
- `scripts/export_sqlite_to_mysql_sql.py` – Export script to regenerate dump anytime.
- `scripts/db_adapter.py` – Database abstraction layer (auto-detects MySQL or SQLite).
- `scripts/garmin_query.py` – Multi-database query & statistics CLI tool.
- `docker-compose.yml` – MySQL 8.0 Docker stack.
- `.env.example` – Environment configuration template.
