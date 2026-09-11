# 🗄️ Garmin Vietējā Datu Bāze un Datu Arhīvs (`garmin_db`)

Šī sistēma nodrošina pilnīgu Garmin Connect datu lokālo glabāšanu, drošību un autonomiju. Ja Garmin jebkad ierobežo piekļuvi vēsturiskajiem datiem vai maina API, **visi tavi dati ir 100% drošībā lokālā datubāzē un JSON arhīvā**.

---

## 🏛️ Sistēmas Arhitektūra

```text
ag_ves_dati/
├── garmin_db/
│   ├── garmin.db               # SQLite relāciju datubāze (indeksēta, ātra SQL vaicājumiem)
│   ├── README.md               # Šī dokumentācija un vaicājumu paraugi
│   └── raw/                    # 100% oriģinālie Garmin JSON atbilžu faili
│       ├── activities/         # Katras aktivitātes pilns JSON (899+ faili)
│       ├── daily/              # Dienas veselības kopsavilkumi (soļi, kalorijas, RHR, stress)
│       ├── body_composition/  # Svaru un ķermeņa tauku/muskuļu mērījumi
│       ├── sleep/              # Miega fāzes (deep, light, REM, awake), miega vērtējumi
│       ├── hrv/                # Nakts HRV statuss, bāzes līnijas un novirzes
│       ├── readiness/          # Training Readiness un atjaunošanās laiki
│       ├── training_status/    # VO2 Max, slodzes statuss, akūtā slodze
│       └── misc/               # Ierīces, apavi/inventārs, personīgie rekordi, nozīmītes
└── scripts/
    ├── garmin_sync.py          # Pilnās un inkrementālās sinhronizācijas dzinējs
    └── garmin_query.py         # Ātrā analītika, statistika un SQL CLI rīks
```

---

## 🚀 Kā Lietot un Sinhronizēt Datus

### 1. Inkrementālā sinhronizācija (Ikdienas lietošanai)
Pārbauda pēdējo datumu datubāzē un novelk **tikai jaunās vai izmainītās dienas un aktivitātes**:
```bash
/Users/agrismarkus/ag/AG_VES/garmin_mcp/.venv/bin/python scripts/garmin_sync.py
```

### 2. Pilnā vēstures pārvilkšana
Novelk visu vēsturi no 2023. gada līdz šodienai:
```bash
/Users/agrismarkus/ag/AG_VES/garmin_mcp/.venv/bin/python scripts/garmin_sync.py --full
```

### 3. Pēdējo N dienu pārvilkšana
Piemēram, pārvilkt pēdējās 14 dienas:
```bash
/Users/agrismarkus/ag/AG_VES/garmin_mcp/.venv/bin/python scripts/garmin_sync.py --days 14
```

---

## 📊 Datu Bāzes Struktūra (`garmin.db`)

Datubāzē ir 12 tabulas ar primārajām atslēgām un indeksiem:

1. **`activities`**: Visas aktivitātes (skrējieni, spēks, riteņbraukšana) ar distanci, laiku, tempiem, pulsiem (avg/max), kadenci, kalorijām, TE, VO2 Max, jaudu un slodzi.
2. **`body_composition`**: Visi Garmin Index svaru mērījumi (svars kg, ĶMI, tauku %, ūdens %, muskuļu masa kg, kaulu masa kg, viscerālie tauki).
3. **`daily_summaries`**: Soļi, distances, aktīvās un BMR kalorijas, uzkāptie stāvi, miera pulss (RHR), min/max pulss, stresa līmeņi un Body Battery rādītāji.
4. **`sleep_records`**: Miega ilgums, dziļais, vieglais, REM un nomoda laiks, miega kvalitātes vērtējums (`sleep_score`), nakts pulss un stress.
5. **`hrv_records`**: Nedēļas vidējais HRV, aizvadītās nakts vidējais HRV, 5 min pīķa HRV, bāzes līnijas un statuss (balanced/unbalanced).
6. **`training_readiness`**: Gatavības rādītājs (0–100), atjaunošanās laiks stundās, miega un stresa atgriezeniskā saite.
7. **`training_status`**: Treniņu statuss (Productive, Maintaining, Recovery), VO2 Max, akūtā slodze un optimālais slodzes logs.
8. **`vo2_max_trend`**: VO2 Max vērtību dinamika un izmaiņas pa dienām.
9. **`personal_records`**: Visi personīgie rekordi (1k, 5k, 10k, Pusmaratons, Maratons, Garākais skrējiens utt.).
10. **`badges`**: Nopelnītās Garmin nozīmītes un punkti.
11. **`devices_and_gear`**: Reģistrētās ierīces un skriešanas apavi ar noskrietajiem kilometriem.
12. **`sync_log`**: Sinhronizācijas vēsture, izpildes laiks un statuss.

---

## 🔍 Ātrā Analītika ar `garmin_query.py`

### Kopsavilkums par visiem datiem:
```bash
/Users/agrismarkus/ag/AG_VES/garmin_mcp/.venv/bin/python scripts/garmin_query.py --summary
```

### VO2 Max mēnešu dinamika:
```bash
/Users/agrismarkus/ag/AG_VES/garmin_mcp/.venv/bin/python scripts/garmin_query.py --vo2max
```

### Pēdējie skrējieni ar tempu un pulsu:
```bash
/Users/agrismarkus/ag/AG_VES/garmin_mcp/.venv/bin/python scripts/garmin_query.py --runs
```

---

## 💡 Noderīgi SQL Vaicājumu Paraugi

Var izmantot tieši ar SQLite vai CLI (`--sql "QUERY"`):

### 1. Mēneša skriešanas kilometri un vidējais temps:
```sql
SELECT 
    substr(start_time_local, 1, 7) as menesis,
    COUNT(*) as skrejienu_skaits,
    ROUND(SUM(distance_m)/1000.0, 1) as kopa_km,
    ROUND(AVG(avg_hr), 0) as vid_pulss,
    printf('%d:%02d', CAST(AVG(duration_s / (distance_m / 1000.0)) / 60 AS INT), CAST(AVG(duration_s / (distance_m / 1000.0)) % 60 AS INT)) as vid_temps_min_km
FROM activities
WHERE activity_type = 'running'
GROUP BY menesis
ORDER BY menesis DESC;
```

### 2. Svara un tauku % izmaiņas pa mēnešiem:
```sql
SELECT 
    substr(calendar_date, 1, 7) as menesis,
    COUNT(*) as merijumu_skaits,
    ROUND(AVG(weight_kg), 2) as vid_svars_kg,
    ROUND(MIN(weight_kg), 2) as min_svars_kg,
    ROUND(AVG(body_fat_pct), 1) as vid_tauki_pct,
    ROUND(AVG(muscle_mass_kg), 2) as vid_muskuli_kg
FROM body_composition
GROUP BY menesis
ORDER BY menesis DESC;
```

### 3. Miega vērtējums vs. Treniņu gatavība (Training Readiness):
```sql
SELECT 
    s.calendar_date,
    s.sleep_score,
    ROUND(s.total_sleep_s / 3600.0, 1) as miega_stundas,
    ROUND(s.deep_sleep_s / 3600.0, 1) as dzilais_miegs_h,
    h.last_night_avg_hrv as nakts_hrv,
    r.readiness_score,
    r.readiness_level
FROM sleep_records s
JOIN training_readiness r ON s.calendar_date = r.calendar_date
JOIN hrv_records h ON s.calendar_date = h.calendar_date
ORDER BY s.calendar_date DESC
LIMIT 14;
```

---

## 🐬 MySQL Eksports un Hetzner Servera Izvietošana

Datu bāze ir pilnībā gatava eksportam un darbam uz MySQL 8.0 servera (piemēram, Hetzner ar DeepSeek Harness):

1. **MySQL Dump Fails:** `garmin_db/garmin_mysql_dump.sql` (un `garmin_mysql_dump.sql.gz`)
2. **Eksporta skripts:** `python3 scripts/export_sqlite_to_mysql_sql.py`
3. **Izvietošanas pamācība:** Skatīt detalizētu ceļvedi [HETZNER_DEPLOYMENT.md](file:///Users/agrismarkus/ag/AG_VES/ag_ves_dati/HETZNER_DEPLOYMENT.md).

