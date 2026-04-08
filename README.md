# HR Analytics — Upgraded Project Setup Guide

## Project Structure

```
hr_project/
├── .env.example            ← copy to .env and fill in your credentials
├── requirements.txt        ← pip install -r requirements.txt
├── scheduler.py            ← master runner / scheduler
│
├── config/
│   └── schema.sql          ← run once in MySQL/PostgreSQL to create tables
│
├── scripts/
│   ├── db_connector.py     ← Feature 1: DB integration + live CSV export
│   ├── attrition_model.py  ← Feature 2: ML attrition prediction
│   ├── salary_benchmark.py ← Feature 3: Salary benchmarking vs BLS market data
│   └── performance_tracker.py ← Feature 4: Performance trend analysis
│
├── data/
│   └── dataset.csv         ← put your original CSV here
│
├── exports/                ← auto-created; Tableau reads from here
├── models/                 ← auto-created; trained ML model saved here
└── logs/                   ← auto-created; run logs
```

---

## STEP 1 — Install Python and dependencies

```bash
# Requires Python 3.10 or newer
python --version

# Create a virtual environment (recommended)
python -m venv venv

# Activate it
# Windows:
venv\Scripts\activate
# Mac/Linux:
source venv/bin/activate

# Install all packages
pip install -r requirements.txt
```

---

## STEP 2 — Set up MySQL (or PostgreSQL)

### Option A: MySQL (recommended for beginners)

1. Download and install MySQL Community Server:
   https://dev.mysql.com/downloads/mysql/

2. Open MySQL Workbench or the terminal and create the database:
```sql
CREATE DATABASE hr_database CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'hr_user'@'localhost' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON hr_database.* TO 'hr_user'@'localhost';
FLUSH PRIVILEGES;
```

3. Run the schema script:
```bash
mysql -u hr_user -p hr_database < config/schema.sql
```

### Option B: PostgreSQL

1. Download and install PostgreSQL:
   https://www.postgresql.org/download/

2. Create database:
```sql
CREATE DATABASE hr_database;
CREATE USER hr_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE hr_database TO hr_user;
```

3. In schema.sql, uncomment the PostgreSQL trigger section and run:
```bash
psql -U hr_user -d hr_database -f config/schema.sql
```

---

## STEP 3 — Configure the .env file

```bash
# Copy the example file
cp .env.example .env

# Edit it with your values
# Windows: notepad .env
# Mac/Linux: nano .env
```

Set these values:
```
DB_TYPE=mysql
DB_HOST=localhost
DB_PORT=3306
DB_NAME=hr_database
DB_USER=hr_user
DB_PASSWORD=your_password
EXPORT_DIR=./exports
```

---

## STEP 4 — Migrate your CSV data to the database

Put your original `dataset.csv` inside the `data/` folder, then:

```bash
python -c "
from scripts.db_connector import get_engine, migrate_csv_to_db
engine = get_engine()
migrate_csv_to_db('data/dataset.csv', engine, if_exists='replace')
print('Migration complete!')
"
```

---

## STEP 5 — Run the full pipeline (first time)

```bash
# Run all 4 features once
python scheduler.py --once

# Or run a specific feature
python scheduler.py --once --task attrition
python scheduler.py --once --task salary
python scheduler.py --once --task performance
```

This will create these files in `exports/`:
- `hr_live_export.csv`        ← Feature 1
- `attrition_risk.csv`        ← Feature 2
- `salary_benchmark.csv`      ← Feature 3
- `salary_comparison.csv`     ← Feature 3
- `performance_trends.csv`    ← Feature 4
- `performance_summary.csv`   ← Feature 4

---

## STEP 6 — Connect Tableau to the exports

### For each of the 6 CSV files:
1. Open Tableau Public Desktop
2. Click **Connect → Text File**
3. Select the CSV from your `exports/` folder
4. Repeat for each file
5. Use **Data → Edit Data Source** to connect multiple CSVs together

### Joining the data sources in Tableau:
- `hr_live_export.csv`   ← primary (replaces your original dataset.csv)
- Join `attrition_risk.csv` on `employee_id` (Left join)
- Join `salary_comparison.csv` on `employee_id` (Left join)
- Join `performance_summary.csv` on `employee_id` (Left join)

---

## STEP 7 — New Tableau sheets to build

### Feature 2: Attrition Risk Dashboard
| Sheet Name         | Fields                            | Chart Type |
|--------------------|-----------------------------------|------------|
| Risk Score Dist.   | risk_label (Color), Count         | Bar Chart  |
| High Risk List     | full_name, dept, risk_score, label | Table      |
| Risk by Dept       | department, avg(risk_score)       | Bar Chart  |
| Top Risk Factors   | top_factor_1, Count               | Bar Chart  |

**Key calculated field in Tableau:**
```
// Risk Score Color (for conditional formatting)
IF [Risk Label] = "High" THEN "red"
ELSEIF [Risk Label] = "Medium" THEN "orange"
ELSE "green"
END
```

### Feature 3: Salary Benchmark Dashboard
| Sheet Name          | Fields                                | Chart Type  |
|---------------------|---------------------------------------|-------------|
| Salary Band Summary | salary_band, Count                    | Pie / Bar   |
| Role Comparison     | job_title, salary, benchmark_mid      | Dual Bar    |
| Over/Under Market   | full_name, salary_vs_market_pct       | Diverging Bar|
| Dept Heatmap        | department × salary_band, Count       | Heatmap     |

**Key calculated field:**
```
// Salary vs Market Color
IF [Salary Vs Market Pct] < -10 THEN "Below Market"
ELSEIF [Salary Vs Market Pct] > 10 THEN "Above Market"
ELSE "At Market"
END
```

### Feature 4: Performance Trend Dashboard
| Sheet Name          | Fields                                | Chart Type  |
|---------------------|---------------------------------------|-------------|
| Trend Over Time     | review_year (Cols), score (Rows), emp | Line Chart  |
| Trend Direction     | trend_direction, Count                | Bar Chart   |
| High Performers     | full_name, high_performer_streak      | Bar (sorted)|
| At-Risk Table       | full_name, dept, latest_score, trend  | Table       |
| Score Heatmap       | employee × review_year, score (Color) | Heatmap     |

**Key calculated field:**
```
// Trend Arrow indicator
IF [Trend Direction] = "Improving" THEN "▲"
ELSEIF [Trend Direction] = "Declining" THEN "▼"
ELSE "●"
END
```

---

## STEP 8 — Schedule automatic refresh

### Option A: Python scheduler (runs in background)
```bash
# Refresh every 6 hours (set in .env: REFRESH_INTERVAL_HOURS=6)
python scheduler.py
```

### Option B: Windows Task Scheduler
1. Open Task Scheduler → Create Basic Task
2. Name: "HR Analytics Refresh"
3. Trigger: Daily at 02:00 AM
4. Action: Start a program
5. Program: `C:\path\to\venv\Scripts\python.exe`
6. Arguments: `C:\path\to\hr_project\scheduler.py --once`

### Option C: Linux/Mac cron
```bash
# Edit crontab
crontab -e

# Add this line (runs at 2AM daily)
0 2 * * * cd /path/to/hr_project && source venv/bin/activate && python scheduler.py --once >> logs/cron.log 2>&1
```

After the scheduler runs, refresh the Tableau data source:
- In Tableau Public: **Data → Refresh Data Source**
- Or republish to Tableau Public to update the live dashboard.

---

## Troubleshooting

| Error | Fix |
|-------|-----|
| `ModuleNotFoundError: pymysql` | `pip install pymysql` |
| `Access denied for user` | Check DB_USER and DB_PASSWORD in .env |
| `Can't connect to MySQL server` | Ensure MySQL service is running |
| `FileNotFoundError: dataset.csv` | Put your CSV in the `data/` folder |
| `No trained model found` | Run `python scheduler.py --once --task attrition` |
| BLS API 404 | Normal — falls back to static benchmarks automatically |
| Tableau won't refresh CSV | Ensure the `exports/` path in Tableau matches your local path |
