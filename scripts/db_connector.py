"""
db_connector.py
────────────────────────────────────────────────────────────
Feature 1: Real-time Database Integration

Handles:
  - DB connection via SQLAlchemy (MySQL & PostgreSQL)
  - One-time CSV → DB migration (run once to seed the database)
  - Daily export of live data back to CSV for Tableau
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from loguru import logger
from pathlib import Path

load_dotenv()

# ─── CONNECTION ──────────────────────────────────────────────────────────────

def get_engine():
    """
    Build and return a SQLAlchemy engine from .env config.
    Supports mysql (via PyMySQL) and postgresql (via psycopg2).
    """
    db_type = os.getenv("DB_TYPE", "mysql").lower()
    host    = os.getenv("DB_HOST", "localhost")
    port    = os.getenv("DB_PORT", "3306")
    name    = os.getenv("DB_NAME", "hr_database")
    user    = os.getenv("DB_USER", "root")
    pw      = os.getenv("DB_PASSWORD", "")

    if db_type == "mysql":
        url = f"mysql+pymysql://{user}:{pw}@{host}:{port}/{name}?charset=utf8mb4"
    elif db_type == "postgresql":
        url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{name}"
    else:
        raise ValueError(f"Unsupported DB_TYPE: {db_type}. Use 'mysql' or 'postgresql'.")

    engine = create_engine(url, pool_pre_ping=True, pool_recycle=3600)
    logger.info(f"Connected to {db_type} at {host}:{port}/{name}")
    return engine


def test_connection(engine):
    """Quick health-check — raises if DB is unreachable."""
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logger.success("Database connection OK.")


# ─── CSV → DB MIGRATION ──────────────────────────────────────────────────────

def migrate_csv_to_db(csv_path: str, engine, if_exists: str = "replace"):
    """
    One-time migration: read your original dataset.csv and
    insert all rows into the employees table.

    Args:
        csv_path   : path to your CSV (e.g. 'data/dataset.csv')
        engine     : SQLAlchemy engine
        if_exists  : 'replace' (drop + recreate) or 'append'
    """
    logger.info(f"Reading CSV: {csv_path}")
    df = pd.read_csv(csv_path)

    # ── Normalise column names to match schema ──────────────────
    col_map = {
        "Employee ID":     "employee_id",
        "EmployeeID":      "employee_id",
        "First Name":      "first_name",
        "Last Name":       "last_name",
        "Gender":          "gender",
        "Birthdate":       "birthdate",
        "Hiredate":        "hiredate",
        "Termdate":        "termdate",
        "Department":      "department",
        "Job Title":       "job_title",
        "State":           "state",
        "City":            "city",
        "Education Level": "education_level",
        "Performance Rating": "performance_rating_snapshot",  # historical snapshot
        "Salary":          "salary",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # ── Clean date columns ──────────────────────────────────────
    for date_col in ["birthdate", "hiredate", "termdate"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
            df[date_col] = df[date_col].dt.date  # strip time component

    # ── Ensure employee_id is string ────────────────────────────
    if "employee_id" in df.columns:
        df["employee_id"] = df["employee_id"].astype(str).str.strip()

    # ── Drop columns not in the employees schema ─────────────────
    keep_cols = ["employee_id","first_name","last_name","gender","birthdate",
                 "hiredate","termdate","department","job_title","state","city",
                 "education_level","salary"]
    df = df[[c for c in keep_cols if c in df.columns]]

    # ── Write to DB ──────────────────────────────────────────────
    df.to_sql("employees", engine, if_exists=if_exists, index=False,
              chunksize=500, method="multi")
    logger.success(f"Migrated {len(df):,} employee records to 'employees' table.")


# ─── LIVE EXPORT → CSV ───────────────────────────────────────────────────────

def export_hr_live(engine, export_dir: str = "./exports"):
    """
    Pull full employee data from DB (with computed fields),
    save as hr_live_export.csv for Tableau to reload.

    This runs on a schedule — Tableau then refreshes its data source.
    """
    Path(export_dir).mkdir(parents=True, exist_ok=True)

    query = """
        SELECT
            e.employee_id,
            e.first_name,
            e.last_name,
            CONCAT(e.first_name, ' ', e.last_name)               AS full_name,
            e.gender,
            e.birthdate,
            e.hiredate,
            e.termdate,
            e.department,
            e.job_title,
            e.state,
            e.city,
            e.education_level,
            e.salary,

            -- Status: Active vs Terminated
            CASE WHEN e.termdate IS NULL THEN 'Active' ELSE 'Terminated' END  AS status,

            -- Location: HQ vs Branch
            CASE WHEN e.state = 'New York' THEN 'HQ' ELSE 'Branch' END       AS location,

            -- Age (computed live from today's date)
            TIMESTAMPDIFF(YEAR, e.birthdate, CURDATE())                       AS age,

            -- Length of hire in years
            CASE
                WHEN e.termdate IS NULL THEN TIMESTAMPDIFF(YEAR, e.hiredate, CURDATE())
                ELSE TIMESTAMPDIFF(YEAR, e.hiredate, e.termdate)
            END                                                               AS length_of_hire,

            -- Attrition risk score (from ML pipeline)
            COALESCE(ar.risk_score, 0)   AS attrition_risk_score,
            COALESCE(ar.risk_label, 'N/A') AS attrition_risk_label,
            ar.top_factor_1,
            ar.top_factor_2,
            ar.top_factor_3,

            -- Salary vs market benchmark
            sb.benchmark_mid             AS market_salary_mid,
            ROUND((e.salary - sb.benchmark_mid) / sb.benchmark_mid * 100, 1)
                                         AS salary_vs_market_pct

        FROM employees e
        LEFT JOIN attrition_risk ar  ON e.employee_id = ar.employee_id
        LEFT JOIN salary_benchmarks sb
            ON  LOWER(e.job_title) = LOWER(sb.job_title)
            AND (sb.state = e.state OR sb.state IS NULL)
    """

    # PostgreSQL uses AGE() and EXTRACT — swap the query for PG if needed
    db_type = os.getenv("DB_TYPE", "mysql")
    if db_type == "postgresql":
        query = _pg_live_query()

    df = pd.read_sql(query, engine)
    out_path = Path(export_dir) / "hr_live_export.csv"
    df.to_csv(out_path, index=False)
    logger.success(f"Exported {len(df):,} rows → {out_path}")
    return df


def _pg_live_query():
    """PostgreSQL equivalent of the export query above."""
    return """
        SELECT
            e.employee_id,
            e.first_name,
            e.last_name,
            e.first_name || ' ' || e.last_name                               AS full_name,
            e.gender,
            e.birthdate,
            e.hiredate,
            e.termdate,
            e.department,
            e.job_title,
            e.state,
            e.city,
            e.education_level,
            e.salary,
            CASE WHEN e.termdate IS NULL THEN 'Active' ELSE 'Terminated' END  AS status,
            CASE WHEN e.state = 'New York' THEN 'HQ' ELSE 'Branch' END       AS location,
            DATE_PART('year', AGE(e.birthdate))::INT                          AS age,
            CASE
                WHEN e.termdate IS NULL THEN DATE_PART('year', AGE(e.hiredate))::INT
                ELSE DATE_PART('year', AGE(e.hiredate, e.termdate))::INT
            END                                                               AS length_of_hire,
            COALESCE(ar.risk_score, 0)                                        AS attrition_risk_score,
            COALESCE(ar.risk_label, 'N/A')                                    AS attrition_risk_label,
            ar.top_factor_1, ar.top_factor_2, ar.top_factor_3,
            sb.benchmark_mid                                                  AS market_salary_mid,
            ROUND(((e.salary - sb.benchmark_mid) / sb.benchmark_mid * 100)::NUMERIC, 1)
                                                                              AS salary_vs_market_pct
        FROM employees e
        LEFT JOIN attrition_risk ar  ON e.employee_id = ar.employee_id
        LEFT JOIN salary_benchmarks sb
            ON LOWER(e.job_title) = LOWER(sb.job_title)
            AND (sb.state = e.state OR sb.state IS NULL)
    """


# ─── MAIN (quick test) ───────────────────────────────────────────────────────

if __name__ == "__main__":
    engine = get_engine()
    test_connection(engine)

    # Uncomment to run one-time CSV migration:
    # migrate_csv_to_db("../data/dataset.csv", engine, if_exists="replace")

    export_hr_live(engine, export_dir="../exports")
    logger.info("Done.")
