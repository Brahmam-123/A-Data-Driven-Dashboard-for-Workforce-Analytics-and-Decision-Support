"""
salary_benchmark.py
────────────────────────────────────────────────────────────
Feature 3: Salary Benchmarking

Approach:
  1. Fetch occupational wage data from the U.S. Bureau of Labor
     Statistics (BLS) Occupational Employment Statistics API (free).
  2. Map internal job titles to BLS Standard Occupational Codes (SOC).
  3. Build a benchmarks table: low (10th pct) / mid (50th pct) / high (90th pct).
  4. Compare each employee's salary against the benchmark for their role.
  5. Export salary_benchmark.csv for Tableau.

BLS API docs: https://www.bls.gov/developers/api_signature_v2.htm
No API key required for basic use (500 queries/day limit).
Register at https://www.bls.gov/developers/ for 500→2500 limit.
"""

import os
import json
import time
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_API_KEY = os.getenv("BLS_API_KEY", "")

# ─── JOB TITLE → BLS SOC SERIES MAPPING ──────────────────────────────────────
# BLS series format for National OES:  OEUN000000XXXXXXXX08
# Where XXXXXXXX is the 6-digit SOC code + level suffix
# Full list: https://www.bls.gov/oes/current/oes_stru.htm

JOB_TITLE_TO_BLS_SERIES = {
    # Internal Job Title    : (BLS Series ID,           BLS Occupation Name)
    "Software Engineer"     : ("OEUN0000001113200108",   "Software Developers"),
    "Senior Engineer"       : ("OEUN0000001113200108",   "Software Developers"),
    "Data Analyst"          : ("OEUN0000001512000308",   "Business Intelligence Analysts"),
    "Data Scientist"        : ("OEUN0000001519310008",   "Data Scientists"),
    "HR Manager"            : ("OEUN0000001121100108",   "Human Resources Managers"),
    "HR Specialist"         : ("OEUN0000001311200108",   "Human Resources Specialists"),
    "Financial Analyst"     : ("OEUN0000001313100108",   "Financial and Investment Analysts"),
    "Accountant"            : ("OEUN0000001312000108",   "Accountants and Auditors"),
    "Marketing Analyst"     : ("OEUN0000001316010008",   "Market Research Analysts"),
    "Marketing Manager"     : ("OEUN0000001121400108",   "Marketing Managers"),
    "Sales Manager"         : ("OEUN0000001121200108",   "Sales Managers"),
    "IT Manager"            : ("OEUN0000001119300108",   "Computer and Information Systems Managers"),
    "Project Manager"       : ("OEUN0000001311210008",   "Project Management Specialists"),
    "Operations Manager"    : ("OEUN0000001121000108",   "General and Operations Managers"),
    "Customer Service Rep"  : ("OEUN0000004341200108",   "Customer Service Representatives"),
}

# ─── STATIC FALLBACK (if BLS API is unavailable) ─────────────────────────────
# These are approximate 2024 U.S. national median salaries from BLS OES data.
# Update annually from: https://www.bls.gov/oes/current/oes_nat.htm

STATIC_BENCHMARKS = {
    "Software Engineer"    : {"low": 75000,  "mid": 120000, "high": 180000},
    "Senior Engineer"      : {"low": 100000, "mid": 150000, "high": 220000},
    "Data Analyst"         : {"low": 55000,  "mid": 85000,  "high": 130000},
    "Data Scientist"       : {"low": 80000,  "mid": 120000, "high": 180000},
    "HR Manager"           : {"low": 65000,  "mid": 100000, "high": 150000},
    "HR Specialist"        : {"low": 45000,  "mid": 65000,  "high": 95000},
    "Financial Analyst"    : {"low": 58000,  "mid": 90000,  "high": 140000},
    "Accountant"           : {"low": 50000,  "mid": 78000,  "high": 120000},
    "Marketing Analyst"    : {"low": 48000,  "mid": 72000,  "high": 110000},
    "Marketing Manager"    : {"low": 70000,  "mid": 105000, "high": 160000},
    "Sales Manager"        : {"low": 72000,  "mid": 110000, "high": 175000},
    "IT Manager"           : {"low": 95000,  "mid": 140000, "high": 200000},
    "Project Manager"      : {"low": 72000,  "mid": 108000, "high": 160000},
    "Operations Manager"   : {"low": 65000,  "mid": 98000,  "high": 155000},
    "Customer Service Rep" : {"low": 35000,  "mid": 46000,  "high": 65000},
}


# ─── STEP 1: FETCH FROM BLS API ───────────────────────────────────────────────

def fetch_bls_wages(series_ids: list, year: str = "2024") -> dict:
    """
    Call the BLS API for a list of series IDs.
    Returns {series_id: annual_wage} dict.

    The BLS OES series returns hourly wages — multiply by 2080 for annual.
    """
    results = {}
    # BLS allows max 50 series per request
    for i in range(0, len(series_ids), 50):
        batch = series_ids[i:i+50]
        payload = {
            "seriesid":    batch,
            "startyear":   year,
            "endyear":     year,
            "registrationkey": BLS_API_KEY if BLS_API_KEY else None,
        }
        payload = {k: v for k, v in payload.items() if v is not None}

        try:
            resp = requests.post(BLS_API_URL, json=payload, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") != "REQUEST_SUCCEEDED":
                logger.warning(f"BLS API returned non-success: {data.get('message')}")
                continue

            for series in data.get("Results", {}).get("series", []):
                sid = series["seriesID"]
                if series.get("data"):
                    latest = series["data"][0]
                    # BLS OES reports annual wages directly (not hourly for this series type)
                    val = latest.get("value", "0").replace(",", "")
                    try:
                        results[sid] = float(val)
                    except ValueError:
                        results[sid] = None

            time.sleep(0.5)   # be polite to the API

        except Exception as e:
            logger.warning(f"BLS API error: {e}. Will fall back to static data.")

    logger.info(f"Fetched {len(results)} BLS wage series.")
    return results


# ─── STEP 2: BUILD BENCHMARK TABLE ───────────────────────────────────────────

def build_benchmarks(employee_df: pd.DataFrame,
                     use_bls_api: bool = True,
                     export_dir: str = "./exports") -> pd.DataFrame:
    """
    Build a benchmark table for all job titles present in employee_df.
    Tries BLS API first; falls back to static values.

    Exports salary_benchmark.csv with columns:
        job_title, benchmark_low, benchmark_mid, benchmark_high, source, as_of_date
    """
    today = pd.Timestamp("today").date()
    Path(export_dir).mkdir(parents=True, exist_ok=True)

    job_titles = employee_df["job_title"].dropna().unique().tolist()
    logger.info(f"Building benchmarks for {len(job_titles)} job titles...")

    rows = []
    bls_cache = {}

    # ── Try BLS API for mapped titles ──────────────────────
    if use_bls_api:
        mapped = {jt: JOB_TITLE_TO_BLS_SERIES[jt]
                  for jt in job_titles if jt in JOB_TITLE_TO_BLS_SERIES}
        series_ids = list({v[0] for v in mapped.values()})
        if series_ids:
            bls_cache = fetch_bls_wages(series_ids)

    # ── Build one row per job title ─────────────────────────
    for jt in job_titles:
        source = "static"
        low = mid = high = None

        # Try BLS first
        if jt in JOB_TITLE_TO_BLS_SERIES:
            series_id, _ = JOB_TITLE_TO_BLS_SERIES[jt]
            bls_wage = bls_cache.get(series_id)
            if bls_wage and bls_wage > 0:
                # BLS OES annual median wage → low = 70%, high = 140% of median
                mid    = round(bls_wage, 0)
                low    = round(bls_wage * 0.70, 0)
                high   = round(bls_wage * 1.40, 0)
                source = "BLS OES"

        # Fall back to static
        if mid is None:
            static = STATIC_BENCHMARKS.get(jt)
            if static:
                low, mid, high = static["low"], static["mid"], static["high"]
                source = "static"
            else:
                # Generic fallback: estimate from salary distribution in the dataset
                dept_salaries = employee_df.loc[
                    employee_df["job_title"] == jt, "salary"
                ].dropna()
                if len(dept_salaries) >= 3:
                    low    = round(dept_salaries.quantile(0.25), 0)
                    mid    = round(dept_salaries.quantile(0.50), 0)
                    high   = round(dept_salaries.quantile(0.75), 0)
                    source = "internal_quartiles"
                else:
                    logger.warning(f"No benchmark data for '{jt}'. Skipping.")
                    continue

        rows.append({
            "job_title":      jt,
            "benchmark_low":  low,
            "benchmark_mid":  mid,
            "benchmark_high": high,
            "source":         source,
            "as_of_date":     str(today),
        })
        logger.debug(f"  {jt}: ${low:,.0f} / ${mid:,.0f} / ${high:,.0f} [{source}]")

    benchmark_df = pd.DataFrame(rows)
    out_path = Path(export_dir) / "salary_benchmark.csv"
    benchmark_df.to_csv(out_path, index=False)
    logger.success(f"Salary benchmarks exported → {out_path}")
    return benchmark_df


# ─── STEP 3: COMPARE EMPLOYEES vs BENCHMARKS ─────────────────────────────────

def compare_salaries(employee_df: pd.DataFrame,
                     benchmark_df: pd.DataFrame,
                     export_dir: str = "./exports") -> pd.DataFrame:
    """
    Join employee salaries with benchmarks and compute:
      - salary_vs_market_pct  : % above/below market median
      - salary_band           : 'Below Market', 'At Market', 'Above Market'

    Exports salary_comparison.csv for Tableau.
    """
    # Merge on job_title (case-insensitive)
    emp = employee_df.copy()
    emp["jt_lower"] = emp["job_title"].str.lower().str.strip()
    bench = benchmark_df.copy()
    bench["jt_lower"] = bench["job_title"].str.lower().str.strip()

    merged = emp.merge(
        bench[["jt_lower","benchmark_low","benchmark_mid","benchmark_high","source"]],
        on="jt_lower", how="left"
    )

    # % difference vs market median
    merged["salary_vs_market_pct"] = (
        (merged["salary"] - merged["benchmark_mid"]) / merged["benchmark_mid"] * 100
    ).round(1)

    # Band labels
    def salary_band(row):
        if pd.isna(row["benchmark_mid"]) or pd.isna(row["salary"]):
            return "Unknown"
        pct = row["salary_vs_market_pct"]
        if pct < -10:
            return "Below Market"
        elif pct > 10:
            return "Above Market"
        return "At Market"

    merged["salary_band"] = merged.apply(salary_band, axis=1)

    # Clean up
    merged = merged.drop(columns=["jt_lower"])

    out_path = Path(export_dir) / "salary_comparison.csv"
    export_cols = ["employee_id","first_name","last_name","department","job_title",
                   "state","city","salary","benchmark_low","benchmark_mid","benchmark_high",
                   "salary_vs_market_pct","salary_band","source"]
    export_cols = [c for c in export_cols if c in merged.columns]
    merged[export_cols].to_csv(out_path, index=False)
    logger.success(f"Salary comparison exported → {out_path}")

    # Summary
    logger.info("\n── Salary Band Distribution ──")
    logger.info(merged["salary_band"].value_counts().to_string())
    logger.info(f"\nAvg salary vs market: {merged['salary_vs_market_pct'].mean():+.1f}%")

    return merged


# ─── MAIN ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    csv_path   = sys.argv[1] if len(sys.argv) > 1 else "../data/dataset.csv"
    export_dir = os.getenv("EXPORT_DIR", "../exports")

    df = pd.read_csv(csv_path)
    # Normalize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    if "job_title" not in df.columns and "jobtitle" in df.columns:
        df = df.rename(columns={"jobtitle": "job_title"})

    benchmark_df = build_benchmarks(df, use_bls_api=True, export_dir=export_dir)
    result_df    = compare_salaries(df, benchmark_df, export_dir=export_dir)

    print("\nSample salary comparison (first 10 rows):")
    print(result_df[["employee_id","job_title","salary",
                      "benchmark_mid","salary_vs_market_pct","salary_band"]].head(10).to_string(index=False))
