"""
performance_tracker.py
────────────────────────────────────────────────────────────
Feature 4: Performance Trend Tracking

Takes time-series performance ratings from the DB (one row
per employee per review cycle) and builds two CSV exports:

  1. performance_trends.csv   — one row per review per employee
     → used for the time-series line chart in Tableau

  2. performance_summary.csv  — one row per employee with trend stats
     → used for the employee detail / summary views

Also computes:
  - Trend direction: 'Improving', 'Declining', 'Stable'
  - YoY score delta
  - Consecutive high-performance streak
  - At-risk flag (two consecutive low ratings)
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

# Numeric mapping for string performance labels
PERFORMANCE_SCORE_MAP = {
    "Excellent":          5,
    "Good":               4,
    "Satisfactory":       3,
    "Needs Improvement":  2,
    "Poor":               1,
    # Aliases
    "Outstanding":        5,
    "Exceeds Expectations": 5,
    "Meets Expectations": 3,
    "Below Expectations": 2,
}

TREND_WINDOW = 3      # years to consider for trend direction


# ─── STEP 1: LOAD PERFORMANCE DATA ───────────────────────────────────────────

def load_performance_from_db(engine) -> pd.DataFrame:
    """Load performance_ratings table from the live DB."""
    query = """
        SELECT
            pr.id,
            pr.employee_id,
            pr.review_date,
            pr.performance,
            pr.score,
            e.first_name,
            e.last_name,
            e.department,
            e.job_title,
            e.state,
            e.termdate
        FROM performance_ratings pr
        JOIN employees e ON pr.employee_id = e.employee_id
        ORDER BY pr.employee_id, pr.review_date
    """
    return pd.read_sql(query, engine)


def load_performance_from_csv(ratings_csv: str,
                               employees_csv: str) -> pd.DataFrame:
    """
    Load from CSV files.
    If you only have the original dataset.csv (one performance column,
    no time-series), this function SIMULATES historical trend data
    so you can still demonstrate the feature.
    """
    emp = pd.read_csv(employees_csv)
    emp.columns = emp.columns.str.strip().str.lower().str.replace(" ","_")

    # Try loading separate ratings CSV first
    try:
        ratings = pd.read_csv(ratings_csv)
        ratings.columns = ratings.columns.str.strip().str.lower().str.replace(" ","_")
        logger.info(f"Loaded {len(ratings):,} rating records from {ratings_csv}.")
    except FileNotFoundError:
        logger.warning(f"{ratings_csv} not found. Simulating trend data from performance snapshot.")
        ratings = _simulate_performance_history(emp)

    # Join with employee info
    for col_variant in ["employee_id", "employeeid", "emp_id"]:
        if col_variant in emp.columns:
            emp = emp.rename(columns={col_variant: "employee_id"})
            break

    merged = ratings.merge(
        emp[["employee_id","first_name","last_name","department","job_title","state","termdate"]],
        on="employee_id", how="left"
    )
    return merged


def _simulate_performance_history(emp_df: pd.DataFrame) -> pd.DataFrame:
    """
    Simulate 3 years of performance reviews from a single performance
    snapshot column. Used for demo/testing when no historical data exists.

    Logic:
      - Year 1 (2021): slight regression of current rating
      - Year 2 (2022): current rating
      - Year 3 (2023): slight progression for active employees
    """
    rows = []
    perf_col = None
    for c in ["performance_rating", "performance", "performancerating"]:
        if c in emp_df.columns:
            perf_col = c
            break

    if perf_col is None:
        logger.warning("No performance column found. Generating random data.")
        emp_df["performance_rating"] = np.random.choice(
            list(PERFORMANCE_SCORE_MAP.keys()), size=len(emp_df))
        perf_col = "performance_rating"

    emp_id_col = "employee_id" if "employee_id" in emp_df.columns else "employeeid"

    for _, row in emp_df.iterrows():
        current_rating = str(row.get(perf_col, "Good")).strip()
        current_score  = PERFORMANCE_SCORE_MAP.get(current_rating, 3)

        for year_offset, year in enumerate([2021, 2022, 2023]):
            # Add slight variation per year
            noise = np.random.choice([-1, 0, 0, 1], p=[0.15, 0.5, 0.2, 0.15])
            sim_score  = max(1, min(5, current_score + noise - (2 - year_offset) * 0.3))
            sim_rating = _score_to_label(round(sim_score))

            rows.append({
                "employee_id": str(row.get(emp_id_col, "")),
                "review_date": f"{year}-12-31",
                "performance": sim_rating,
                "score":       round(sim_score, 2),
            })

    return pd.DataFrame(rows)


def _score_to_label(score: int) -> str:
    inv = {v: k for k, v in PERFORMANCE_SCORE_MAP.items()
           if k in ["Poor","Needs Improvement","Satisfactory","Good","Excellent"]}
    return inv.get(score, "Good")


# ─── STEP 2: ENRICH AND COMPUTE TRENDS ───────────────────────────────────────

def compute_trends(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Input : raw performance ratings df (one row per review per employee)
    Output: (trends_df, summary_df)

    trends_df   — enriched with numeric score + year columns, ready for
                  Tableau's line chart
    summary_df  — one row per employee with trend stats for the detail view
    """
    df = df.copy()
    df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce")
    df = df.dropna(subset=["review_date"])
    df = df.sort_values(["employee_id", "review_date"])

    # Numeric score from label (if raw score column missing)
    if "score" not in df.columns or df["score"].isna().all():
        df["score"] = df["performance"].map(PERFORMANCE_SCORE_MAP).fillna(3.0)
    else:
        df["score"] = pd.to_numeric(df["score"], errors="coerce")
        df["score"] = df["score"].fillna(df["performance"].map(PERFORMANCE_SCORE_MAP))

    df["review_year"]  = df["review_date"].dt.year
    df["review_month"] = df["review_date"].dt.month

    # YoY delta within each employee
    df["prev_score"]   = df.groupby("employee_id")["score"].shift(1)
    df["yoy_delta"]    = (df["score"] - df["prev_score"]).round(2)

    # ── Summary stats per employee ───────────────────────────
    def employee_stats(grp):
        grp = grp.sort_values("review_date")
        scores = grp["score"].tolist()
        n      = len(scores)

        # Trend direction (linear regression slope over last TREND_WINDOW reviews)
        recent = scores[-TREND_WINDOW:]
        if len(recent) >= 2:
            x = np.arange(len(recent), dtype=float)
            slope = np.polyfit(x, recent, 1)[0]
            if slope > 0.15:
                trend = "Improving"
            elif slope < -0.15:
                trend = "Declining"
            else:
                trend = "Stable"
        else:
            trend = "Stable"
            slope = 0.0

        # Consecutive high-performer streak (score >= 4)
        streak = 0
        for s in reversed(scores):
            if s >= 4:
                streak += 1
            else:
                break

        # At-risk: two or more consecutive low scores (score <= 2)
        at_risk_count = 0
        for s in reversed(scores):
            if s <= 2:
                at_risk_count += 1
            else:
                break
        at_risk = at_risk_count >= 2

        return pd.Series({
            "latest_performance":    grp.iloc[-1]["performance"],
            "latest_score":          grp.iloc[-1]["score"],
            "avg_score":             round(np.mean(scores), 2),
            "min_score":             min(scores),
            "max_score":             max(scores),
            "trend_direction":       trend,
            "trend_slope":           round(slope, 3),
            "yoy_delta_latest":      grp.iloc[-1]["yoy_delta"] if n > 1 else 0,
            "high_performer_streak": streak,
            "at_risk_flag":          at_risk,
            "review_count":          n,
            "first_review_year":     grp.iloc[0]["review_year"],
            "latest_review_year":    grp.iloc[-1]["review_year"],
        })

    summary_df = (
        df.groupby("employee_id")
        .apply(employee_stats)
        .reset_index()
    )

    # Merge employee info back into summary
    emp_info_cols = ["employee_id","first_name","last_name","department","job_title","state"]
    emp_info_cols = [c for c in emp_info_cols if c in df.columns]
    emp_info = df[emp_info_cols].drop_duplicates("employee_id")
    summary_df = summary_df.merge(emp_info, on="employee_id", how="left")

    logger.info(f"\n── Performance Trend Summary ──")
    logger.info(summary_df["trend_direction"].value_counts().to_string())
    logger.info(f"At-risk employees (2+ consecutive low scores): {summary_df['at_risk_flag'].sum()}")
    logger.info(f"Top performers (3+ streak): {(summary_df['high_performer_streak'] >= 3).sum()}")

    return df, summary_df


# ─── STEP 3: EXPORT ──────────────────────────────────────────────────────────

def export_performance_files(trends_df: pd.DataFrame,
                              summary_df: pd.DataFrame,
                              export_dir: str = "./exports"):
    """Write both CSVs to the export folder for Tableau."""
    Path(export_dir).mkdir(parents=True, exist_ok=True)

    # ── performance_trends.csv ───────────────────────────────
    trends_cols = ["employee_id","first_name","last_name","department","job_title",
                   "state","review_date","review_year","performance","score","yoy_delta"]
    trends_cols = [c for c in trends_cols if c in trends_df.columns]
    trends_path = Path(export_dir) / "performance_trends.csv"
    trends_df[trends_cols].to_csv(trends_path, index=False)
    logger.success(f"Performance trends exported → {trends_path}")

    # ── performance_summary.csv ──────────────────────────────
    summary_cols = ["employee_id","first_name","last_name","department","job_title","state",
                    "latest_performance","latest_score","avg_score","trend_direction",
                    "trend_slope","yoy_delta_latest","high_performer_streak",
                    "at_risk_flag","review_count","first_review_year","latest_review_year"]
    summary_cols = [c for c in summary_cols if c in summary_df.columns]
    summary_path = Path(export_dir) / "performance_summary.csv"
    summary_df[summary_cols].to_csv(summary_path, index=False)
    logger.success(f"Performance summary exported → {summary_path}")


# ─── MAIN ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    employees_csv = sys.argv[1] if len(sys.argv) > 1 else "../data/dataset.csv"
    ratings_csv   = sys.argv[2] if len(sys.argv) > 2 else "../data/performance_ratings.csv"
    export_dir    = os.getenv("EXPORT_DIR", "../exports")

    df = load_performance_from_csv(ratings_csv, employees_csv)
    trends_df, summary_df = compute_trends(df)
    export_performance_files(trends_df, summary_df, export_dir=export_dir)

    print("\nSample trend data (10 rows):")
    print(trends_df[["employee_id","review_year","performance","score","yoy_delta"]].head(10).to_string(index=False))
    print("\nSample performance summary (5 employees):")
    print(summary_df[["employee_id","avg_score","trend_direction","high_performer_streak","at_risk_flag"]].head().to_string(index=False))
