"""
scheduler.py
────────────────────────────────────────────────────────────
Master pipeline runner.

Runs all four features on a configurable schedule:
  1. Export live HR data from DB → hr_live_export.csv
  2. Build salary benchmarks    → salary_benchmark.csv / salary_comparison.csv
  3. Score attrition risk       → attrition_risk.csv
  4. Compute performance trends → performance_trends.csv / performance_summary.csv

How to run:
  python scheduler.py                  # runs once, then loops on schedule
  python scheduler.py --once           # runs once and exits (good for cron)
  python scheduler.py --task attrition # run only one task

Cron alternative (runs at 2AM daily):
  0 2 * * * cd /path/to/hr_project && python scheduler.py --once >> logs/scheduler.log 2>&1
"""

import argparse
import sys
import os
import schedule
import time
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

EXPORT_DIR = os.getenv("EXPORT_DIR", "./exports")
MODEL_DIR  = os.getenv("MODEL_DIR",  "./models")
DATA_DIR   = "./data"
INTERVAL_H = int(os.getenv("REFRESH_INTERVAL_HOURS", 6))


# ─── INDIVIDUAL TASKS ────────────────────────────────────────────────────────

def task_live_export():
    """Feature 1: Pull from DB and export hr_live_export.csv"""
    logger.info("── Task 1: Live HR Export ──────────────────────")
    try:
        from scripts.db_connector import get_engine, export_hr_live
        engine = get_engine()
        export_hr_live(engine, export_dir=EXPORT_DIR)
    except Exception as e:
        logger.error(f"Live export failed: {e}")
        logger.info("Hint: Check DB credentials in .env")


def task_salary_benchmark():
    """Feature 3: Build and export salary benchmarks"""
    logger.info("── Task 3: Salary Benchmarking ─────────────────")
    try:
        import pandas as pd
        from scripts.salary_benchmark import build_benchmarks, compare_salaries

        # Try DB export first, fall back to CSV
        csv_path = _find_data_source()
        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.strip().str.lower().str.replace(" ","_")

        benchmark_df = build_benchmarks(df, use_bls_api=True, export_dir=EXPORT_DIR)
        compare_salaries(df, benchmark_df, export_dir=EXPORT_DIR)
    except Exception as e:
        logger.error(f"Salary benchmark failed: {e}")


def task_attrition_model():
    """Feature 2: Train / score attrition risk model"""
    logger.info("── Task 2: Attrition Risk Scoring ──────────────")
    try:
        import pandas as pd
        from scripts.attrition_model import (
            load_data_from_csv, train_model, score_employees
        )

        retrain = os.getenv("RETRAIN_ON_STARTUP","false").lower() == "true"
        model_path = Path(MODEL_DIR) / "attrition_model.pkl"

        csv_path = _find_data_source()
        df = load_data_from_csv(csv_path)

        if retrain or not model_path.exists():
            logger.info("Training attrition model (this may take a few minutes)...")
            train_model(df, model_dir=MODEL_DIR)

        score_employees(df, model_dir=MODEL_DIR, export_dir=EXPORT_DIR)
    except Exception as e:
        logger.error(f"Attrition model failed: {e}")


def task_performance_trends():
    """Feature 4: Compute and export performance trends"""
    logger.info("── Task 4: Performance Trends ───────────────────")
    try:
        from scripts.performance_tracker import (
            load_performance_from_csv, compute_trends, export_performance_files
        )

        employees_csv = _find_data_source()
        ratings_csv   = Path(DATA_DIR) / "performance_ratings.csv"

        df = load_performance_from_csv(str(ratings_csv), employees_csv)
        trends_df, summary_df = compute_trends(df)
        export_performance_files(trends_df, summary_df, export_dir=EXPORT_DIR)
    except Exception as e:
        logger.error(f"Performance trends failed: {e}")


def run_all():
    """Run all 4 tasks in sequence."""
    logger.info("═══════════════════════════════════════════════")
    logger.info("  HR Analytics Pipeline — Full Run")
    logger.info("═══════════════════════════════════════════════")

    task_live_export()
    task_salary_benchmark()
    task_attrition_model()
    task_performance_trends()

    logger.success("All tasks complete. Exports ready in: " + EXPORT_DIR)
    logger.info("═══════════════════════════════════════════════")


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def _find_data_source() -> str:
    """
    Look for the best available data source.
    Priority: live export > original dataset.
    """
    live_export = Path(EXPORT_DIR) / "hr_live_export.csv"
    if live_export.exists():
        return str(live_export)

    for candidate in [
        Path(DATA_DIR) / "dataset.csv",
        Path(DATA_DIR) / "Final HR Dashboard.csv",
        Path("dataset.csv"),
    ]:
        if candidate.exists():
            return str(candidate)

    raise FileNotFoundError(
        "No data source found. Place dataset.csv in the ./data/ folder."
    )


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="HR Analytics Pipeline Scheduler")
    parser.add_argument("--once",  action="store_true",
                        help="Run once and exit (for cron)")
    parser.add_argument("--task",  choices=["live", "salary", "attrition", "performance", "all"],
                        default="all", help="Which task to run")
    args = parser.parse_args()

    # Set up logging
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    logger.add(log_dir / "scheduler_{time}.log",
               rotation="1 week", retention="1 month", level="INFO")

    task_map = {
        "live":        task_live_export,
        "salary":      task_salary_benchmark,
        "attrition":   task_attrition_model,
        "performance": task_performance_trends,
        "all":         run_all,
    }
    task_fn = task_map[args.task]

    # First run immediately
    task_fn()

    if args.once:
        sys.exit(0)

    # Schedule recurring runs
    logger.info(f"Scheduling '{args.task}' every {INTERVAL_H} hour(s).")
    schedule.every(INTERVAL_H).hours.do(task_fn)

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
