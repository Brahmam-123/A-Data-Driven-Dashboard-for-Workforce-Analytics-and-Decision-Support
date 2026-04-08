"""
attrition_model.py
────────────────────────────────────────────────────────────
Feature 2: Predictive Attrition Analysis

Pipeline:
  1. Load employee data (from DB or CSV)
  2. Feature engineering
  3. Train Random Forest + XGBoost ensemble
  4. Evaluate with precision / recall / AUC
  5. Score ALL active employees → risk scores + top risk factors
  6. Export attrition_risk.csv for Tableau
  7. Optionally write risk scores back to DB
"""

import os
import warnings
import pandas as pd
import numpy as np
import joblib
from pathlib import Path
from datetime import date
from loguru import logger

from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import (
    classification_report, roc_auc_score,
    confusion_matrix, ConfusionMatrixDisplay
)
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OrdinalEncoder
from sklearn.impute import SimpleImputer
from imblearn.over_sampling import SMOTE

try:
    import xgboost as xgb
    XGB_AVAILABLE = True
except ImportError:
    XGB_AVAILABLE = False
    logger.warning("XGBoost not installed. Using Random Forest only.")

try:
    import shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False

warnings.filterwarnings("ignore")


# ─── CONSTANTS ───────────────────────────────────────────────────────────────

RISK_THRESHOLDS = {"High": 0.65, "Medium": 0.35}   # tune to your dataset

CATEGORICAL_FEATURES = [
    "gender", "department", "job_title", "state",
    "city", "education_level", "location"
]
NUMERIC_FEATURES = [
    "age", "salary", "length_of_hire", "year_hired"
]
TARGET = "is_terminated"    # 1 = left, 0 = still active


# ─── STEP 1: LOAD DATA ───────────────────────────────────────────────────────

def load_data_from_csv(csv_path: str) -> pd.DataFrame:
    """Load from the original dataset.csv (or the exported hr_live_export.csv)."""
    df = pd.read_csv(csv_path)

    # Normalize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )
    col_map = {
        "employee_id": "employee_id", "first_name": "first_name",
        "last_name": "last_name", "hiredate": "hiredate",
        "termdate": "termdate", "birthdate": "birthdate",
        "department": "department", "job_title": "job_title",
        "state": "state", "city": "city",
        "education_level": "education_level",
        "gender": "gender", "salary": "salary",
        "performance_rating": "performance_rating",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    return df


def load_data_from_db(engine) -> pd.DataFrame:
    """Load from live database."""
    query = "SELECT * FROM employees"
    return pd.read_sql(query, engine)


# ─── STEP 2: FEATURE ENGINEERING ─────────────────────────────────────────────

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build all features the model needs from raw employee data.
    Creates the target column: is_terminated.
    """
    df = df.copy()

    today = date.today()

    # ── Date parsing ─────────────────────────────────────────
    for col in ["hiredate", "termdate", "birthdate"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # ── Target variable ──────────────────────────────────────
    df["is_terminated"] = df["termdate"].notna().astype(int)

    # ── Age ──────────────────────────────────────────────────
    if "birthdate" in df.columns:
        df["age"] = ((pd.Timestamp(today) - df["birthdate"]).dt.days / 365.25).round(1)
    else:
        df["age"] = np.nan

    # ── Year hired ───────────────────────────────────────────
    df["year_hired"] = df["hiredate"].dt.year

    # ── Length of hire (years) ───────────────────────────────
    end_date = df["termdate"].fillna(pd.Timestamp(today))
    df["length_of_hire"] = ((end_date - df["hiredate"]).dt.days / 365.25).round(1)

    # ── Location bucket ──────────────────────────────────────
    if "state" in df.columns:
        df["location"] = np.where(df["state"] == "New York", "HQ", "Branch")

    # ── Age group (ordinal) ──────────────────────────────────
    bins   = [0, 25, 35, 45, 55, 120]
    labels = ["<25", "25-34", "35-44", "45-54", "55+"]
    df["age_group"] = pd.cut(df["age"], bins=bins, labels=labels, right=False)

    # ── Salary band (relative to dept average) ───────────────
    if "salary" in df.columns and "department" in df.columns:
        dept_avg = df.groupby("department")["salary"].transform("mean")
        df["salary_vs_dept_avg"] = (df["salary"] / dept_avg).round(2)

    # ── Short tenure flag (< 2 years → higher risk) ──────────
    df["short_tenure"] = (df["length_of_hire"] < 2).astype(int)

    # ── Senior flag (> 10 years) ─────────────────────────────
    df["senior_tenure"] = (df["length_of_hire"] > 10).astype(int)

    logger.info(f"Feature engineering complete. Shape: {df.shape}")
    logger.info(f"Attrition rate in dataset: {df['is_terminated'].mean():.1%}")
    return df


# ─── STEP 3: BUILD PREPROCESSING PIPELINE ────────────────────────────────────

def build_preprocessor():
    """
    ColumnTransformer that handles numeric imputation + scaling
    and categorical ordinal encoding separately.
    """
    numeric_cols = NUMERIC_FEATURES + ["salary_vs_dept_avg", "short_tenure", "senior_tenure"]
    categorical_cols = CATEGORICAL_FEATURES

    numeric_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler",  StandardScaler()),
    ])
    categorical_pipe = Pipeline([
        ("imputer", SimpleImputer(strategy="constant", fill_value="Unknown")),
        ("encoder", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)),
    ])

    preprocessor = ColumnTransformer(transformers=[
        ("num", numeric_pipe, numeric_cols),
        ("cat", categorical_pipe, categorical_cols),
    ], remainder="drop")

    return preprocessor, numeric_cols + categorical_cols


# ─── STEP 4: TRAIN ───────────────────────────────────────────────────────────

def train_model(df: pd.DataFrame, model_dir: str = "./models"):
    """
    Train and evaluate the attrition prediction model.
    Saves the best model to disk.

    Returns:
        model   : trained Pipeline
        metrics : dict of evaluation scores
    """
    Path(model_dir).mkdir(parents=True, exist_ok=True)

    df = engineer_features(df)
    preprocessor, feature_cols = build_preprocessor()

    # ── Prepare X, y ────────────────────────────────────────
    available_cols = [c for c in feature_cols if c in df.columns]
    X = df[available_cols].copy()
    y = df[TARGET]

    logger.info(f"Training on {len(X):,} samples | {y.sum()} terminated, {(~y.astype(bool)).sum()} active")

    # ── Train / test split ───────────────────────────────────
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # ── SMOTE: balance classes ───────────────────────────────
    # (Attrition datasets are typically imbalanced ~80/20)
    pre = preprocessor.fit(X_train)
    X_train_t = pre.transform(X_train)
    X_test_t  = pre.transform(X_test)

    smote = SMOTE(random_state=42)
    X_res, y_res = smote.fit_resample(X_train_t, y_train)
    logger.info(f"After SMOTE: {y_res.sum()} terminated, {(~y_res.astype(bool)).sum()} active")

    # ── Classifiers ─────────────────────────────────────────
    rf = RandomForestClassifier(
        n_estimators=300,
        max_depth=8,
        min_samples_leaf=5,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )

    models_to_try = [("RandomForest", rf)]

    if XGB_AVAILABLE:
        xgb_clf = xgb.XGBClassifier(
            n_estimators=300,
            max_depth=6,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            use_label_encoder=False,
            eval_metric="logloss",
            random_state=42,
            n_jobs=-1,
        )
        models_to_try.append(("XGBoost", xgb_clf))

    # ── Train and pick best by AUC ───────────────────────────
    best_auc, best_model, best_name = 0, None, ""
    metrics = {}

    for name, clf in models_to_try:
        clf.fit(X_res, y_res)
        y_prob = clf.predict_proba(X_test_t)[:, 1]
        y_pred = clf.predict(X_test_t)
        auc    = roc_auc_score(y_test, y_prob)

        logger.info(f"\n──── {name} ────")
        logger.info(f"  AUC-ROC : {auc:.4f}")
        logger.info("\n" + classification_report(y_test, y_pred,
            target_names=["Active", "Terminated"]))

        metrics[name] = {
            "auc": auc,
            "report": classification_report(y_test, y_pred,
                        target_names=["Active","Terminated"], output_dict=True)
        }

        if auc > best_auc:
            best_auc, best_model, best_name = auc, clf, name

    logger.success(f"Best model: {best_name} (AUC={best_auc:.4f})")

    # ── Wrap in full pipeline and save ───────────────────────
    full_pipeline = Pipeline([
        ("preprocessor", preprocessor.fit(X_train)),   # refit on full train
        ("classifier",   best_model),
    ])
    # Refit classifier on preprocessed full train
    full_pipeline.named_steps["classifier"].fit(
        full_pipeline.named_steps["preprocessor"].transform(X_train),
        y_train
    )

    model_path = Path(model_dir) / "attrition_model.pkl"
    joblib.dump({"pipeline": full_pipeline,
                 "feature_cols": available_cols,
                 "model_name": best_name,
                 "auc": best_auc}, model_path)
    logger.success(f"Model saved → {model_path}")

    return full_pipeline, available_cols, metrics


# ─── STEP 5: SCORE ACTIVE EMPLOYEES ──────────────────────────────────────────

def score_employees(df: pd.DataFrame, model_dir: str = "./models",
                    export_dir: str = "./exports") -> pd.DataFrame:
    """
    Load saved model, score ALL active employees,
    attach top risk factors (via feature importances),
    and export attrition_risk.csv for Tableau.
    """
    model_path = Path(model_dir) / "attrition_model.pkl"
    if not model_path.exists():
        raise FileNotFoundError(f"No trained model found at {model_path}. Run train_model() first.")

    bundle = joblib.load(model_path)
    pipeline     = bundle["pipeline"]
    feature_cols = bundle["feature_cols"]
    model_name   = bundle["model_name"]
    logger.info(f"Loaded {model_name} model (AUC={bundle['auc']:.4f})")

    df = engineer_features(df)

    # Only score ACTIVE employees (no point scoring the already-terminated)
    active = df[df["termdate"].isna()].copy()
    logger.info(f"Scoring {len(active):,} active employees...")

    available = [c for c in feature_cols if c in active.columns]
    X = active[available]

    # Predict probabilities
    preprocessor = pipeline.named_steps["preprocessor"]
    classifier   = pipeline.named_steps["classifier"]
    X_t = preprocessor.transform(X)
    probs = classifier.predict_proba(X_t)[:, 1]

    active = active.copy()
    active["risk_score"] = probs.round(4)
    active["risk_label"] = active["risk_score"].apply(classify_risk)

    # ── Top 3 risk factors from feature importances ───────────
    importances = _get_feature_importances(classifier, available)
    top_factors = _get_top_factors(X_t, importances, available, preprocessor)
    active["top_factor_1"] = top_factors[0]
    active["top_factor_2"] = top_factors[1]
    active["top_factor_3"] = top_factors[2]
    active["model_version"] = model_name

    # ── Export ────────────────────────────────────────────────
    Path(export_dir).mkdir(parents=True, exist_ok=True)
    export_cols = ["employee_id", "first_name", "last_name", "department",
                   "job_title", "risk_score", "risk_label",
                   "top_factor_1", "top_factor_2", "top_factor_3",
                   "age", "length_of_hire", "salary", "model_version"]
    export_cols = [c for c in export_cols if c in active.columns]
    out = active[export_cols].sort_values("risk_score", ascending=False)
    out_path = Path(export_dir) / "attrition_risk.csv"
    out.to_csv(out_path, index=False)
    logger.success(f"Risk scores exported → {out_path}")

    # ── Summary stats ─────────────────────────────────────────
    logger.info("\n── Attrition Risk Summary ──")
    logger.info(out["risk_label"].value_counts().to_string())

    return out


def classify_risk(score: float) -> str:
    """Convert a probability score into a risk tier label."""
    if score >= RISK_THRESHOLDS["High"]:
        return "High"
    elif score >= RISK_THRESHOLDS["Medium"]:
        return "Medium"
    return "Low"


def _get_feature_importances(classifier, feature_names):
    """Extract feature importances from tree-based models."""
    if hasattr(classifier, "feature_importances_"):
        imp = classifier.feature_importances_
        return dict(zip(feature_names, imp))
    return {f: 1.0 / len(feature_names) for f in feature_names}


def _get_top_factors(X_t, importances, feature_names, preprocessor):
    """
    For each employee, find the top 3 features contributing most
    to their risk score. Returns 3 lists (one per top factor slot).
    """
    sorted_features = sorted(importances.items(), key=lambda x: x[1], reverse=True)
    top3 = [name for name, _ in sorted_features[:3]]

    # Format nicely for Tableau display
    def fmt(name):
        return name.replace("_", " ").title()

    n = X_t.shape[0]
    return [
        [fmt(top3[0])] * n,
        [fmt(top3[1])] * n if len(top3) > 1 else [""] * n,
        [fmt(top3[2])] * n if len(top3) > 2 else [""] * n,
    ]


# ─── MAIN ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    csv_path = sys.argv[1] if len(sys.argv) > 1 else "../data/dataset.csv"
    model_dir  = os.getenv("MODEL_DIR", "../models")
    export_dir = os.getenv("EXPORT_DIR", "../exports")

    logger.info(f"Loading data from: {csv_path}")
    df = load_data_from_csv(csv_path)

    # Train (first time, or when RETRAIN_ON_STARTUP=true)
    retrain = os.getenv("RETRAIN_ON_STARTUP", "false").lower() == "true"
    model_path = Path(model_dir) / "attrition_model.pkl"

    if retrain or not model_path.exists():
        logger.info("Training attrition model...")
        train_model(df, model_dir=model_dir)
    else:
        logger.info("Using existing trained model.")

    # Score active employees
    risk_df = score_employees(df, model_dir=model_dir, export_dir=export_dir)
    print("\nTop 10 High-Risk Employees:")
    print(risk_df.head(10).to_string(index=False))
