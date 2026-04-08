"""
ml/model_anomaly.py
===================
Anomaly detection for individual payroll records.
Flags employees whose net pay deviated abnormally from their personal baseline.

Two complementary approaches:
  1. Z-score per employee (statistical baseline)
  2. Isolation Forest (unsupervised, captures multivariate outliers)

Run:
    python -m ml.model_anomaly
"""
from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import LabelEncoder, StandardScaler

from ml.data_loader import load_individual_payroll

MODELS_DIR = Path(__file__).resolve().parent.parent / "ml" / "models"
MODELS_DIR.mkdir(exist_ok=True)

# Thresholds
ZSCORE_THRESHOLD   = 3.0   # flag if personal z-score > 3σ
IF_CONTAMINATION   = 0.02  # expect ~2% anomalies in Isolation Forest


def _compute_employee_baseline(df: pd.DataFrame) -> pd.DataFrame:
    """Add per-employee rolling baseline columns."""
    df = df.sort_values(["employee_sk", "year_num", "month_num"]).copy()

    grp = df.groupby("employee_sk")["m_netpay"]
    df["emp_mean"]   = grp.transform("mean")
    df["emp_std"]    = grp.transform("std").fillna(0)
    df["emp_median"] = grp.transform("median")
    df["emp_count"]  = grp.transform("count")

    # Z-score: how many std devs from personal mean
    df["z_score"] = np.where(
        df["emp_std"] > 0,
        (df["m_netpay"] - df["emp_mean"]) / df["emp_std"],
        0.0,
    )
    # Absolute % deviation from personal median
    df["pct_deviation"] = np.where(
        df["emp_median"] > 0,
        np.abs(df["m_netpay"] - df["emp_median"]) / df["emp_median"] * 100,
        0.0,
    )
    df["zscore_flag"] = df["z_score"].abs() > ZSCORE_THRESHOLD
    return df


def train_anomaly_model() -> dict:
    print("=" * 55)
    print("MODEL 4 — Payroll Anomaly Detection")
    print("=" * 55)

    df = load_individual_payroll()
    print(f"  Data loaded: {len(df):,} records, {df['employee_sk'].nunique():,} employees")

    # ── Step 1: per-employee z-score baseline ────────────────────────────────
    df = _compute_employee_baseline(df)

    n_zscore = df["zscore_flag"].sum()
    print(f"\n  Z-score anomalies (|z| > {ZSCORE_THRESHOLD}): {n_zscore:,} "
          f"({n_zscore / len(df) * 100:.2f}%)")

    # ── Step 2: Isolation Forest on feature matrix ───────────────────────────
    cat_cols = ["grade_code", "nature_code", "ministry_code"]
    encoders = {}
    for col in cat_cols:
        df[col] = df[col].fillna("UNKNOWN").astype(str).str.strip()
        le = LabelEncoder()
        df[col + "_enc"] = le.fit_transform(df[col])
        encoders[col] = le

    num_cols = ["pa_eche", "year_num", "month_num"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    feature_cols = (
        [c + "_enc" for c in cat_cols] +
        num_cols +
        ["emp_mean", "emp_std", "emp_count", "z_score", "pct_deviation"]
    )

    X = df[feature_cols].values.astype(float)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    iso = IsolationForest(
        n_estimators=200,
        contamination=IF_CONTAMINATION,
        random_state=42,
        n_jobs=-1,
    )
    iso.fit(X_scaled)

    df["if_score"]  = iso.score_samples(X_scaled)   # more negative = more anomalous
    df["if_flag"]   = iso.predict(X_scaled) == -1   # -1 = anomaly

    n_if = df["if_flag"].sum()
    print(f"  Isolation Forest anomalies (~{IF_CONTAMINATION*100:.0f}%): {n_if:,} "
          f"({n_if / len(df) * 100:.2f}%)")

    # ── Step 3: Combined flag ─────────────────────────────────────────────────
    df["anomaly_flag"] = df["zscore_flag"] | df["if_flag"]
    n_combined = df["anomaly_flag"].sum()
    print(f"  Combined anomalies (z-score OR IF): {n_combined:,} "
          f"({n_combined / len(df) * 100:.2f}%)")

    # ── Step 4: Top anomalies summary ────────────────────────────────────────
    anomalies = (
        df[df["anomaly_flag"]]
        .sort_values("z_score", key=abs, ascending=False)
        [["employee_sk", "year_num", "month_num", "m_netpay",
          "emp_mean", "z_score", "pct_deviation", "if_score",
          "zscore_flag", "if_flag", "grade_code", "nature_code", "ministry_code"]]
        .head(20)
    )

    print("\n  Top 10 anomalies by |z-score|:")
    print(f"  {'employee_sk':>12s} {'year':>5s} {'month':>5s} "
          f"{'netpay':>12s} {'emp_mean':>12s} {'z_score':>8s} {'pct_dev':>8s}")
    for _, row in anomalies.head(10).iterrows():
        print(f"  {int(row['employee_sk']):>12d} {int(row['year_num']):>5d} "
              f"{int(row['month_num']):>5d} {row['m_netpay']:>12,.2f} "
              f"{row['emp_mean']:>12,.2f} {row['z_score']:>8.2f} "
              f"{row['pct_deviation']:>7.1f}%")

    # ── Step 5: Grade/ministry breakdown of anomalies ─────────────────────────
    print("\n  Anomalies by ministry (top 10):")
    ministry_counts = (
        df[df["anomaly_flag"]]
        .groupby("ministry_code")
        .size()
        .sort_values(ascending=False)
        .head(10)
    )
    for ministry, count in ministry_counts.items():
        pct = count / n_combined * 100
        print(f"    {ministry:<20s} {count:>6,} ({pct:.1f}%)")

    # ── Save ──────────────────────────────────────────────────────────────────
    joblib.dump(iso,          MODELS_DIR / "anomaly_model.pkl")
    joblib.dump(scaler,       MODELS_DIR / "anomaly_scaler.pkl")
    joblib.dump(encoders,     MODELS_DIR / "anomaly_encoders.pkl")
    joblib.dump(feature_cols, MODELS_DIR / "anomaly_features.pkl")

    # Save anomaly report (top 500)
    report_df = (
        df[df["anomaly_flag"]]
        .sort_values("z_score", key=abs, ascending=False)
        [["employee_sk", "year_num", "month_num", "m_netpay",
          "emp_mean", "emp_median", "z_score", "pct_deviation",
          "if_score", "zscore_flag", "if_flag",
          "grade_code", "nature_code", "ministry_code"]]
        .head(500)
    )
    report_df.to_csv(MODELS_DIR / "anomaly_report.csv", index=False)

    result = {
        "model":                "anomaly_detection",
        "total_records":        int(len(df)),
        "total_employees":      int(df["employee_sk"].nunique()),
        "zscore_threshold":     ZSCORE_THRESHOLD,
        "if_contamination":     IF_CONTAMINATION,
        "zscore_anomalies":     int(n_zscore),
        "if_anomalies":         int(n_if),
        "combined_anomalies":   int(n_combined),
        "anomaly_rate_pct":     round(n_combined / len(df) * 100, 4),
        "top_anomalies": anomalies[["employee_sk", "year_num", "month_num",
                                    "m_netpay", "emp_mean", "z_score",
                                    "pct_deviation"]].head(20).to_dict(orient="records"),
    }
    (MODELS_DIR / "anomaly_results.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )
    print(f"\n  Model saved: {MODELS_DIR / 'anomaly_model.pkl'}")
    print(f"  Report saved: {MODELS_DIR / 'anomaly_report.csv'}")
    return result


if __name__ == "__main__":
    train_anomaly_model()
