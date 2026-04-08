"""
ml/predict.py
=============
Inference layer — loads saved models and makes predictions on new data.
Called by the web platform API endpoints.

No retraining here. Models must already be trained via ml/run_all_models.py.

Usage:
    from ml.predict import predict_salary, predict_payroll_next_months, flag_anomalies
"""
from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np
import pandas as pd

MODELS_DIR = Path(__file__).resolve().parent / "models"


# ── Model loaders (lazy, cached) ──────────────────────────────────────────────

_salary_cache: dict = {}


def _load_salary():
    if not _salary_cache:
        _salary_cache["model"]     = joblib.load(MODELS_DIR / "salary_model.pkl")
        _salary_cache["encoders"]  = joblib.load(MODELS_DIR / "salary_encoders.pkl")
        _salary_cache["features"]  = joblib.load(MODELS_DIR / "salary_features.pkl")
        _salary_cache["emp_stats"] = joblib.load(MODELS_DIR / "salary_emp_stats.pkl")
    return _salary_cache


def _load_forecast():
    return {
        "model":    joblib.load(MODELS_DIR / "payroll_forecast.pkl"),
        "features": joblib.load(MODELS_DIR / "payroll_forecast_features.pkl"),
    }


def _load_anomaly():
    return {
        "model":    joblib.load(MODELS_DIR / "anomaly_model.pkl"),
        "scaler":   joblib.load(MODELS_DIR / "anomaly_scaler.pkl"),
        "encoders": joblib.load(MODELS_DIR / "anomaly_encoders.pkl"),
        "features": joblib.load(MODELS_DIR / "anomaly_features.pkl"),
    }


# ── Public API ────────────────────────────────────────────────────────────────

def predict_salary(
    grade_code: str,
    nature_code: str,
    ministry_code: str,
    pa_sitfam: str,
    pa_eche: float,
    year_num: int,
    month_num: int,
    employee_sk: int | None = None,
) -> dict:
    """
    Predict net salary for a single employee.
    If employee_sk is provided and known, uses their personal history.
    Otherwise falls back to population average.

    Returns: {"predicted_netpay": float, "used_personal_history": bool}
    """
    m = _load_salary()
    encoders  = m["encoders"]
    features  = m["features"]
    model     = m["model"]
    emp_stats = m["emp_stats"]

    # Encode categoricals — handle unseen values gracefully
    def safe_encode(le, value: str) -> int:
        value = str(value).strip()
        if value in le.classes_:
            return int(le.transform([value])[0])
        return -1  # unseen category → -1 (XGBoost handles this)

    grade_enc    = safe_encode(encoders["grade_code"],    grade_code)
    nature_enc   = safe_encode(encoders["nature_code"],   nature_code)
    ministry_enc = safe_encode(encoders["ministry_code"], ministry_code)
    sitfam_enc   = safe_encode(encoders["pa_sitfam"],     pa_sitfam)
    grade_x_eche = grade_enc * pa_eche

    month_sin = np.sin(2 * np.pi * month_num / 12)
    month_cos = np.cos(2 * np.pi * month_num / 12)

    # Employee personal history
    used_personal = False
    if employee_sk is not None:
        row = emp_stats[emp_stats["employee_sk"] == employee_sk]
        if not row.empty:
            emp_mean   = float(row["emp_mean"].iloc[0])
            emp_median = float(row["emp_median"].iloc[0])
            emp_std    = float(row["emp_std"].iloc[0])
            used_personal = True
        else:
            emp_mean = emp_median = float(emp_stats["emp_mean"].mean())
            emp_std  = 0.0
    else:
        emp_mean = emp_median = float(emp_stats["emp_mean"].mean())
        emp_std  = 0.0

    row_data = {
        "grade_code_enc":    grade_enc,
        "nature_code_enc":   nature_enc,
        "ministry_code_enc": ministry_enc,
        "pa_sitfam_enc":     sitfam_enc,
        "pa_eche":           pa_eche,
        "grade_x_eche":      grade_x_eche,
        "year_num":          year_num,
        "month_sin":         month_sin,
        "month_cos":         month_cos,
        "emp_mean":          emp_mean,
        "emp_median":        emp_median,
        "emp_std":           emp_std,
    }

    X = np.array([[row_data[f] for f in features]])
    predicted = float(model.predict(X)[0])

    return {
        "predicted_netpay":     round(predicted, 2),
        "used_personal_history": used_personal,
    }


def predict_payroll_next_months(n_months: int = 6) -> list[dict]:
    """
    Forecast total payroll for the next N months.
    Queries current DW data to get latest history, then forecasts forward.

    Returns: list of {"date": "YYYY-MM", "predicted_netpay": float}
    """
    from ml.data_loader import load_monthly_payroll

    m = _load_forecast()
    model        = m["model"]
    feature_cols = m["features"]

    df = load_monthly_payroll()
    target = "total_netpay"

    # Rebuild lag/rolling features from current history
    df = df.sort_values("month_start_date").reset_index(drop=True)
    y       = df[target].values
    history = list(y)
    last_date = pd.Timestamp(df["month_start_date"].iloc[-1])

    preds = []
    for i in range(1, n_months + 1):
        future_date  = last_date + pd.DateOffset(months=i)
        future_month = future_date.month
        future_year  = future_date.year

        row = {}
        for lag in range(1, 7):
            key = f"lag_{lag}"
            if key in feature_cols:
                row[key] = history[-lag] if lag <= len(history) else 0
        row["rolling_mean_3"] = np.mean(history[-3:])
        row["rolling_mean_6"] = np.mean(history[-6:])
        row["rolling_std_3"]  = np.std(history[-3:])
        row["yoy_growth"]     = (history[-1] - history[-13]) / abs(history[-13]) if len(history) >= 13 else 0
        row["mom_delta"]      = history[-1] - history[-2] if len(history) >= 2 else 0
        row["month_sin"]      = np.sin(2 * np.pi * future_month / 12)
        row["month_cos"]      = np.cos(2 * np.pi * future_month / 12)
        row["year_norm"]      = (future_year - df["year_num"].min()) / (
                                 df["year_num"].max() - df["year_num"].min() + 1)
        row["month_num"]      = future_month
        row["year_num"]       = future_year

        feat = np.array([[row.get(c, 0) for c in feature_cols]])
        pred = float(model.predict(feat)[0])
        preds.append({"date": future_date.strftime("%Y-%m"), "predicted_netpay": round(pred, 2)})
        history.append(pred)

    return preds


def flag_anomalies(df_new: pd.DataFrame) -> pd.DataFrame:
    """
    Flag anomalies in a new batch of payroll records.

    Input: DataFrame with columns matching load_individual_payroll() output.
    Returns: same DataFrame with added columns:
        - z_score, pct_deviation, zscore_flag
        - if_score, if_flag
        - anomaly_flag
    """
    m = _load_anomaly()
    model    = m["model"]
    scaler   = m["scaler"]
    encoders = m["encoders"]
    features = m["features"]

    df = df_new.copy()

    # Encode categoricals
    for col in ["grade_code", "nature_code", "ministry_code"]:
        le = encoders[col]
        df[col] = df[col].fillna("UNKNOWN").astype(str).str.strip()
        df[col + "_enc"] = df[col].apply(
            lambda v: int(le.transform([v])[0]) if v in le.classes_ else -1
        )

    for col in ["pa_eche", "year_num", "month_num"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Employee baseline
    grp = df.groupby("employee_sk")["m_netpay"]
    df["emp_mean"]   = grp.transform("mean")
    df["emp_std"]    = grp.transform("std").fillna(0)
    df["emp_median"] = grp.transform("median")
    df["emp_count"]  = grp.transform("count")

    df["z_score"] = np.where(
        df["emp_std"] > 0,
        (df["m_netpay"] - df["emp_mean"]) / df["emp_std"],
        0.0,
    )
    df["pct_deviation"] = np.where(
        df["emp_median"] > 0,
        np.abs(df["m_netpay"] - df["emp_median"]) / df["emp_median"] * 100,
        0.0,
    )
    df["zscore_flag"] = df["z_score"].abs() > 3.0

    X = df[features].values.astype(float)
    X_scaled = scaler.transform(X)

    df["if_score"] = model.score_samples(X_scaled)
    df["if_flag"]  = model.predict(X_scaled) == -1
    df["anomaly_flag"] = df["zscore_flag"] | df["if_flag"]

    return df
