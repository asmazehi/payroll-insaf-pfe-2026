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

PEER_GROUP = ["grade_code", "ministry_code", "pa_eche"]


# ── Model loaders (lazy, cached) ──────────────────────────────────────────────

_salary_cache: dict = {}


def _load_salary():
    if not _salary_cache:
        _salary_cache["model"]      = joblib.load(MODELS_DIR / "salary_model.pkl")
        _salary_cache["encoders"]   = joblib.load(MODELS_DIR / "salary_encoders.pkl")
        _salary_cache["features"]   = joblib.load(MODELS_DIR / "salary_features.pkl")
        _salary_cache["emp_stats"]  = joblib.load(MODELS_DIR / "salary_emp_stats.pkl")
        _salary_cache["peer_stats"] = joblib.load(MODELS_DIR / "salary_peer_stats.pkl")
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

    Fallback priority for personal history:
      1. Employee's own historical mean (if employee_sk known)
      2. Peer group mean (same grade + ministry + echelon)
      3. Global mean (last resort)

    Returns: {
        predicted_netpay: float,
        used_personal_history: bool,
        used_peer_group: bool,
        peer_mean: float,   -- what this job slot typically pays
    }
    """
    m          = _load_salary()
    encoders   = m["encoders"]
    features   = m["features"]
    model      = m["model"]
    emp_stats  = m["emp_stats"]
    peer_stats = m["peer_stats"]

    global_mean = float(emp_stats["emp_mean"].mean())

    # ── Encode categoricals (handle unseen values gracefully) ─────────────────
    def safe_encode(le, value: str) -> int:
        value = str(value).strip()
        return int(le.transform([value])[0]) if value in le.classes_ else -1

    grade_enc    = safe_encode(encoders["grade_code"],    grade_code)
    nature_enc   = safe_encode(encoders["nature_code"],   nature_code)
    ministry_enc = safe_encode(encoders["ministry_code"], ministry_code)
    sitfam_enc   = safe_encode(encoders["pa_sitfam"],     pa_sitfam)
    grade_x_eche = grade_enc * pa_eche

    month_sin = np.sin(2 * np.pi * month_num / 12)
    month_cos = np.cos(2 * np.pi * month_num / 12)

    # ── Peer group lookup (grade + ministry + echelon) ────────────────────────
    peer_row = peer_stats[
        (peer_stats["grade_code"]    == str(grade_code).strip()) &
        (peer_stats["ministry_code"] == str(ministry_code).strip()) &
        (peer_stats["pa_eche"]       == float(pa_eche))
    ]
    if not peer_row.empty:
        peer_mean   = float(peer_row["peer_mean"].iloc[0])
        peer_median = float(peer_row["peer_median"].iloc[0])
        peer_std    = float(peer_row["peer_std"].iloc[0])
        peer_count  = float(peer_row["peer_count"].iloc[0])
    else:
        peer_mean = peer_median = global_mean
        peer_std  = 0.0
        peer_count = 0.0

    # ── Employee personal history ─────────────────────────────────────────────
    used_personal = False
    used_peer     = False

    if employee_sk is not None:
        emp_row = emp_stats[emp_stats["employee_sk"] == employee_sk]
        if not emp_row.empty:
            emp_mean   = float(emp_row["emp_mean"].iloc[0])
            emp_median = float(emp_row["emp_median"].iloc[0])
            emp_std    = float(emp_row["emp_std"].iloc[0])
            used_personal = True
        else:
            # New employee — fall back to peer group
            emp_mean = emp_median = peer_mean
            emp_std  = peer_std
            used_peer = True
    else:
        # No employee_sk given — use peer group
        emp_mean = emp_median = peer_mean
        emp_std  = peer_std
        used_peer = True

    # ── Build feature vector ──────────────────────────────────────────────────
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
        "peer_mean":         peer_mean,
        "peer_median":       peer_median,
        "peer_std":          peer_std,
        "peer_count":        peer_count,
    }

    X         = np.array([[row_data[f] for f in features]])
    predicted = float(model.predict(X)[0])

    return {
        "predicted_netpay":      round(predicted, 2),
        "peer_mean":             round(peer_mean, 2),
        "peer_group_size":       int(peer_count),
        "used_personal_history": used_personal,
        "used_peer_group":       used_peer,
    }


def predict_payroll_next_months(n_months: int = 6) -> list[dict]:
    """
    Forecast total payroll for the next N months.
    Always reads the latest data from the DW so forecasts are current.

    Returns: list of {"date": "YYYY-MM", "predicted_netpay": float}
    """
    from ml.data_loader import load_monthly_payroll

    m            = _load_forecast()
    model        = m["model"]
    feature_cols = m["features"]

    df        = load_monthly_payroll()
    df        = df.sort_values("month_start_date").reset_index(drop=True)
    y         = df["total_netpay"].values
    history   = list(y)
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
        row["yoy_growth"]     = (
            (history[-1] - history[-13]) / abs(history[-13])
            if len(history) >= 13 else 0
        )
        row["mom_delta"]  = history[-1] - history[-2] if len(history) >= 2 else 0
        row["month_sin"]  = np.sin(2 * np.pi * future_month / 12)
        row["month_cos"]  = np.cos(2 * np.pi * future_month / 12)
        row["year_norm"]  = (future_year - df["year_num"].min()) / (
                             df["year_num"].max() - df["year_num"].min() + 1)
        row["month_num"]  = future_month
        row["year_num"]   = future_year

        feat = np.array([[row.get(c, 0) for c in feature_cols]])
        pred = float(model.predict(feat)[0])
        preds.append({
            "date":               future_date.strftime("%Y-%m"),
            "predicted_netpay":   round(pred, 2),
        })
        history.append(pred)

    return preds


def flag_anomalies(df_new: pd.DataFrame) -> pd.DataFrame:
    """
    Flag anomalies in a new batch of payroll records.

    Input : DataFrame with columns matching load_individual_payroll() output.
    Output: same DataFrame + columns:
              z_score, pct_deviation, zscore_flag,
              if_score, if_flag, anomaly_flag
    """
    m        = _load_anomaly()
    model    = m["model"]
    scaler   = m["scaler"]
    encoders = m["encoders"]
    features = m["features"]

    df = df_new.copy()

    for col in ["grade_code", "nature_code", "ministry_code"]:
        le = encoders[col]
        df[col] = df[col].fillna("UNKNOWN").astype(str).str.strip()
        df[col + "_enc"] = df[col].apply(
            lambda v, le=le: int(le.transform([v])[0]) if v in le.classes_ else -1
        )

    for col in ["pa_eche", "year_num", "month_num"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

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
    df["zscore_flag"]  = df["z_score"].abs() > 3.0

    X_scaled           = scaler.transform(df[features].values.astype(float))
    df["if_score"]     = model.score_samples(X_scaled)
    df["if_flag"]      = model.predict(X_scaled) == -1
    df["anomaly_flag"] = df["zscore_flag"] | df["if_flag"]

    return df
