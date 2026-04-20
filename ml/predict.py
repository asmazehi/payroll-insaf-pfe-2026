"""
ml/predict.py
=============
Inference layer — loads saved models and makes predictions.
Called by the API endpoints. No retraining here.

Available:
    predict_payroll_next_months(n)  -> Model 1 forecast
    flag_anomalies(df)              -> Model 4 anomaly flags
"""
from __future__ import annotations

from pathlib import Path

import joblib
import numpy as np
import pandas as pd

MODELS_DIR = Path(__file__).resolve().parent / "models"

_forecast_cache: dict = {}
_anomaly_cache:  dict = {}


# ── Loaders ───────────────────────────────────────────────────────────────────

def _load_forecast():
    if not _forecast_cache:
        _forecast_cache["model"]    = joblib.load(MODELS_DIR / "payroll_forecast.pkl")
        _forecast_cache["features"] = joblib.load(MODELS_DIR / "payroll_forecast_features.pkl")
        _forecast_cache["winner"]   = joblib.load(MODELS_DIR / "payroll_forecast_winner.pkl")
    return _forecast_cache


def _load_anomaly():
    if not _anomaly_cache:
        _anomaly_cache["model"]    = joblib.load(MODELS_DIR / "anomaly_model.pkl")
        _anomaly_cache["scaler"]   = joblib.load(MODELS_DIR / "anomaly_scaler.pkl")
        _anomaly_cache["encoders"] = joblib.load(MODELS_DIR / "anomaly_encoders.pkl")
        _anomaly_cache["features"] = joblib.load(MODELS_DIR / "anomaly_features.pkl")
    return _anomaly_cache


# ── Public API ────────────────────────────────────────────────────────────────

def predict_payroll_next_months(n_months: int = 6) -> list[dict]:
    """
    Forecast total payroll for next N months using the winning model.
    Reads latest data from DW so forecasts are always current.
    """
    from ml.data_loader import load_monthly_payroll
    import numpy as np

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
        for lag in range(1, 13):
            key = f"lag_{lag}"
            if key in feature_cols:
                row[key] = history[-lag] if lag <= len(history) else 0
        row["rolling_mean_3"]  = np.mean(history[-3:])
        row["rolling_mean_6"]  = np.mean(history[-6:])
        row["rolling_mean_12"] = np.mean(history[-12:])
        row["rolling_std_3"]   = np.std(history[-3:])
        row["yoy_growth"]      = (history[-1] - history[-13]) / abs(history[-13]) \
                                  if len(history) >= 13 else 0
        row["mom_delta"]  = history[-1] - history[-2] if len(history) >= 2 else 0
        row["month_sin"]  = np.sin(2 * np.pi * future_month / 12)
        row["month_cos"]  = np.cos(2 * np.pi * future_month / 12)
        row["year_norm"]  = (future_year - int(df["year_num"].min())) / max(
                             int(df["year_num"].max()) - int(df["year_num"].min()), 1)
        row["month_num"]  = future_month
        row["year_num"]   = future_year

        feat = np.array([[row.get(c, 0) for c in feature_cols]])
        pred = float(model.predict(feat)[0])
        preds.append({
            "date":             future_date.strftime("%Y-%m"),
            "predicted_netpay": round(pred, 2),
        })
        history.append(pred)

    return preds


def flag_anomalies(df_new: pd.DataFrame) -> pd.DataFrame:
    """
    Flag anomalies in a new batch of payroll records.
    Input : DataFrame matching load_individual_payroll() schema.
    Output: same DataFrame + z_score, zscore_flag, if_score, if_flag, anomaly_flag
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
    df["zscore_flag"] = df["z_score"].abs() > 3.0

    X_scaled       = scaler.transform(df[features].values.astype(float))
    df["if_score"] = model.score_samples(X_scaled)
    df["if_flag"]  = model.predict(X_scaled) == -1
    df["anomaly_flag"] = df["zscore_flag"] | df["if_flag"]

    return df
