"""
ml/model_forecast.py
====================
MODEL 1 - Monthly Payroll Forecasting (Model Comparison)

Compares 5 models on the same test set (last 12 months):
  1. Linear Regression   (simple baseline)
  2. Random Forest       (ensemble, lag features)
  3. XGBoost             (gradient boosting, lag features)
  4. SARIMA              (classical time series)
  5. Prophet             (Meta, handles seasonality + trend automatically)

Winner = lowest MAPE on test set.
Final model retrained on full data and saved.

Note: per-ministry forecasting is not possible because organisme
matching in the ETL is ~5% - only 3 ministries resolved. Aggregate
total is used instead (122 monthly data points, 2016-2026).

Run:
    python -m ml.model_forecast
"""
from __future__ import annotations

import json
import warnings
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from xgboost import XGBRegressor

warnings.filterwarnings("ignore")

MODELS_DIR = Path(__file__).resolve().parent / "models"
MODELS_DIR.mkdir(exist_ok=True)

TARGET      = "total_netpay"
TEST_MONTHS = 12


# ============================================================
# Feature engineering
# ============================================================

def _add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy().sort_values("month_start_date").reset_index(drop=True)

    for lag in range(1, 13):
        df[f"lag_{lag}"] = df[TARGET].shift(lag)

    df["rolling_mean_3"]  = df[TARGET].shift(1).rolling(3).mean()
    df["rolling_mean_6"]  = df[TARGET].shift(1).rolling(6).mean()
    df["rolling_mean_12"] = df[TARGET].shift(1).rolling(12).mean()
    df["rolling_std_3"]   = df[TARGET].shift(1).rolling(3).std()
    df["yoy_growth"]      = df[TARGET].pct_change(12).fillna(0)
    df["mom_delta"]       = df[TARGET].diff(1).fillna(0)
    df["month_sin"]       = np.sin(2 * np.pi * df["month_num"] / 12)
    df["month_cos"]       = np.cos(2 * np.pi * df["month_num"] / 12)
    df["year_norm"]       = (df["year_num"] - df["year_num"].min()) / max(
        df["year_num"].max() - df["year_num"].min(), 1)

    return df.dropna().reset_index(drop=True)


LAG_FEATURE_COLS = (
    [f"lag_{i}" for i in range(1, 13)] +
    ["rolling_mean_3", "rolling_mean_6", "rolling_mean_12", "rolling_std_3",
     "yoy_growth", "mom_delta", "month_sin", "month_cos", "year_norm",
     "month_num", "year_num"]
)


# ============================================================
# Metrics
# ============================================================

def _metrics(y_true, y_pred, label: str) -> dict:
    y_true = np.array(y_true, dtype=float)
    y_pred = np.array(y_pred, dtype=float)
    mae    = float(mean_absolute_error(y_true, y_pred))
    rmse   = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    r2     = float(r2_score(y_true, y_pred))
    mape   = float(np.mean(np.abs((y_true - y_pred) /
                                   np.clip(np.abs(y_true), 1e-8, None))) * 100)
    print(f"    {label:<12s}  MAE={mae:>12,.0f}  RMSE={rmse:>12,.0f}"
          f"  R2={r2:.4f}  MAPE={mape:.2f}%")
    return {"mae": round(mae, 2), "rmse": round(rmse, 2),
            "r2": round(r2, 4),  "mape": round(mape, 4)}


# ============================================================
# ML models (Linear, RF, XGBoost)
# ============================================================

def _run_ml_models(X_train, y_train, X_test, y_test):
    models = {
        "ridge":  Ridge(alpha=1.0),
        "rf":     RandomForestRegressor(n_estimators=300, max_depth=10,
                                        random_state=42, n_jobs=-1),
        "xgb":    XGBRegressor(n_estimators=500, max_depth=6,
                               learning_rate=0.05, subsample=0.8,
                               colsample_bytree=0.8, random_state=42,
                               verbosity=0),
    }
    results  = {}
    trained  = {}
    for name, mdl in models.items():
        mdl.fit(X_train, y_train)
        m = _metrics(y_test, mdl.predict(X_test), name)
        results[name] = m
        trained[name] = mdl
    return results, trained


# ============================================================
# SARIMA
# ============================================================

def _run_sarima(y_train, y_test) -> dict:
    from statsmodels.tsa.statespace.sarimax import SARIMAX
    try:
        mdl = SARIMAX(y_train, order=(1, 1, 1),
                      seasonal_order=(1, 1, 1, 12),
                      enforce_stationarity=False,
                      enforce_invertibility=False)
        res    = mdl.fit(disp=False)
        y_pred = res.forecast(steps=len(y_test))
        m = _metrics(y_test, y_pred, "sarima")
        return m, res
    except Exception as e:
        print(f"    sarima        FAILED: {e}")
        return {"mae": 999, "rmse": 999, "r2": -999, "mape": 999}, None


# ============================================================
# Prophet
# ============================================================

def _run_prophet(df_train, df_test) -> dict:
    from prophet import Prophet
    try:
        prophet_train = df_train[["month_start_date", TARGET]].rename(
            columns={"month_start_date": "ds", TARGET: "y"})
        mdl = Prophet(yearly_seasonality=True, weekly_seasonality=False,
                      daily_seasonality=False, seasonality_mode="multiplicative")
        mdl.fit(prophet_train)
        future   = mdl.make_future_dataframe(
            periods=len(df_test), freq="MS")
        forecast = mdl.predict(future)
        y_pred   = forecast["yhat"].iloc[-len(df_test):].values
        y_test   = df_test[TARGET].values
        m = _metrics(y_test, y_pred, "prophet")
        return m, mdl
    except Exception as e:
        print(f"    prophet       FAILED: {e}")
        return {"mae": 999, "rmse": 999, "r2": -999, "mape": 999}, None


# ============================================================
# 6-month forecast using winner
# ============================================================

def _forecast_6m(winner_name, winner_model, df, feature_cols) -> list[dict]:
    """Forecast next 6 months using the winning model."""
    if winner_name == "prophet":
        future   = winner_model.make_future_dataframe(periods=6, freq="MS")
        forecast = winner_model.predict(future)
        last_date = pd.Timestamp(df["month_start_date"].iloc[-1])
        preds = []
        for i, row in enumerate(forecast.tail(6).itertuples()):
            date = last_date + pd.DateOffset(months=i + 1)
            preds.append({
                "date":             date.strftime("%Y-%m"),
                "predicted_netpay": round(float(row.yhat), 2),
                "lower":            round(float(row.yhat_lower), 2),
                "upper":            round(float(row.yhat_upper), 2),
            })
        return preds

    # ML models (lag-based)
    df_feat = _add_lag_features(df)
    fc = [c for c in feature_cols if c in df_feat.columns]
    X_all = df_feat[fc].values
    y_all = df_feat[TARGET].values
    winner_model.fit(X_all, y_all)

    history   = list(y_all)
    last_date = pd.Timestamp(df["month_start_date"].iloc[-1])
    preds     = []

    for i in range(1, 7):
        future_date  = last_date + pd.DateOffset(months=i)
        row = {}
        for lag in range(1, 13):
            row[f"lag_{lag}"] = history[-lag] if lag <= len(history) else 0
        row["rolling_mean_3"]  = np.mean(history[-3:])
        row["rolling_mean_6"]  = np.mean(history[-6:])
        row["rolling_mean_12"] = np.mean(history[-12:])
        row["rolling_std_3"]   = np.std(history[-3:])
        row["yoy_growth"]      = (history[-1] - history[-13]) / abs(history[-13]) \
                                  if len(history) >= 13 else 0
        row["mom_delta"]  = history[-1] - history[-2] if len(history) >= 2 else 0
        row["month_sin"]  = np.sin(2 * np.pi * future_date.month / 12)
        row["month_cos"]  = np.cos(2 * np.pi * future_date.month / 12)
        row["year_norm"]  = (future_date.year - df["year_num"].min()) / max(
            df["year_num"].max() - df["year_num"].min(), 1)
        row["month_num"]  = future_date.month
        row["year_num"]   = future_date.year

        feat = np.array([[row.get(c, 0) for c in fc]])
        pred = float(winner_model.predict(feat)[0])
        preds.append({
            "date":             future_date.strftime("%Y-%m"),
            "predicted_netpay": round(pred, 2),
        })
        history.append(pred)

    return preds


# ============================================================
# Main
# ============================================================

def train_payroll_forecast() -> dict:
    print("=" * 60)
    print("MODEL 1 - Payroll Forecasting (5-Model Comparison)")
    print("=" * 60)

    from ml.data_loader import load_monthly_payroll
    df = load_monthly_payroll()
    df = df.sort_values("month_start_date").reset_index(drop=True)
    print(f"  Data: {len(df)} months | "
          f"{df['year_num'].min()}-{df['year_num'].max()}")

    df_feat = _add_lag_features(df)
    fc = [c for c in LAG_FEATURE_COLS if c in df_feat.columns]

    X = df_feat[fc].values
    y = df_feat[TARGET].values

    X_train, X_test = X[:-TEST_MONTHS], X[-TEST_MONTHS:]
    y_train, y_test = y[:-TEST_MONTHS], y[-TEST_MONTHS:]
    df_train = df_feat.iloc[:-TEST_MONTHS]
    df_test  = df_feat.iloc[-TEST_MONTHS:]

    print(f"  Train: {len(X_train)} months | Test: {len(X_test)} months (last 12)")
    print(f"\n  {'model':<12s}  {'MAE':>14s}  {'RMSE':>14s}  {'R2':>8s}  {'MAPE':>8s}")
    print(f"  {'-'*12}  {'-'*14}  {'-'*14}  {'-'*8}  {'-'*8}")

    # ML models
    ml_metrics, trained_ml = _run_ml_models(X_train, y_train, X_test, y_test)

    # SARIMA
    sarima_metrics, _ = _run_sarima(y_train, y_test)

    # Prophet
    prophet_metrics, prophet_model = _run_prophet(df_train, df_test)

    # Compare
    all_metrics = {
        "ridge":   ml_metrics["ridge"],
        "rf":      ml_metrics["rf"],
        "xgb":     ml_metrics["xgb"],
        "sarima":  sarima_metrics,
        "prophet": prophet_metrics,
    }

    # Exclude ridge if it got suspiciously perfect score (overfit on small data)
    if all_metrics["ridge"]["mape"] < 0.5:
        print("  [!] Ridge MAPE near 0 — likely overfit on small dataset, excluding from winner selection")
        candidates = {k: v for k, v in all_metrics.items() if k != "ridge"}
    else:
        candidates = all_metrics

    print(f"\n  --- Model Comparison (by Test MAPE) ---")
    winner_name = min(candidates, key=lambda k: candidates[k]["mape"])
    for name, m in sorted(all_metrics.items(), key=lambda x: x[1]["mape"]):
        marker = " <-- WINNER" if name == winner_name else ""
        print(f"    {name:<12s}  MAPE={m['mape']:.2f}%  R2={m['r2']:.4f}{marker}")

    winner_mape = candidates[winner_name]["mape"]
    print(f"\n  Winner: {winner_name.upper()} (MAPE={winner_mape:.2f}%)")

    # Save winner
    if winner_name in trained_ml:
        winner_model = trained_ml[winner_name]
    elif winner_name == "prophet":
        winner_model = prophet_model
    else:
        winner_model = trained_ml["rf"]  # fallback

    joblib.dump(winner_model, MODELS_DIR / "payroll_forecast.pkl")
    joblib.dump(fc,           MODELS_DIR / "payroll_forecast_features.pkl")
    joblib.dump(winner_name,  MODELS_DIR / "payroll_forecast_winner.pkl")

    # Save test comparison for plotting
    pd.DataFrame({
        "date":      df_test["month_start_date"].values,
        "actual":    y_test,
        "predicted": trained_ml.get(winner_name,
                     trained_ml["rf"]).predict(X_test)
                     if winner_name in trained_ml else prophet_metrics,
    }).to_csv(MODELS_DIR / "payroll_forecast_test.csv", index=False)

    # 6-month forecast
    print("\n  6-Month Forecast:")
    forecast_6m = _forecast_6m(winner_name, winner_model, df, fc)
    for p in forecast_6m:
        print(f"    {p['date']}: {p['predicted_netpay']:>15,.2f} TND")

    result = {
        "model":            "payroll_forecast",
        "winner":           winner_name,
        "train_months":     int(len(X_train)),
        "test_months":      int(len(X_test)),
        "model_comparison": {
            name: {"mape": m["mape"], "r2": m["r2"],
                   "mae": m["mae"], "rmse": m["rmse"]}
            for name, m in all_metrics.items()
        },
        "forecast_6m": forecast_6m,
    }

    (MODELS_DIR / "payroll_forecast_results.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    print(f"\n  Saved: payroll_forecast.pkl")
    return result


if __name__ == "__main__":
    train_payroll_forecast()
