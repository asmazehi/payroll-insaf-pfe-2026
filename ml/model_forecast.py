"""
ml/model_forecast.py
====================
Time series forecasting for total monthly payroll and indemnity.
Model: Random Forest Regressor with lag features.

Evaluation: last 12 months as holdout test set (proper for small time series).

Run:
    python -m ml.model_forecast
"""
from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from ml.data_loader import load_monthly_payroll, load_monthly_indemnity

MODELS_DIR = Path(__file__).resolve().parent.parent / "ml" / "models"
MODELS_DIR.mkdir(exist_ok=True)


def _add_lag_features(df: pd.DataFrame, target: str, lags: int = 6) -> pd.DataFrame:
    df = df.copy().sort_values("month_start_date").reset_index(drop=True)

    # Lag features — "what was payroll 1, 2, ... 6 months ago?"
    for i in range(1, lags + 1):
        df[f"lag_{i}"] = df[target].shift(i)

    # Rolling statistics
    df["rolling_mean_3"] = df[target].shift(1).rolling(3).mean()
    df["rolling_mean_6"] = df[target].shift(1).rolling(6).mean()
    df["rolling_std_3"]  = df[target].shift(1).rolling(3).std()

    # Year-over-year: this month vs same month last year
    df["yoy_growth"] = df[target].pct_change(12).fillna(0)

    # Month-over-month delta
    df["mom_delta"] = df[target].diff(1).fillna(0)

    # Calendar features
    df["month_sin"] = np.sin(2 * np.pi * df["month_num"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month_num"] / 12)
    df["year_norm"] = (df["year_num"] - df["year_num"].min()) / (
        df["year_num"].max() - df["year_num"].min() + 1
    )

    return df.dropna().reset_index(drop=True)


def _evaluate(y_true, y_pred, label: str) -> dict:
    mae  = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2   = r2_score(y_true, y_pred)
    mape = np.mean(np.abs((y_true - y_pred) / np.clip(np.abs(y_true), 1e-8, None))) * 100
    print(f"\n  {label}")
    print(f"    MAE  : {mae:>14,.2f} TND")
    print(f"    RMSE : {rmse:>14,.2f} TND")
    print(f"    R²   : {r2:.4f}")
    print(f"    MAPE : {mape:.2f}%")
    return {"mae": mae, "rmse": rmse, "r2": r2, "mape": mape}


def train_payroll_forecast() -> dict:
    print("=" * 55)
    print("MODEL 1 — Monthly Payroll Forecasting")
    print("=" * 55)

    df = load_monthly_payroll()
    print(f"  Data loaded: {len(df)} months ({df['year_num'].min()}–{df['year_num'].max()})")

    target = "total_netpay"
    df = _add_lag_features(df, target, lags=6)

    feature_cols = [c for c in df.columns if
                    c.startswith("lag_") or
                    c.startswith("rolling_") or
                    c in ("month_sin", "month_cos", "year_norm",
                          "month_num", "year_num", "yoy_growth", "mom_delta")]

    X = df[feature_cols].values
    y = df[target].values

    # ── Proper evaluation: last 12 months as test ────────────────────────────
    # Why 12? Enough to see seasonality. Small dataset → can't afford more.
    TEST_SIZE = 12
    X_train, X_test = X[:-TEST_SIZE], X[-TEST_SIZE:]
    y_train, y_test = y[:-TEST_SIZE], y[-TEST_SIZE:]
    dates_test = df["month_start_date"].iloc[-TEST_SIZE:].values

    print(f"  Train: {len(X_train)} months | Test: {len(X_test)} months (last 12)")

    # ── Final model ───────────────────────────────────────────────────────────
    model = RandomForestRegressor(n_estimators=300, max_depth=10, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)

    train_metrics = _evaluate(y_train, model.predict(X_train), "Training set")
    test_metrics  = _evaluate(y_test,  model.predict(X_test),  "Test set (last 12 months)")

    # Feature importance
    importances = dict(zip(feature_cols, model.feature_importances_))
    top5 = sorted(importances.items(), key=lambda x: x[1], reverse=True)[:5]
    print(f"\n  Top 5 features: {top5}")

    # ── Forecast next 6 months ────────────────────────────────────────────────
    # Retrain on ALL data for forecasting
    final_model = RandomForestRegressor(n_estimators=300, max_depth=10, random_state=42, n_jobs=-1)
    final_model.fit(X, y)

    future_preds = []
    history = list(y)
    last_date = pd.Timestamp(df["month_start_date"].iloc[-1])

    for i in range(1, 7):
        future_date  = last_date + pd.DateOffset(months=i)
        future_month = future_date.month
        future_year  = future_date.year

        row = {}
        for lag in range(1, 7):
            row[f"lag_{lag}"] = history[-lag] if lag <= len(history) else 0
        row["rolling_mean_3"] = np.mean(history[-3:])
        row["rolling_mean_6"] = np.mean(history[-6:])
        row["rolling_std_3"]  = np.std(history[-3:])
        row["yoy_growth"]     = (history[-1] - history[-13]) / abs(history[-13]) if len(history) >= 13 else 0
        row["mom_delta"]      = history[-1] - history[-2] if len(history) >= 2 else 0
        row["month_sin"]      = np.sin(2 * np.pi * future_month / 12)
        row["month_cos"]      = np.cos(2 * np.pi * future_month / 12)
        row["year_norm"]      = (future_year - df["year_num"].min()) / (df["year_num"].max() - df["year_num"].min() + 1)
        row["month_num"]      = future_month
        row["year_num"]       = future_year

        feat = np.array([[row[c] for c in feature_cols]])
        pred = final_model.predict(feat)[0]
        future_preds.append({"date": future_date.strftime("%Y-%m"), "predicted_netpay": round(pred, 2)})
        history.append(pred)

    print("\n  6-Month Forecast:")
    for p in future_preds:
        print(f"    {p['date']}: {p['predicted_netpay']:>15,.2f} TND")

    # Save
    joblib.dump(final_model,  MODELS_DIR / "payroll_forecast.pkl")
    joblib.dump(feature_cols, MODELS_DIR / "payroll_forecast_features.pkl")

    # Save actual vs predicted for test set (for plotting)
    test_comparison = pd.DataFrame({
        "date":      dates_test,
        "actual":    y_test,
        "predicted": model.predict(X_test),
    })
    test_comparison.to_csv(MODELS_DIR / "payroll_forecast_test.csv", index=False)

    result = {
        "model":         "payroll_forecast",
        "train_months":  int(len(X_train)),
        "test_months":   int(len(X_test)),
        "train_metrics": {k: round(float(v), 4) for k, v in train_metrics.items()},
        "test_metrics":  {k: round(float(v), 4) for k, v in test_metrics.items()},
        "forecast_6m":   future_preds,
    }
    (MODELS_DIR / "payroll_forecast_results.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"\n  Model saved: {MODELS_DIR / 'payroll_forecast.pkl'}")
    return result


def train_indemnity_forecast() -> dict:
    print("\n" + "=" * 55)
    print("MODEL 2 — Monthly Indemnity Forecasting")
    print("=" * 55)

    df = load_monthly_indemnity()
    print(f"  Data loaded: {len(df)} months ({df['year_num'].min()}–{df['year_num'].max()})")

    target = "total_indemnity"
    df = _add_lag_features(df, target, lags=6)

    feature_cols = [c for c in df.columns if
                    c.startswith("lag_") or
                    c.startswith("rolling_") or
                    c in ("month_sin", "month_cos", "year_norm",
                          "month_num", "year_num", "yoy_growth", "mom_delta")]

    X = df[feature_cols].values
    y = df[target].values

    TEST_SIZE = 12
    X_train, X_test = X[:-TEST_SIZE], X[-TEST_SIZE:]
    y_train, y_test = y[:-TEST_SIZE], y[-TEST_SIZE:]
    dates_test = df["month_start_date"].iloc[-TEST_SIZE:].values

    print(f"  Train: {len(X_train)} months | Test: {len(X_test)} months (last 12)")

    model = RandomForestRegressor(n_estimators=300, max_depth=10, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)

    train_metrics = _evaluate(y_train, model.predict(X_train), "Training set")
    test_metrics  = _evaluate(y_test,  model.predict(X_test),  "Test set (last 12 months)")

    final_model = RandomForestRegressor(n_estimators=300, max_depth=10, random_state=42, n_jobs=-1)
    final_model.fit(X, y)

    test_comparison = pd.DataFrame({
        "date":      dates_test,
        "actual":    y_test,
        "predicted": model.predict(X_test),
    })
    test_comparison.to_csv(MODELS_DIR / "indemnity_forecast_test.csv", index=False)

    joblib.dump(final_model,  MODELS_DIR / "indemnity_forecast.pkl")
    joblib.dump(feature_cols, MODELS_DIR / "indemnity_forecast_features.pkl")

    result = {
        "model":         "indemnity_forecast",
        "train_months":  int(len(X_train)),
        "test_months":   int(len(X_test)),
        "train_metrics": {k: round(float(v), 4) for k, v in train_metrics.items()},
        "test_metrics":  {k: round(float(v), 4) for k, v in test_metrics.items()},
    }
    (MODELS_DIR / "indemnity_forecast_results.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"\n  Model saved: {MODELS_DIR / 'indemnity_forecast.pkl'}")
    return result


if __name__ == "__main__":
    r1 = train_payroll_forecast()
    r2 = train_indemnity_forecast()
    print("\n\nAll forecasting models trained successfully.")
