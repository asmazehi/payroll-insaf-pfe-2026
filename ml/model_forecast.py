"""
ml/model_forecast.py
====================
MODEL 1 - Monthly Payroll Forecasting (Model Comparison)

Compares 6 models on the same test set (last 12 months):
  1. Ridge Regression    (simple baseline)
  2. Random Forest       (ensemble, lag features)
  3. XGBoost             (gradient boosting, lag features)
  4. SARIMA              (classical time series)
  5. Prophet             (Meta, handles seasonality + trend automatically)
  6. TFT                 (Temporal Fusion Transformer - deep learning)

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
# TFT (Temporal Fusion Transformer)
# ============================================================

def _run_tft(df_full: pd.DataFrame, test_len: int) -> tuple[dict, object | None]:
    """
    Train TFT via pytorch-forecasting on the monthly payroll series.
    Returns (metrics_dict, tft_predictor_or_None).
    Falls back gracefully if pytorch / pytorch-forecasting not installed.
    """
    try:
        import torch
        import pytorch_lightning as pl
        from pytorch_forecasting import TemporalFusionTransformer, TimeSeriesDataSet
        from pytorch_forecasting.metrics import MAE as PF_MAE
        from pytorch_forecasting.data import GroupNormalizer
    except ImportError as e:
        print(f"    tft           SKIPPED (missing deps: {e})")
        return {"mae": 999, "rmse": 999, "r2": -999, "mape": 999}, None

    try:
        # ---- prepare dataset -------------------------------------------------
        df_tft = df_full[["month_start_date", TARGET, "month_num", "year_num"]].copy()
        df_tft = df_tft.sort_values("month_start_date").reset_index(drop=True)
        df_tft["time_idx"]  = df_tft.index.astype(int)
        df_tft["group"]     = "total"                      # single series
        df_tft["month_str"] = df_tft["month_num"].astype(str)
        df_tft["year_str"]  = df_tft["year_num"].astype(str)

        max_encoder = 24          # look-back window
        max_predict = test_len    # predict the test horizon

        training_cutoff = df_tft["time_idx"].max() - test_len

        tft_train = TimeSeriesDataSet(
            df_tft[df_tft["time_idx"] <= training_cutoff],
            time_idx           = "time_idx",
            target             = TARGET,
            group_ids          = ["group"],
            min_encoder_length = max_encoder // 2,
            max_encoder_length = max_encoder,
            min_prediction_length = 1,
            max_prediction_length = max_predict,
            static_categoricals   = ["group"],
            time_varying_known_categoricals  = ["month_str", "year_str"],
            time_varying_known_reals         = ["time_idx"],
            time_varying_unknown_reals       = [TARGET],
            target_normalizer  = GroupNormalizer(groups=["group"], transformation="softplus"),
            add_relative_time_idx  = True,
            add_target_scales      = True,
            add_encoder_length     = True,
        )

        tft_val = TimeSeriesDataSet.from_dataset(
            tft_train, df_tft, predict=True, stop_randomization=True
        )

        train_dl = tft_train.to_dataloader(train=True,  batch_size=32, num_workers=0)
        val_dl   = tft_val.to_dataloader(train=False, batch_size=32, num_workers=0)

        # ---- model -----------------------------------------------------------
        tft = TemporalFusionTransformer.from_dataset(
            tft_train,
            learning_rate          = 0.03,
            hidden_size            = 32,
            attention_head_size    = 2,
            dropout                = 0.1,
            hidden_continuous_size = 16,
            output_size            = 7,           # quantiles
            loss                   = PF_MAE(),
            log_interval           = -1,
            reduce_on_plateau_patience = 4,
        )

        accelerator = "gpu" if torch.cuda.is_available() else "cpu"
        trainer = pl.Trainer(
            max_epochs           = 30,
            accelerator          = accelerator,
            devices              = 1,
            gradient_clip_val    = 0.1,
            enable_progress_bar  = False,
            enable_model_summary = False,
            logger               = False,
        )

        trainer.fit(tft, train_dataloaders=train_dl, val_dataloaders=val_dl)

        # ---- evaluate --------------------------------------------------------
        raw_preds = tft.predict(val_dl, mode="prediction", return_x=False)
        y_pred    = raw_preds.numpy().flatten()[:test_len]
        y_true    = df_tft[df_tft["time_idx"] > training_cutoff][TARGET].values[:test_len]

        m = _metrics(y_true, y_pred, "tft")
        return m, (tft, tft_train, df_tft)

    except Exception as e:
        print(f"    tft           FAILED: {e}")
        return {"mae": 999, "rmse": 999, "r2": -999, "mape": 999}, None


def _forecast_tft_6m(tft_bundle, df_full: pd.DataFrame) -> list[dict]:
    """Forecast next 6 months using a trained TFT."""
    try:
        import torch
        from pytorch_forecasting import TimeSeriesDataSet

        tft, tft_train, df_tft = tft_bundle

        # extend the dataframe with 6 placeholder future rows
        last_idx   = df_tft["time_idx"].max()
        last_date  = pd.Timestamp(df_full["month_start_date"].iloc[-1])
        last_val   = df_tft[TARGET].iloc[-1]

        future_rows = []
        for i in range(1, 7):
            fd = last_date + pd.DateOffset(months=i)
            future_rows.append({
                "time_idx":     last_idx + i,
                "group":        "total",
                TARGET:         last_val,           # placeholder (required by dataset)
                "month_num":    fd.month,
                "year_num":     fd.year,
                "month_str":    str(fd.month),
                "year_str":     str(fd.year),
                "month_start_date": fd,
            })

        df_ext = pd.concat([df_tft, pd.DataFrame(future_rows)], ignore_index=True)

        pred_ds = TimeSeriesDataSet.from_dataset(
            tft_train, df_ext, predict=True, stop_randomization=True
        )
        pred_dl  = pred_ds.to_dataloader(train=False, batch_size=32, num_workers=0)
        raw      = tft.predict(pred_dl, mode="prediction", return_x=False)
        y_pred   = raw.numpy().flatten()[-6:]

        preds = []
        for i, val in enumerate(y_pred):
            fd = last_date + pd.DateOffset(months=i + 1)
            preds.append({
                "date":             fd.strftime("%Y-%m"),
                "predicted_netpay": round(float(val), 2),
            })
        return preds
    except Exception as e:
        print(f"  TFT 6m forecast failed: {e}")
        return []


# ============================================================
# 6-month forecast using winner
# ============================================================

def _forecast_6m(winner_name, winner_model, df, feature_cols) -> list[dict]:
    """Forecast next 6 months using the winning model."""
    if winner_name == "tft":
        return _forecast_tft_6m(winner_model, df)

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
    print("MODEL 1 - Payroll Forecasting (6-Model Comparison)")
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

    # TFT (deep learning - skipped gracefully if deps missing)
    print("\n  Training TFT (Temporal Fusion Transformer)...")
    tft_metrics, tft_bundle = _run_tft(df_feat, TEST_MONTHS)

    # Compare
    all_metrics = {
        "ridge":   ml_metrics["ridge"],
        "rf":      ml_metrics["rf"],
        "xgb":     ml_metrics["xgb"],
        "sarima":  sarima_metrics,
        "prophet": prophet_metrics,
        "tft":     tft_metrics,
    }

    # Exclude ridge if overfit; exclude any failed model (mape=999)
    if all_metrics["ridge"]["mape"] < 0.5:
        print("  [!] Ridge MAPE near 0 --- likely overfit on small dataset, excluding from winner selection")
        candidates = {k: v for k, v in all_metrics.items() if k != "ridge" and v["mape"] < 999}
    else:
        candidates = {k: v for k, v in all_metrics.items() if v["mape"] < 999}

    if not candidates:
        candidates = {"rf": ml_metrics["rf"]}

    print(f"\n  --- Model Comparison (by Test MAPE) ---")
    winner_name = min(candidates, key=lambda k: candidates[k]["mape"])
    for name, m in sorted(all_metrics.items(), key=lambda x: x[1]["mape"]):
        marker = " <-- WINNER" if name == winner_name else ""
        skip   = " [excluded/failed]" if m["mape"] >= 999 else ""
        print(f"    {name:<12s}  MAPE={m['mape']:.2f}%  R2={m['r2']:.4f}{marker}{skip}")

    winner_mape = candidates[winner_name]["mape"]
    print(f"\n  Winner: {winner_name.upper()} (MAPE={winner_mape:.2f}%)")

    # Save winner
    if winner_name == "tft":
        winner_model = tft_bundle
    elif winner_name in trained_ml:
        winner_model = trained_ml[winner_name]
    elif winner_name == "prophet":
        winner_model = prophet_model
    else:
        winner_model = trained_ml["rf"]  # fallback

    joblib.dump(winner_model, MODELS_DIR / "payroll_forecast.pkl")
    joblib.dump(fc,           MODELS_DIR / "payroll_forecast_features.pkl")
    joblib.dump(winner_name,  MODELS_DIR / "payroll_forecast_winner.pkl")

    # Save test comparison for plotting
    if winner_name in trained_ml:
        test_preds = trained_ml[winner_name].predict(X_test)
    elif winner_name == "prophet":
        test_preds = [prophet_metrics] * len(y_test)  # placeholder
    else:
        test_preds = trained_ml["rf"].predict(X_test)

    pd.DataFrame({
        "date":      df_test["month_start_date"].values,
        "actual":    y_test,
        "predicted": test_preds,
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
