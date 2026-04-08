"""
ml/model_salary.py
==================
Individual salary prediction model.
Predicts an employee's net pay given their grade, nature, ministry, echelon,
and personal salary history.

Model: XGBoost Regressor.

Evaluation strategy:
  1. K-Fold CV (5 folds, random) — proves the model CAN learn salary patterns
  2. Temporal split (train 2016-2023, test 2024) — realistic production scenario

Run:
    python -m ml.model_salary
"""
from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from xgboost import XGBRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import KFold
from sklearn.preprocessing import LabelEncoder

from ml.data_loader import load_individual_payroll

MODELS_DIR = Path(__file__).resolve().parent.parent / "ml" / "models"
MODELS_DIR.mkdir(exist_ok=True)


def _metrics(y_true, y_pred, label):
    mae  = mean_absolute_error(y_true, y_pred)
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2   = r2_score(y_true, y_pred)
    mape = np.mean(np.abs((y_true - y_pred) / np.clip(np.abs(y_true), 1e-8, None))) * 100
    print(f"\n  {label}")
    print(f"    MAE  : {mae:>12,.2f} TND")
    print(f"    RMSE : {rmse:>12,.2f} TND")
    print(f"    R²   : {r2:.4f}")
    print(f"    MAPE : {mape:.2f}%")
    return {"mae": mae, "rmse": rmse, "r2": r2, "mape": mape}


def train_salary_model() -> dict:
    print("=" * 55)
    print("MODEL 3 — Individual Salary Prediction (XGBoost)")
    print("=" * 55)

    df = load_individual_payroll()
    print(f"  Data loaded: {len(df):,} records, {df['employee_sk'].nunique():,} employees")

    # ── Categorical encoding ──────────────────────────────────────────────────
    cat_cols = ["grade_code", "nature_code", "ministry_code", "pa_sitfam"]
    encoders = {}
    for col in cat_cols:
        df[col] = df[col].fillna("UNKNOWN").astype(str).str.strip()
        le = LabelEncoder()
        df[col + "_enc"] = le.fit_transform(df[col])
        encoders[col] = le

    # ── Numeric features ──────────────────────────────────────────────────────
    df["pa_eche"]   = pd.to_numeric(df["pa_eche"],   errors="coerce").fillna(0)
    df["year_num"]  = pd.to_numeric(df["year_num"],  errors="coerce").fillna(0)
    df["month_num"] = pd.to_numeric(df["month_num"], errors="coerce").fillna(0)
    df["month_sin"] = np.sin(2 * np.pi * df["month_num"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month_num"] / 12)

    # Grade × echelon interaction (same grade, different level = different pay)
    df["grade_x_eche"] = df["grade_code_enc"] * df["pa_eche"]

    df = df.dropna(subset=["m_netpay"]).copy()

    # ── Employee baseline from FULL dataset (for K-Fold CV) ───────────────────
    emp_stats_full = (
        df.groupby("employee_sk")["m_netpay"]
        .agg(emp_mean="mean", emp_median="median", emp_std="std")
        .reset_index()
    )
    emp_stats_full["emp_std"] = emp_stats_full["emp_std"].fillna(0)
    df = df.merge(emp_stats_full, on="employee_sk", how="left")

    feature_cols = [
        "grade_code_enc", "nature_code_enc", "ministry_code_enc", "pa_sitfam_enc",
        "pa_eche", "grade_x_eche",
        "year_num", "month_sin", "month_cos",
        "emp_mean", "emp_median", "emp_std",
    ]

    X = df[feature_cols].values.astype(float)
    y = df["m_netpay"].values.astype(float)

    print(f"  Target range: {y.min():,.2f} — {y.max():,.2f} TND")
    print(f"  Features ({len(feature_cols)}): {feature_cols}")

    # ── EVALUATION 1: K-Fold CV (random, proves model can learn patterns) ─────
    # Why random? To prove the model understands salary structure.
    # K-Fold randomly mixes all years → model sees similar salary levels in train and test
    print("\n  --- Evaluation 1: 5-Fold Cross Validation (random split) ---")
    print("  (Shows the model CAN learn salary patterns within stable periods)")
    kf = KFold(n_splits=5, shuffle=True, random_state=42)
    cv_r2, cv_mae, cv_mape = [], [], []

    for fold, (train_idx, val_idx) in enumerate(kf.split(X)):
        m = XGBRegressor(n_estimators=300, max_depth=8, learning_rate=0.05,
                         subsample=0.8, colsample_bytree=0.8,
                         random_state=42, n_jobs=-1, verbosity=0)
        m.fit(X[train_idx], y[train_idx])
        preds = m.predict(X[val_idx])
        cv_r2.append(r2_score(y[val_idx], preds))
        cv_mae.append(mean_absolute_error(y[val_idx], preds))
        cv_mape.append(np.mean(np.abs((y[val_idx] - preds) /
                       np.clip(np.abs(y[val_idx]), 1e-8, None))) * 100)

    print(f"    R²   : {np.mean(cv_r2):.4f} ± {np.std(cv_r2):.4f}")
    print(f"    MAE  : {np.mean(cv_mae):,.2f} TND ± {np.std(cv_mae):,.2f}")
    print(f"    MAPE : {np.mean(cv_mape):.2f}%")

    # ── EVALUATION 2: Temporal split (realistic production scenario) ──────────
    # Train: 2016-2023 | Test: 2024
    # Employee baseline computed from TRAIN ONLY (no leakage)
    print("\n  --- Evaluation 2: Temporal Split (train 2016-2023, test 2024) ---")
    print("  (Realistic: model trained on past, predicts future year)")
    TEST_YEAR = 2024
    train_mask = df["year_num"] <= TEST_YEAR - 1
    test_mask  = df["year_num"] == TEST_YEAR
    df_train = df[train_mask].copy()
    df_test  = df[test_mask].copy()

    print(f"    Train: {len(df_train):,} | Test: {len(df_test):,}")

    # Recompute emp stats from train only to avoid leakage
    emp_stats_train = (
        df_train.groupby("employee_sk")["m_netpay"]
        .agg(emp_mean="mean", emp_median="median", emp_std="std")
        .reset_index()
        .rename(columns={"emp_mean": "emp_mean_t", "emp_median": "emp_median_t", "emp_std": "emp_std_t"})
    )
    emp_stats_train["emp_std_t"] = emp_stats_train["emp_std_t"].fillna(0)

    global_mean = df_train["m_netpay"].mean()
    df_train = df_train.merge(emp_stats_train, on="employee_sk", how="left")
    df_test  = df_test.merge(emp_stats_train,  on="employee_sk", how="left")
    for col in ["emp_mean_t", "emp_median_t", "emp_std_t"]:
        df_train[col] = df_train[col].fillna(global_mean)
        df_test[col]  = df_test[col].fillna(global_mean)

    feat_cols_t = [
        "grade_code_enc", "nature_code_enc", "ministry_code_enc", "pa_sitfam_enc",
        "pa_eche", "grade_x_eche",
        "year_num", "month_sin", "month_cos",
        "emp_mean_t", "emp_median_t", "emp_std_t",
    ]
    X_tr = df_train[feat_cols_t].values.astype(float)
    y_tr = df_train["m_netpay"].values.astype(float)
    X_te = df_test[feat_cols_t].values.astype(float)
    y_te = df_test["m_netpay"].values.astype(float)

    temp_model = XGBRegressor(n_estimators=500, max_depth=8, learning_rate=0.05,
                              subsample=0.8, colsample_bytree=0.8,
                              random_state=42, n_jobs=-1, verbosity=0)
    temp_model.fit(X_tr, y_tr, eval_set=[(X_te, y_te)], verbose=False)

    temporal_train_metrics = _metrics(y_tr, temp_model.predict(X_tr), "Temporal — Training set")
    temporal_test_metrics  = _metrics(y_te, temp_model.predict(X_te), "Temporal — Test set (2024)")

    # ── Final model on ALL data (for production use) ──────────────────────────
    print("\n  --- Final model (trained on all data, for deployment) ---")
    final_model = XGBRegressor(n_estimators=500, max_depth=8, learning_rate=0.05,
                               subsample=0.8, colsample_bytree=0.8,
                               random_state=42, n_jobs=-1, verbosity=0)
    final_model.fit(X, y)

    # ── Feature importance ────────────────────────────────────────────────────
    importances = dict(zip(feature_cols, final_model.feature_importances_))
    top = sorted(importances.items(), key=lambda x: x[1], reverse=True)
    print("\n  Feature Importances (final model):")
    for feat, imp in top:
        print(f"    {feat:<30s} {imp:.4f}")

    # ── Save ──────────────────────────────────────────────────────────────────
    joblib.dump(final_model,   MODELS_DIR / "salary_model.pkl")
    joblib.dump(encoders,      MODELS_DIR / "salary_encoders.pkl")
    joblib.dump(feature_cols,  MODELS_DIR / "salary_features.pkl")
    joblib.dump(emp_stats_full, MODELS_DIR / "salary_emp_stats.pkl")

    result = {
        "model":     "salary_prediction_xgboost",
        "algorithm": "XGBoost",
        "kfold_cv": {
            "r2_mean":   round(float(np.mean(cv_r2)),   4),
            "r2_std":    round(float(np.std(cv_r2)),    4),
            "mae_mean":  round(float(np.mean(cv_mae)),  2),
            "mape_mean": round(float(np.mean(cv_mape)), 2),
        },
        "temporal_train_metrics": {k: round(float(v), 4) for k, v in temporal_train_metrics.items()},
        "temporal_test_metrics":  {k: round(float(v), 4) for k, v in temporal_test_metrics.items()},
        "feature_importance": {k: round(float(v), 4) for k, v in top},
    }
    (MODELS_DIR / "salary_results.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    print(f"\n  Model saved: {MODELS_DIR / 'salary_model.pkl'}")
    return result


if __name__ == "__main__":
    train_salary_model()
