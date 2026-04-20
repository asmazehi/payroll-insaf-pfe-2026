"""
ml/model_anomaly.py
===================
MODEL 4 — Payroll Anomaly Detection (Method Comparison)

Compares 4 unsupervised methods on the same data:
  1. Z-score          (statistical baseline per employee)
  2. Isolation Forest (ensemble, captures multivariate outliers)
  3. LOF              (Local Outlier Factor — density-based)
  4. One-Class SVM    (boundary-based)

Comparison metrics (unsupervised — no ground truth):
  - Anomaly rate % (should be 2–8% for payroll data)
  - Agreement rate between methods
  - Average z-score of flagged records (quality proxy)
  - Top anomaly severity (max z-score)

Winner = best balance of anomaly rate + agreement + severity.
Final combined flag = Z-score OR winner method.

Run:
    python -m ml.model_anomaly
"""
from __future__ import annotations

import json
import warnings
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.svm import OneClassSVM

from ml.data_loader import load_individual_payroll

warnings.filterwarnings("ignore")

MODELS_DIR = Path(__file__).resolve().parent / "models"
MODELS_DIR.mkdir(exist_ok=True)

ZSCORE_THRESHOLD  = 3.0
IF_CONTAMINATION  = 0.03
LOF_CONTAMINATION = 0.03
OCSVN_NU          = 0.03
AE_CONTAMINATION  = 0.03   # top 3% reconstruction errors flagged as anomalies
AE_EPOCHS         = 30
AE_BATCH_SIZE     = 1024


# ═══════════════════════════════════════════════════════════════
# Employee baseline (z-score)
# ═══════════════════════════════════════════════════════════════

def _compute_employee_baseline(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["employee_sk", "year_num", "month_num"]).copy()
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
    df["zscore_flag"] = df["z_score"].abs() > ZSCORE_THRESHOLD
    return df


# ═══════════════════════════════════════════════════════════════
# Feature matrix builder
# ═══════════════════════════════════════════════════════════════

def _build_features(df: pd.DataFrame):
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
        ["m_netpay", "m_salbrut", "emp_mean", "emp_std",
         "emp_count", "z_score", "pct_deviation"]
    )

    X = df[feature_cols].values.astype(float)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    return X_scaled, feature_cols, encoders, scaler


# ═══════════════════════════════════════════════════════════════
# Method comparison
# ═══════════════════════════════════════════════════════════════

def _evaluate_method(name: str, flags: np.ndarray, df: pd.DataFrame,
                     zscore_flags: np.ndarray) -> dict:
    n_total    = len(flags)
    n_flagged  = int(flags.sum())
    rate       = round(n_flagged / n_total * 100, 3)
    agreement  = round(float(np.mean(flags == zscore_flags)) * 100, 2)
    avg_zscore = round(float(df.loc[flags, "z_score"].abs().mean()), 3) \
                 if n_flagged > 0 else 0.0
    max_zscore = round(float(df.loc[flags, "z_score"].abs().max()), 3) \
                 if n_flagged > 0 else 0.0

    print(f"    {name:<20s}  flagged={n_flagged:>7,} ({rate:.2f}%)  "
          f"z_score_avg={avg_zscore:.2f}  z_score_max={max_zscore:.2f}  "
          f"agreement_with_zscore={agreement:.1f}%")
    return {
        "n_flagged":   n_flagged,
        "rate_pct":    rate,
        "agreement_pct": agreement,
        "avg_zscore":  avg_zscore,
        "max_zscore":  max_zscore,
    }


def _score_method(metrics: dict) -> float:
    """
    Score a method: higher is better.
    Rewards: high avg z-score of flagged records (quality),
             reasonable anomaly rate (2–8%),
             high agreement with z-score baseline.
    """
    rate     = metrics["rate_pct"]
    rate_ok  = 1.0 if 1.5 <= rate <= 8.0 else max(0, 1 - abs(rate - 4.5) / 4.5)
    quality  = min(metrics["avg_zscore"] / 5.0, 1.0)   # normalize to 0–1
    agreement = metrics["agreement_pct"] / 100.0
    return round(rate_ok * 0.3 + quality * 0.5 + agreement * 0.2, 4)


# ═══════════════════════════════════════════════════════════════
# Method 5: Autoencoder (Deep Learning)
# ═══════════════════════════════════════════════════════════════

def _run_autoencoder(X_scaled: np.ndarray) -> tuple[np.ndarray, np.ndarray, object]:
    """
    Train an Autoencoder on the full dataset.
    Logic: train on ALL data — the network learns to reconstruct normal patterns.
    Records with high reconstruction error are anomalies.

    Architecture:
        Input(n_features) -> Dense(64, relu) -> Dense(32, relu) -> Dense(16, relu)
        -> Dense(32, relu) -> Dense(64, relu) -> Output(n_features)

    Returns: (reconstruction_errors, ae_flags, autoencoder_model)
    """
    import tensorflow as tf
    tf.get_logger().setLevel("ERROR")

    n_features = X_scaled.shape[1]

    inputs   = tf.keras.Input(shape=(n_features,))
    encoded  = tf.keras.layers.Dense(64, activation="relu")(inputs)
    encoded  = tf.keras.layers.Dense(32, activation="relu")(encoded)
    bottleneck = tf.keras.layers.Dense(16, activation="relu")(encoded)
    decoded  = tf.keras.layers.Dense(32, activation="relu")(bottleneck)
    decoded  = tf.keras.layers.Dense(64, activation="relu")(decoded)
    outputs  = tf.keras.layers.Dense(n_features, activation="linear")(decoded)

    ae = tf.keras.Model(inputs, outputs)
    ae.compile(optimizer="adam", loss="mse")

    ae.fit(
        X_scaled, X_scaled,
        epochs=AE_EPOCHS,
        batch_size=AE_BATCH_SIZE,
        validation_split=0.1,
        verbose=0,
    )

    reconstructed = ae.predict(X_scaled, batch_size=AE_BATCH_SIZE, verbose=0)
    errors        = np.mean(np.square(X_scaled - reconstructed), axis=1)

    # Flag top AE_CONTAMINATION% as anomalies
    threshold     = np.percentile(errors, (1 - AE_CONTAMINATION) * 100)
    ae_flags      = errors > threshold

    return errors, ae_flags, ae


# ═══════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════

def train_anomaly_model() -> dict:
    print("=" * 60)
    print("MODEL 4 — Anomaly Detection (Method Comparison)")
    print("=" * 60)

    df = load_individual_payroll()
    print(f"  Data: {len(df):,} records | {df['employee_sk'].nunique():,} employees")

    # Step 1: baseline
    df = _compute_employee_baseline(df)
    n_zscore = int(df["zscore_flag"].sum())
    print(f"\n  [1/4] Z-score baseline (|z| > {ZSCORE_THRESHOLD}): "
          f"{n_zscore:,} ({n_zscore/len(df)*100:.2f}%)")

    # Step 2: feature matrix
    print("\n  Building feature matrix...")
    X_scaled, feature_cols, encoders, scaler = _build_features(df)

    zscore_flags = df["zscore_flag"].values

    # ── Method 2: Isolation Forest ────────────────────────────────────────────
    print("\n  [2/4] Isolation Forest...")
    iso = IsolationForest(n_estimators=200, contamination=IF_CONTAMINATION,
                          random_state=42, n_jobs=-1)
    iso.fit(X_scaled)
    df["if_score"] = iso.score_samples(X_scaled)
    df["if_flag"]  = iso.predict(X_scaled) == -1
    if_metrics = _evaluate_method("Isolation Forest", df["if_flag"].values, df, zscore_flags)

    # ── Method 3: LOF ─────────────────────────────────────────────────────────
    print("\n  [3/4] Local Outlier Factor...")
    lof = LocalOutlierFactor(n_neighbors=20, contamination=LOF_CONTAMINATION, n_jobs=-1)
    lof_preds = lof.fit_predict(X_scaled)
    df["lof_flag"]  = lof_preds == -1
    df["lof_score"] = lof.negative_outlier_factor_
    lof_metrics = _evaluate_method("LOF", df["lof_flag"].values, df, zscore_flags)

    # ── Method 4: One-Class SVM (on sample — OCSVM is slow on 700k rows) ──────
    print("\n  [4/5] One-Class SVM (sample 50k)...")
    sample_idx = np.random.RandomState(42).choice(len(X_scaled),
                                                   min(50_000, len(X_scaled)),
                                                   replace=False)
    X_sample = X_scaled[sample_idx]
    ocsvm = OneClassSVM(kernel="rbf", nu=OCSVN_NU, gamma="scale")
    ocsvm.fit(X_sample)
    df["ocsvm_flag"] = ocsvm.predict(X_scaled) == -1
    ocsvm_metrics = _evaluate_method("One-Class SVM", df["ocsvm_flag"].values, df, zscore_flags)

    # ── Method 5: Autoencoder (Deep Learning) ────────────────────────────────
    print(f"\n  [5/5] Autoencoder (Deep Learning, {AE_EPOCHS} epochs)...")
    ae_errors, ae_flags, ae_model = _run_autoencoder(X_scaled)
    df["ae_error"] = ae_errors
    df["ae_flag"]  = ae_flags
    ae_metrics = _evaluate_method("Autoencoder (DL)", df["ae_flag"].values, df, zscore_flags)
    joblib.dump(ae_model, MODELS_DIR / "anomaly_autoencoder.pkl")

    # ── Z-score metrics (for comparison table) ────────────────────────────────
    zscore_metrics = _evaluate_method("Z-score", zscore_flags, df, zscore_flags)

    # ── Compare & pick winner ─────────────────────────────────────────────────
    candidates = {
        "if":       if_metrics,
        "lof":      lof_metrics,
        "ocsvm":    ocsvm_metrics,
        "autoencoder": ae_metrics,
    }
    scores = {name: _score_method(m) for name, m in candidates.items()}

    print("\n  --- Method Scores (higher = better) ---")
    for name, score in sorted(scores.items(), key=lambda x: -x[1]):
        marker = " <-- WINNER" if score == max(scores.values()) else ""
        print(f"    {name:<20s}  score = {score:.4f}{marker}")

    winner_name = max(scores, key=scores.get)
    winner_flag_col = {"if": "if_flag", "lof": "lof_flag",
                       "ocsvm": "ocsvm_flag",
                       "autoencoder": "ae_flag"}[winner_name]

    # ── Final combined flag: Z-score OR winner ────────────────────────────────
    df["anomaly_flag"] = df["zscore_flag"] | df[winner_flag_col]
    n_combined = int(df["anomaly_flag"].sum())
    print(f"\n  Final flag (Z-score OR {winner_name}): "
          f"{n_combined:,} ({n_combined/len(df)*100:.2f}%)")

    # ── Top anomalies ─────────────────────────────────────────────────────────
    anomalies_df = (
        df[df["anomaly_flag"]]
        .sort_values("z_score", key=abs, ascending=False)
        [["employee_sk", "year_num", "month_num", "m_netpay",
          "emp_mean", "emp_median", "z_score", "pct_deviation",
          "grade_code", "nature_code", "ministry_code",
          "zscore_flag", "if_flag", "lof_flag", "ocsvm_flag", "ae_flag"]]
    )

    print("\n  Top 10 anomalies:")
    print(f"  {'emp_sk':>8s} {'year':>5s} {'month':>5s} {'netpay':>12s} "
          f"{'mean':>12s} {'z':>7s} {'%dev':>8s} {'ministry':>12s}")
    for _, row in anomalies_df.head(10).iterrows():
        print(f"  {int(row['employee_sk']):>8d} {int(row['year_num']):>5d} "
              f"{int(row['month_num']):>5d} {row['m_netpay']:>12,.2f} "
              f"{row['emp_mean']:>12,.2f} {row['z_score']:>7.2f} "
              f"{row['pct_deviation']:>7.1f}% {str(row['ministry_code']):>12s}")

    print("\n  Anomalies by ministry (top 10):")
    ministry_counts = (
        df[df["anomaly_flag"]]
        .groupby("ministry_code").size()
        .sort_values(ascending=False).head(10)
    )
    for ministry, count in ministry_counts.items():
        print(f"    {ministry:<20s} {count:>6,} ({count/n_combined*100:.1f}%)")

    # ── Save ──────────────────────────────────────────────────────────────────
    joblib.dump(iso,          MODELS_DIR / "anomaly_model.pkl")
    joblib.dump(scaler,       MODELS_DIR / "anomaly_scaler.pkl")
    joblib.dump(encoders,     MODELS_DIR / "anomaly_encoders.pkl")
    joblib.dump(feature_cols, MODELS_DIR / "anomaly_features.pkl")

    # Full anomaly report
    anomalies_df.to_csv(MODELS_DIR / "anomaly_report.csv", index=False)

    result = {
        "model":              "anomaly_detection_comparison",
        "winner":             winner_name,
        "total_records":      int(len(df)),
        "total_employees":    int(df["employee_sk"].nunique()),
        "zscore_threshold":   ZSCORE_THRESHOLD,
        "contamination":      IF_CONTAMINATION,
        "method_comparison": {
            "zscore":       {**zscore_metrics, "score": _score_method(zscore_metrics)},
            "if":           {**if_metrics,     "score": scores["if"]},
            "lof":          {**lof_metrics,    "score": scores["lof"]},
            "ocsvm":        {**ocsvm_metrics,  "score": scores["ocsvm"]},
            "autoencoder":  {**ae_metrics,     "score": scores["autoencoder"]},
        },
        "final_flag": {
            "method":       f"zscore OR {winner_name}",
            "n_anomalies":  n_combined,
            "anomaly_rate": round(n_combined / len(df) * 100, 4),
        },
        "top_anomalies": anomalies_df.head(20)[
            ["employee_sk", "year_num", "month_num",
             "m_netpay", "emp_mean", "z_score", "pct_deviation"]
        ].to_dict(orient="records"),
    }

    (MODELS_DIR / "anomaly_results.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    print(f"\n  Model saved: anomaly_model.pkl")
    print(f"  Report saved: anomaly_report.csv ({len(anomalies_df):,} rows)")
    return result


if __name__ == "__main__":
    train_anomaly_model()
