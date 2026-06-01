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

    # Rolling z-score: 12-month trailing window (shift=1 avoids data leakage)
    rolling_mean = grp.transform(lambda x: x.rolling(12, min_periods=3).mean().shift(1))
    rolling_std  = grp.transform(lambda x: x.rolling(12, min_periods=3).std().shift(1)).fillna(0)
    df["rolling_mean"] = rolling_mean
    df["rolling_std"]  = rolling_std
    df["rolling_z"] = np.where(
        df["rolling_std"] > 0,
        (df["m_netpay"] - df["rolling_mean"]) / df["rolling_std"],
        df["z_score"],
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
# Method 5: LSTM Autoencoder (Deep Learning)
# ═══════════════════════════════════════════════════════════════

LSTM_SEQ_LEN   = 12   # look at each employee's last 12 months
LSTM_LATENT    = 32   # bottleneck size

LSTM_TRAIN_SAMPLE = 80_000   # employees sampled for training (memory limit)

def _build_lstm_sequences(df: pd.DataFrame,
                           train_sample: int = LSTM_TRAIN_SAMPLE
                           ) -> tuple[np.ndarray, np.ndarray, np.ndarray, object]:
    """
    Build ONE sequence (the last LSTM_SEQ_LEN months) per employee.

    Training uses up to `train_sample` randomly sampled employees.
    Scoring uses all employees (one sequence each) — stays in RAM.

    Returns:
        train_seq : (T, SEQ_LEN, 1) — training sequences
        all_seq   : (N, SEQ_LEN, 1) — scoring sequences (all employees)
        row_idxs  : (N,) — df index of the LAST month in each employee's window
        seq_scaler: fitted MinMaxScaler
    """
    from sklearn.preprocessing import MinMaxScaler

    df_sorted = df.sort_values(["employee_sk", "year_num", "month_num"]).reset_index()
    seq_scaler = MinMaxScaler()
    df_sorted["netpay_scaled"] = seq_scaler.fit_transform(df_sorted[["m_netpay"]])

    all_seqs, row_idxs = [], []
    for _emp_sk, grp in df_sorted.groupby("employee_sk"):
        vals = grp["netpay_scaled"].values
        idxs = grp["index"].values
        if len(vals) < LSTM_SEQ_LEN:
            continue
        # Only last window per employee
        all_seqs.append(vals[-LSTM_SEQ_LEN:].reshape(LSTM_SEQ_LEN, 1))
        row_idxs.append(idxs[-1])

    all_seqs  = np.array(all_seqs,  dtype=np.float32)
    row_idxs  = np.array(row_idxs)

    # Sub-sample for training to stay within memory
    rng = np.random.RandomState(42)
    n_train = min(train_sample, len(all_seqs))
    train_idx = rng.choice(len(all_seqs), n_train, replace=False)
    train_seqs = all_seqs[train_idx]

    return train_seqs, all_seqs, row_idxs, seq_scaler


def _run_lstm_autoencoder(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, object]:
    """
    LSTM Autoencoder — learns normal salary trajectories per employee.

    Architecture (sequence-to-sequence):
        Encoder: LSTM(64) -> RepeatVector -> Decoder: LSTM(64) -> TimeDistributed Dense(1)

    Anomaly score = mean squared reconstruction error per sequence.
    High error = the salary trajectory deviates from learned normal patterns.

    Returns: (per_record_errors, lstm_flags, lstm_model)
    """
    import tensorflow as tf
    tf.get_logger().setLevel("ERROR")

    print(f"    Building employee salary sequences (window={LSTM_SEQ_LEN} months)...")
    train_seqs, all_seqs, row_idxs, seq_scaler = _build_lstm_sequences(df)

    if len(all_seqs) == 0:
        print("    [!] No sequences built — not enough employee history")
        return np.zeros(len(df)), np.zeros(len(df), dtype=bool), None

    print(f"    Sequences: {len(all_seqs):,} employees "
          f"({len(train_seqs):,} sampled for training)")

    # Build LSTM Autoencoder
    inputs  = tf.keras.Input(shape=(LSTM_SEQ_LEN, 1))
    encoded = tf.keras.layers.LSTM(64, activation="tanh",
                                   return_sequences=False)(inputs)
    repeated = tf.keras.layers.RepeatVector(LSTM_SEQ_LEN)(encoded)
    decoded  = tf.keras.layers.LSTM(64, activation="tanh",
                                    return_sequences=True)(repeated)
    outputs  = tf.keras.layers.TimeDistributed(
                   tf.keras.layers.Dense(1))(decoded)

    lstm_ae = tf.keras.Model(inputs, outputs)
    lstm_ae.compile(optimizer="adam", loss="mse")

    print(f"    Training LSTM Autoencoder ({AE_EPOCHS} epochs, "
          f"{len(train_seqs):,} sequences)...")
    lstm_ae.fit(
        train_seqs, train_seqs,
        epochs=AE_EPOCHS,
        batch_size=AE_BATCH_SIZE,
        validation_split=0.1,
        verbose=0,
    )

    # Reconstruction error — score ALL employees (one window each)
    reconstructed  = lstm_ae.predict(all_seqs, batch_size=AE_BATCH_SIZE, verbose=0)
    seq_errors     = np.mean(np.square(all_seqs - reconstructed), axis=(1, 2))

    # Map sequence errors back to individual records (last month of each window)
    record_errors = np.full(len(df), np.median(seq_errors))
    for seq_err, row_idx in zip(seq_errors, row_idxs):
        if row_idx < len(record_errors):
            record_errors[row_idx] = seq_err

    # Records not covered by any sequence keep median error
    uncovered = record_errors == 0
    if uncovered.any():
        record_errors[uncovered] = np.median(seq_errors)

    # Flag top AE_CONTAMINATION%
    threshold  = np.percentile(record_errors, (1 - AE_CONTAMINATION) * 100)
    lstm_flags = record_errors > threshold

    # Save scaler alongside model
    joblib.dump(seq_scaler, MODELS_DIR / "anomaly_lstm_scaler.pkl")

    return record_errors, lstm_flags, lstm_ae


# ═══════════════════════════════════════════════════════════════
# Temporal context
# ═══════════════════════════════════════════════════════════════

def _add_temporal_context(anomalies_df: pd.DataFrame, full_df: pd.DataFrame) -> pd.DataFrame:
    """Attach pay values for 2 months prior and 3 months after each anomalous record."""
    pay_lookup = full_df.set_index(["employee_sk", "year_num", "month_num"])["m_netpay"]

    for delta, col in [(-2, "pay_prev_2m"), (-1, "pay_prev_1m"),
                        (1,  "pay_next_1m"),  (2, "pay_next_2m"), (3, "pay_next_3m")]:
        total   = anomalies_df["year_num"].astype(int) * 12 + anomalies_df["month_num"].astype(int) - 1 + delta
        t_year  = total // 12
        t_month = total % 12 + 1
        idx = pd.MultiIndex.from_arrays([
            anomalies_df["employee_sk"].values,
            t_year.values,
            t_month.values,
        ])
        anomalies_df[col] = pay_lookup.reindex(idx).values

    anomalies_df["pct_change_vs_prev"] = np.where(
        (anomalies_df["pay_prev_1m"].notna()) & (anomalies_df["pay_prev_1m"] > 0),
        (anomalies_df["m_netpay"] - anomalies_df["pay_prev_1m"]) / anomalies_df["pay_prev_1m"] * 100,
        np.nan,
    )
    return anomalies_df


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
    # IsoForest uses only 256 samples/tree by default — training on 1M vs 42M
    # gives identical model quality but avoids per-worker 161 MB splitter arrays.
    _iso_n = min(1_000_000, len(X_scaled))
    _iso_idx = np.random.RandomState(42).choice(len(X_scaled), _iso_n, replace=False)
    iso = IsolationForest(n_estimators=200, contamination=IF_CONTAMINATION,
                          random_state=42, n_jobs=-1)
    iso.fit(X_scaled[_iso_idx])
    df["if_score"] = iso.score_samples(X_scaled)
    df["if_flag"]  = iso.predict(X_scaled) == -1
    if_metrics = _evaluate_method("Isolation Forest", df["if_flag"].values, df, zscore_flags)

    # ── Method 3: LOF (sample 500k — LOF is O(n²) and can't scale to 42M rows) ──
    print("\n  [3/4] Local Outlier Factor (sample 500k)...")
    lof_sample_idx = np.random.RandomState(42).choice(
        len(X_scaled), min(500_000, len(X_scaled)), replace=False)
    X_lof = X_scaled[lof_sample_idx]
    lof = LocalOutlierFactor(n_neighbors=20, contamination=LOF_CONTAMINATION, n_jobs=-1)
    lof_preds = lof.fit_predict(X_lof)
    df["lof_flag"]  = False
    df["lof_score"] = 0.0
    df.loc[df.index[lof_sample_idx], "lof_flag"]  = lof_preds == -1
    df.loc[df.index[lof_sample_idx], "lof_score"] = lof.negative_outlier_factor_
    lof_metrics = _evaluate_method("LOF", df["lof_flag"].values, df, zscore_flags)

    # ── Method 4: One-Class SVM (on sample — OCSVM is slow on 700k rows) ──────
    print("\n  [4/5] One-Class SVM (sample 50k)...")
    sample_idx = np.random.RandomState(42).choice(len(X_scaled),
                                                   min(50_000, len(X_scaled)),
                                                   replace=False)
    X_sample = X_scaled[sample_idx]
    ocsvm = OneClassSVM(kernel="rbf", nu=OCSVN_NU, gamma="scale")
    ocsvm.fit(X_sample)
    df["ocsvm_flag"] = False
    df.loc[df.index[sample_idx], "ocsvm_flag"] = ocsvm.predict(X_sample) == -1
    ocsvm_metrics = _evaluate_method("One-Class SVM", df["ocsvm_flag"].values, df, zscore_flags)

    # ── Method 5: LSTM Autoencoder (Deep Learning) ───────────────────────────
    print(f"\n  [5/5] LSTM Autoencoder (Deep Learning, {AE_EPOCHS} epochs)...")
    ae_errors, ae_flags, ae_model = _run_lstm_autoencoder(df)
    df["ae_error"] = ae_errors
    df["ae_flag"]  = ae_flags
    ae_metrics = _evaluate_method("LSTM Autoencoder", df["ae_flag"].values, df, zscore_flags)
    if ae_model is not None:
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
    method_labels = {"if": "Isolation Forest", "lof": "LOF",
                     "ocsvm": "OCSVM", "autoencoder": "LSTM"}
    winner_label = method_labels.get(winner_name, winner_name.upper())

    df["detection_method"] = ""
    df.loc[ df["zscore_flag"] & ~df[winner_flag_col], "detection_method"] = "Z-score"
    df.loc[~df["zscore_flag"] &  df[winner_flag_col], "detection_method"] = winner_label
    df.loc[ df["zscore_flag"] &  df[winner_flag_col], "detection_method"] = f"Z-score + {winner_label}"

    df["anomaly_flag"] = df["zscore_flag"] | df[winner_flag_col]
    n_combined = int(df["anomaly_flag"].sum())
    print(f"\n  Final flag (Z-score OR {winner_name}): "
          f"{n_combined:,} ({n_combined/len(df)*100:.2f}%)")

    # ── Top anomalies ─────────────────────────────────────────────────────────
    anomalies_df = (
        df[df["anomaly_flag"]]
        .sort_values("z_score", key=abs, ascending=False)
        [["employee_sk", "year_num", "month_num", "m_netpay",
          "emp_mean", "emp_median", "z_score", "rolling_z", "pct_deviation",
          "grade_code", "nature_code", "ministry_code",
          "zscore_flag", "if_flag", "lof_flag", "ocsvm_flag", "ae_flag",
          "detection_method"]]
    ).copy()

    print("\n  Adding temporal context (±2 months)...")
    anomalies_df = _add_temporal_context(anomalies_df, df)

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

    # Save per-employee baselines so incremental runs don't need to reload 42M rows
    emp_baselines = (
        df.groupby("employee_sk")
          .agg(emp_mean=("m_netpay", "mean"),
               emp_std=("m_netpay", lambda x: x.std() if len(x) > 1 else 0.0),
               emp_median=("m_netpay", "median"),
               emp_count=("m_netpay", "count"))
          .reset_index()
    )
    emp_baselines["emp_std"] = emp_baselines["emp_std"].fillna(0)
    joblib.dump(emp_baselines, MODELS_DIR / "anomaly_emp_baselines.pkl")

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


def _models_exist() -> bool:
    needed = ["anomaly_model.pkl", "anomaly_scaler.pkl", "anomaly_encoders.pkl",
              "anomaly_features.pkl", "anomaly_report.csv"]
    return all((MODELS_DIR / f).exists() for f in needed)


def _ensure_emp_baselines():
    """Compute and cache employee baselines if not yet saved (runs once, ~3 min)."""
    path = MODELS_DIR / "anomaly_emp_baselines.pkl"
    if path.exists():
        return joblib.load(path)

    print("  Computing employee baselines (one-time, ~3 min)...")
    from etl.core.config import DB_CONFIG
    import psycopg2
    conn = psycopg2.connect(**DB_CONFIG, options="-c work_mem=512MB")
    try:
        df = pd.read_sql("""
            SELECT employee_sk,
                   AVG(m_netpay)    AS emp_mean,
                   STDDEV(m_netpay) AS emp_std,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY m_netpay) AS emp_median,
                   COUNT(*)         AS emp_count
            FROM dw.fact_paie
            WHERE employee_sk <> 0 AND m_netpay IS NOT NULL
            GROUP BY employee_sk
        """, conn)
    finally:
        conn.close()

    df["emp_std"] = df["emp_std"].fillna(0)
    joblib.dump(df, path)
    print(f"  Baselines saved for {len(df):,} employees.")
    return df


def score_incremental() -> int:
    """
    Score only rows from periods not yet in anomaly_report.csv.
    Uses saved models — no retraining.  Returns number of new rows added (0 = up to date).
    """
    from etl.core.config import DB_CONFIG
    import psycopg2

    def _conn():
        opts = "-c statement_timeout=0 -c work_mem=256MB"
        return psycopg2.connect(**DB_CONFIG, options=opts)

    report_path = MODELS_DIR / "anomaly_report.csv"

    # Find which (year, month) pairs are already in the report
    existing = pd.read_csv(report_path, usecols=["year_num", "month_num"])
    covered  = set(zip(existing["year_num"].astype(int),
                        existing["month_num"].astype(int)))

    # Ask the DB for all distinct periods in fact_paie
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT dt.year_num, dt.month_num
                FROM dw.fact_paie fp
                JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
                WHERE fp.employee_sk <> 0 AND dt.year_num > 0
                ORDER BY dt.year_num, dt.month_num
            """)
            all_periods = {(r[0], r[1]) for r in cur.fetchall()}

    new_periods = all_periods - covered
    if not new_periods:
        print("  Anomaly report is already up to date. Nothing to score.")
        return 0

    print(f"  {len(new_periods)} new period(s) found: "
          f"{sorted(new_periods)[-min(3, len(new_periods)):]}")

    # Load new rows from DB (only the new periods)
    period_list = ",".join(f"({y},{m})" for y, m in sorted(new_periods))
    sql = f"""
        SELECT fp.employee_sk, dt.year_num, dt.month_num,
               dg.grade_code, dn.nature_code,
               fp.codetab AS ministry_code,
               fp.pa_eche, fp.pa_sitfam,
               fp.m_netpay, fp.m_salbrut, fp.m_salimp, fp.m_retrait,
               fp.m_cps, fp.m_cpe, fp.m_capdeces, fp.m_sub, fp.m_avkm, fp.m_avlog
        FROM dw.fact_paie fp
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        JOIN dw.dim_grade dg ON dg.grade_sk = fp.grade_sk
        JOIN dw.dim_nature dn ON dn.nature_sk = fp.nature_sk
        WHERE fp.employee_sk <> 0
          AND fp.grade_sk <> 0
          AND fp.nature_sk <> 0
          AND fp.codetab IS NOT NULL
          AND dt.year_num > 0
          AND fp.m_netpay IS NOT NULL
          AND (dt.year_num, dt.month_num) IN ({period_list})
    """
    with _conn() as conn:
        df_new = pd.read_sql(sql, conn)

    if df_new.empty:
        print("  No rows found for new periods.")
        return 0

    print(f"  Loaded {len(df_new):,} new rows.")

    # Apply saved employee baselines for z-score (computes once if missing)
    baselines = _ensure_emp_baselines()
    df_new = df_new.merge(baselines[["employee_sk", "emp_mean", "emp_std", "emp_median"]],
                          on="employee_sk", how="left")
    df_new["emp_mean"]   = df_new["emp_mean"].fillna(df_new["m_netpay"])
    df_new["emp_std"]    = df_new["emp_std"].fillna(0)
    df_new["emp_median"] = df_new["emp_median"].fillna(df_new["m_netpay"])

    df_new["z_score"] = np.where(
        df_new["emp_std"] > 0,
        (df_new["m_netpay"] - df_new["emp_mean"]) / df_new["emp_std"],
        0.0,
    )
    df_new["pct_deviation"] = np.where(
        df_new["emp_median"] > 0,
        np.abs(df_new["m_netpay"] - df_new["emp_median"]) / df_new["emp_median"] * 100,
        0.0,
    )
    df_new["rolling_z"]   = df_new["z_score"]   # no rolling window for new rows
    df_new["zscore_flag"] = df_new["z_score"].abs() > ZSCORE_THRESHOLD

    # Score with saved IF model
    feature_cols = joblib.load(MODELS_DIR / "anomaly_features.pkl")
    scaler       = joblib.load(MODELS_DIR / "anomaly_scaler.pkl")
    encoders     = joblib.load(MODELS_DIR / "anomaly_encoders.pkl")
    iso          = joblib.load(MODELS_DIR / "anomaly_model.pkl")

    for col, enc in encoders.items():
        if col in df_new.columns:
            known = set(enc.classes_)
            df_new[col] = df_new[col].astype(str).apply(
                lambda x: x if x in known else enc.classes_[0]
            )
            df_new[col] = enc.transform(df_new[col].astype(str))

    available = [c for c in feature_cols if c in df_new.columns]
    X = df_new[available].fillna(0).values.astype("float32")
    X_scaled = scaler.transform(X)

    df_new["if_flag"]    = iso.predict(X_scaled) == -1
    df_new["lof_flag"]   = False
    df_new["ocsvm_flag"] = False
    df_new["ae_flag"]    = False

    df_new["anomaly_flag"] = df_new["zscore_flag"] | df_new["if_flag"]
    df_new["detection_method"] = ""
    df_new.loc[ df_new["zscore_flag"] & ~df_new["if_flag"], "detection_method"] = "Z-score"
    df_new.loc[~df_new["zscore_flag"] &  df_new["if_flag"], "detection_method"] = "Isolation Forest"
    df_new.loc[ df_new["zscore_flag"] &  df_new["if_flag"], "detection_method"] = "Z-score + IF"

    new_anomalies = df_new[df_new["anomaly_flag"]].copy()

    # Add placeholder temporal context columns to match report schema
    for col in ["pay_prev_2m", "pay_prev_1m", "pay_next_1m", "pay_next_2m",
                "pay_next_3m", "pct_change_vs_prev"]:
        new_anomalies[col] = None

    # Append to existing report (keep only report columns)
    report_cols = pd.read_csv(report_path, nrows=0).columns.tolist()
    for col in report_cols:
        if col not in new_anomalies.columns:
            new_anomalies[col] = None

    new_anomalies[report_cols].to_csv(report_path, mode="a", header=False, index=False)
    print(f"  Appended {len(new_anomalies):,} new anomalies to report.")
    return len(new_anomalies)


if __name__ == "__main__":
    import sys
    force_full = "--full" in sys.argv

    if force_full or not _models_exist():
        if force_full:
            print("  --full flag set: running full retrain.")
        else:
            print("  No saved models found: running full retrain.")
        train_anomaly_model()
    else:
        print("=" * 60)
        print("MODEL 4 — Anomaly Detection (Incremental Scoring)")
        print("=" * 60)
        score_incremental()
