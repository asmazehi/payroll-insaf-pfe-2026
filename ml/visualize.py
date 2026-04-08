"""
ml/visualize.py
===============
Generates all plots for the ML models.
Saves to ml/plots/ as PNG files.

Run:
    python -m ml.visualize
"""
from __future__ import annotations

import warnings
warnings.filterwarnings("ignore")

from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # no display needed, just save files
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import shap

MODELS_DIR = Path(__file__).resolve().parent / "models"
PLOTS_DIR  = Path(__file__).resolve().parent / "plots"
PLOTS_DIR.mkdir(exist_ok=True)

# Style
sns.set_theme(style="whitegrid", palette="muted")
plt.rcParams.update({"figure.dpi": 150, "font.size": 11})

BLUE   = "#2563EB"
RED    = "#DC2626"
GREEN  = "#16A34A"
ORANGE = "#EA580C"
GRAY   = "#6B7280"


# ─────────────────────────────────────────────────────────────────────────────
# MODEL 1 — Payroll Forecast
# ─────────────────────────────────────────────────────────────────────────────

def plot_payroll_forecast():
    print("  Plotting Model 1 — Payroll Forecast...")

    from ml.data_loader import load_monthly_payroll
    df = load_monthly_payroll()
    df = df.sort_values("month_start_date").reset_index(drop=True)

    # Load test comparison
    test_df = pd.read_csv(MODELS_DIR / "payroll_forecast_test.csv", parse_dates=["date"])

    # Load future forecast from results
    import json
    results = json.loads((MODELS_DIR / "payroll_forecast_results.json").read_text(encoding="utf-8"))
    forecast = pd.DataFrame(results["forecast_6m"])
    forecast["date"] = pd.to_datetime(forecast["date"])

    fig, axes = plt.subplots(2, 1, figsize=(14, 10))

    # ── Plot 1: Full history + test predictions + forecast ────────────────────
    ax = axes[0]
    ax.plot(df["month_start_date"], df["total_netpay"] / 1e6,
            color=BLUE, linewidth=1.5, label="Actual payroll", alpha=0.8)
    ax.plot(test_df["date"], test_df["predicted"] / 1e6,
            color=RED, linewidth=2, linestyle="--", label="Model predictions (test set)")
    ax.scatter(test_df["date"], test_df["actual"] / 1e6,
               color=ORANGE, s=40, zorder=5, label="Actual (test set)")
    ax.plot(forecast["date"], forecast["predicted_netpay"] / 1e6,
            color=GREEN, linewidth=2, linestyle=":", marker="o", markersize=5,
            label="6-month forecast")

    ax.set_title("Model 1 — Total Monthly Payroll: Actual vs Predicted", fontsize=13, fontweight="bold")
    ax.set_ylabel("Net Pay (Millions TND)")
    ax.set_xlabel("")
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.xaxis.set_major_locator(mdates.YearLocator())

    # ── Plot 2: Test period zoomed in ─────────────────────────────────────────
    ax2 = axes[1]
    ax2.plot(test_df["date"], test_df["actual"] / 1e6,
             color=BLUE, linewidth=2, marker="o", markersize=5, label="Actual")
    ax2.plot(test_df["date"], test_df["predicted"] / 1e6,
             color=RED, linewidth=2, linestyle="--", marker="s", markersize=5,
             label="Predicted")

    # Error band
    error = (test_df["actual"] - test_df["predicted"]).abs() / 1e6
    ax2.fill_between(test_df["date"],
                     test_df["predicted"] / 1e6 - error,
                     test_df["predicted"] / 1e6 + error,
                     alpha=0.15, color=RED, label="Absolute error band")

    ax2.set_title("Test Set — Last 12 Months (Zoomed)", fontsize=13, fontweight="bold")
    ax2.set_ylabel("Net Pay (Millions TND)")
    ax2.set_xlabel("Month")
    ax2.legend()
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax2.xaxis.set_major_locator(mdates.MonthLocator())
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)

    plt.tight_layout()
    path = PLOTS_DIR / "01_payroll_forecast.png"
    plt.savefig(path, bbox_inches="tight")
    plt.close()
    print(f"    Saved: {path}")


# ─────────────────────────────────────────────────────────────────────────────
# MODEL 2 — Indemnity Forecast
# ─────────────────────────────────────────────────────────────────────────────

def plot_indemnity_forecast():
    print("  Plotting Model 2 — Indemnity Forecast...")

    from ml.data_loader import load_monthly_indemnity
    df = load_monthly_indemnity()
    df = df.sort_values("month_start_date").reset_index(drop=True)

    test_df = pd.read_csv(MODELS_DIR / "indemnity_forecast_test.csv", parse_dates=["date"])

    fig, axes = plt.subplots(2, 1, figsize=(14, 9))

    ax = axes[0]
    ax.plot(df["month_start_date"], df["total_indemnity"] / 1e6,
            color=BLUE, linewidth=1.5, label="Actual indemnity")
    ax.plot(test_df["date"], test_df["predicted"] / 1e6,
            color=RED, linewidth=2, linestyle="--", label="Predicted (test)")
    ax.scatter(test_df["date"], test_df["actual"] / 1e6,
               color=ORANGE, s=40, zorder=5, label="Actual (test)")
    ax.set_title("Model 2 — Monthly Indemnity: Actual vs Predicted", fontsize=13, fontweight="bold")
    ax.set_ylabel("Indemnity (Millions TND)")
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.xaxis.set_major_locator(mdates.YearLocator())

    ax2 = axes[1]
    ax2.plot(test_df["date"], test_df["actual"] / 1e6,
             color=BLUE, linewidth=2, marker="o", markersize=5, label="Actual")
    ax2.plot(test_df["date"], test_df["predicted"] / 1e6,
             color=RED, linewidth=2, linestyle="--", marker="s", markersize=5,
             label="Predicted")
    ax2.set_title("Test Set — Last 12 Months (Zoomed)", fontsize=13, fontweight="bold")
    ax2.set_ylabel("Indemnity (Millions TND)")
    ax2.legend()
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax2.xaxis.set_major_locator(mdates.MonthLocator())
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45)

    plt.tight_layout()
    path = PLOTS_DIR / "02_indemnity_forecast.png"
    plt.savefig(path, bbox_inches="tight")
    plt.close()
    print(f"    Saved: {path}")


# ─────────────────────────────────────────────────────────────────────────────
# MODEL 3 — Salary Prediction
# ─────────────────────────────────────────────────────────────────────────────

def plot_salary_model():
    print("  Plotting Model 3 — Salary Prediction...")

    from ml.data_loader import load_individual_payroll
    from sklearn.preprocessing import LabelEncoder
    import json

    df = load_individual_payroll()

    # Re-encode (same as training)
    cat_cols = ["grade_code", "nature_code", "ministry_code", "pa_sitfam"]
    for col in cat_cols:
        df[col] = df[col].fillna("UNKNOWN").astype(str).str.strip()
        le = LabelEncoder()
        df[col + "_enc"] = le.fit_transform(df[col])

    df["pa_eche"]      = pd.to_numeric(df["pa_eche"],   errors="coerce").fillna(0)
    df["year_num"]     = pd.to_numeric(df["year_num"],  errors="coerce").fillna(0)
    df["month_num"]    = pd.to_numeric(df["month_num"], errors="coerce").fillna(0)
    df["month_sin"]    = np.sin(2 * np.pi * df["month_num"] / 12)
    df["month_cos"]    = np.cos(2 * np.pi * df["month_num"] / 12)
    df["grade_x_eche"] = df["grade_code_enc"] * df["pa_eche"]
    df = df.dropna(subset=["m_netpay"]).copy()

    emp_stats = joblib.load(MODELS_DIR / "salary_emp_stats.pkl")
    df = df.merge(emp_stats, on="employee_sk", how="left")
    df["emp_mean"]   = df["emp_mean"].fillna(df["m_netpay"].mean())
    df["emp_median"] = df["emp_median"].fillna(df["m_netpay"].mean())
    df["emp_std"]    = df["emp_std"].fillna(0)

    feature_cols = joblib.load(MODELS_DIR / "salary_features.pkl")
    model = joblib.load(MODELS_DIR / "salary_model.pkl")

    # Sample for speed
    sample = df.sample(min(50_000, len(df)), random_state=42)
    X_sample = sample[feature_cols].values.astype(float)
    y_sample = sample["m_netpay"].values.astype(float)
    preds = model.predict(X_sample)

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))

    # ── Plot 1: Actual vs Predicted scatter ───────────────────────────────────
    ax = axes[0]
    ax.scatter(y_sample / 1000, preds / 1000, alpha=0.08, s=5, color=BLUE)
    max_val = max(y_sample.max(), preds.max()) / 1000
    ax.plot([0, max_val], [0, max_val], color=RED, linewidth=1.5,
            linestyle="--", label="Perfect prediction")
    ax.set_xlabel("Actual Net Pay (KTND)")
    ax.set_ylabel("Predicted Net Pay (KTND)")
    ax.set_title("Actual vs Predicted Salary", fontweight="bold")
    ax.legend()

    # ── Plot 2: Feature importance ────────────────────────────────────────────
    ax2 = axes[1]
    results = json.loads((MODELS_DIR / "salary_results.json").read_text(encoding="utf-8"))
    fi = results["feature_importance"]
    fi_df = pd.DataFrame(list(fi.items()), columns=["feature", "importance"]).sort_values("importance")
    # Clean feature names for display
    fi_df["feature"] = fi_df["feature"].str.replace("_enc", "").str.replace("_", " ")
    colors = [GREEN if i >= len(fi_df) - 3 else BLUE for i in range(len(fi_df))]
    ax2.barh(fi_df["feature"], fi_df["importance"], color=colors)
    ax2.set_title("Feature Importance", fontweight="bold")
    ax2.set_xlabel("Importance Score")

    # ── Plot 3: Prediction error distribution ─────────────────────────────────
    ax3 = axes[2]
    errors = preds - y_sample
    ax3.hist(errors[np.abs(errors) < 5000], bins=80, color=BLUE, alpha=0.7, edgecolor="white")
    ax3.axvline(0, color=RED, linewidth=2, linestyle="--", label="Zero error")
    ax3.axvline(errors.mean(), color=ORANGE, linewidth=2, label=f"Mean error: {errors.mean():,.0f} TND")
    ax3.set_xlabel("Prediction Error (TND)")
    ax3.set_ylabel("Count")
    ax3.set_title("Prediction Error Distribution", fontweight="bold")
    ax3.legend()

    plt.suptitle("Model 3 — Individual Salary Prediction (XGBoost)", fontsize=14, fontweight="bold", y=1.01)
    plt.tight_layout()
    path = PLOTS_DIR / "03_salary_prediction.png"
    plt.savefig(path, bbox_inches="tight")
    plt.close()
    print(f"    Saved: {path}")


# ─────────────────────────────────────────────────────────────────────────────
# MODEL 3 — SHAP Explainability
# ─────────────────────────────────────────────────────────────────────────────

def plot_shap():
    print("  Plotting SHAP explainability (Model 3)...")

    from ml.data_loader import load_individual_payroll
    from sklearn.preprocessing import LabelEncoder

    df = load_individual_payroll()

    cat_cols = ["grade_code", "nature_code", "ministry_code", "pa_sitfam"]
    for col in cat_cols:
        df[col] = df[col].fillna("UNKNOWN").astype(str).str.strip()
        le = LabelEncoder()
        df[col + "_enc"] = le.fit_transform(df[col])

    df["pa_eche"]      = pd.to_numeric(df["pa_eche"],   errors="coerce").fillna(0)
    df["year_num"]     = pd.to_numeric(df["year_num"],  errors="coerce").fillna(0)
    df["month_num"]    = pd.to_numeric(df["month_num"], errors="coerce").fillna(0)
    df["month_sin"]    = np.sin(2 * np.pi * df["month_num"] / 12)
    df["month_cos"]    = np.cos(2 * np.pi * df["month_num"] / 12)
    df["grade_x_eche"] = df["grade_code_enc"] * df["pa_eche"]
    df = df.dropna(subset=["m_netpay"]).copy()

    emp_stats = joblib.load(MODELS_DIR / "salary_emp_stats.pkl")
    df = df.merge(emp_stats, on="employee_sk", how="left")
    df["emp_mean"]   = df["emp_mean"].fillna(df["m_netpay"].mean())
    df["emp_median"] = df["emp_median"].fillna(df["m_netpay"].mean())
    df["emp_std"]    = df["emp_std"].fillna(0)

    feature_cols = joblib.load(MODELS_DIR / "salary_features.pkl")
    model = joblib.load(MODELS_DIR / "salary_model.pkl")

    # SHAP on a small sample (1000 rows is enough for a summary plot)
    sample = df.sample(1000, random_state=42)
    X_sample = pd.DataFrame(sample[feature_cols].values.astype(float), columns=feature_cols)

    # Clean column names for display
    display_names = {c: c.replace("_enc", "").replace("_", " ") for c in feature_cols}
    X_display = X_sample.rename(columns=display_names)

    explainer   = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_sample)

    fig, axes = plt.subplots(1, 2, figsize=(16, 7))

    # ── SHAP summary bar (mean |SHAP| per feature) ────────────────────────────
    plt.sca(axes[0])
    shap.summary_plot(
        shap_values, X_display,
        plot_type="bar",
        show=False,
        color=BLUE,
    )
    axes[0].set_title("SHAP — Mean Feature Impact\n(how much each feature changes the prediction)",
                      fontweight="bold")

    # ── SHAP beeswarm (direction + magnitude) ─────────────────────────────────
    plt.sca(axes[1])
    shap.summary_plot(
        shap_values, X_display,
        plot_type="dot",
        show=False,
    )
    axes[1].set_title("SHAP — Feature Impact Direction\n(red = high value pushes salary up, blue = pushes down)",
                      fontweight="bold")

    plt.suptitle("Model 3 — SHAP Explainability: Why did the model predict this salary?",
                 fontsize=13, fontweight="bold", y=1.01)
    plt.tight_layout()
    path = PLOTS_DIR / "04_shap_salary.png"
    plt.savefig(path, bbox_inches="tight")
    plt.close()
    print(f"    Saved: {path}")


# ─────────────────────────────────────────────────────────────────────────────
# MODEL 4 — Anomaly Detection
# ─────────────────────────────────────────────────────────────────────────────

def plot_anomaly():
    print("  Plotting Model 4 — Anomaly Detection...")

    from ml.data_loader import load_individual_payroll
    from sklearn.preprocessing import LabelEncoder

    df = load_individual_payroll()

    # Employee baseline
    grp = df.groupby("employee_sk")["m_netpay"]
    df["emp_mean"]   = grp.transform("mean")
    df["emp_std"]    = grp.transform("std").fillna(0)
    df["emp_median"] = grp.transform("median")
    df["emp_count"]  = grp.transform("count")
    df["z_score"]    = np.where(
        df["emp_std"] > 0,
        (df["m_netpay"] - df["emp_mean"]) / df["emp_std"], 0.0
    )
    df["pct_deviation"] = np.where(
        df["emp_median"] > 0,
        np.abs(df["m_netpay"] - df["emp_median"]) / df["emp_median"] * 100, 0.0
    )
    df["zscore_flag"] = df["z_score"].abs() > 3.0

    # Load IF scores
    anomaly_model   = joblib.load(MODELS_DIR / "anomaly_model.pkl")
    anomaly_scaler  = joblib.load(MODELS_DIR / "anomaly_scaler.pkl")
    anomaly_enc     = joblib.load(MODELS_DIR / "anomaly_encoders.pkl")
    feature_cols    = joblib.load(MODELS_DIR / "anomaly_features.pkl")

    for col in ["grade_code", "nature_code", "ministry_code"]:
        le = anomaly_enc[col]
        df[col] = df[col].fillna("UNKNOWN").astype(str).str.strip()
        df[col + "_enc"] = df[col].apply(
            lambda v, le=le: int(le.transform([v])[0]) if v in le.classes_ else -1
        )
    for col in ["pa_eche", "year_num", "month_num"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Sample for speed
    sample = df.sample(min(100_000, len(df)), random_state=42).copy()
    X = sample[feature_cols].values.astype(float)
    X_scaled = anomaly_scaler.transform(X)
    sample["if_score"] = anomaly_model.score_samples(X_scaled)
    sample["if_flag"]  = anomaly_model.predict(X_scaled) == -1
    sample["anomaly_flag"] = sample["zscore_flag"] | sample["if_flag"]

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))

    # ── Plot 1: Z-score distribution ──────────────────────────────────────────
    ax = axes[0, 0]
    z_clipped = sample["z_score"].clip(-8, 8)
    ax.hist(z_clipped[~sample["zscore_flag"]], bins=80, color=BLUE, alpha=0.7,
            label="Normal", density=True)
    ax.hist(z_clipped[sample["zscore_flag"]], bins=40, color=RED, alpha=0.7,
            label="Anomaly (|z| > 3)", density=True)
    ax.axvline(3,  color=RED, linestyle="--", linewidth=1.5)
    ax.axvline(-3, color=RED, linestyle="--", linewidth=1.5)
    ax.set_xlabel("Z-Score (personal deviation)")
    ax.set_ylabel("Density")
    ax.set_title("Z-Score Distribution per Employee", fontweight="bold")
    ax.legend()

    # ── Plot 2: Isolation Forest score distribution ───────────────────────────
    ax2 = axes[0, 1]
    ax2.hist(sample[~sample["if_flag"]]["if_score"], bins=80, color=BLUE,
             alpha=0.7, label="Normal", density=True)
    ax2.hist(sample[sample["if_flag"]]["if_score"], bins=40, color=RED,
             alpha=0.7, label="Anomaly", density=True)
    ax2.set_xlabel("Isolation Forest Score (more negative = more anomalous)")
    ax2.set_ylabel("Density")
    ax2.set_title("Isolation Forest Score Distribution", fontweight="bold")
    ax2.legend()

    # ── Plot 3: Z-score vs IF score scatter ───────────────────────────────────
    ax3 = axes[1, 0]
    normal = sample[~sample["anomaly_flag"]].sample(min(5000, len(sample[~sample["anomaly_flag"]])), random_state=42)
    flagged = sample[sample["anomaly_flag"]]
    ax3.scatter(normal["z_score"].clip(-8, 8), normal["if_score"],
                alpha=0.2, s=4, color=BLUE, label="Normal")
    ax3.scatter(flagged["z_score"].clip(-8, 8), flagged["if_score"],
                alpha=0.6, s=8, color=RED, label=f"Anomaly ({len(flagged):,})")
    ax3.axvline(3,  color=GRAY, linestyle="--", linewidth=1, alpha=0.7)
    ax3.axvline(-3, color=GRAY, linestyle="--", linewidth=1, alpha=0.7)
    ax3.set_xlabel("Z-Score")
    ax3.set_ylabel("Isolation Forest Score")
    ax3.set_title("Anomaly Map: Z-Score vs Isolation Forest", fontweight="bold")
    ax3.legend()

    # ── Plot 4: Anomalies by ministry ─────────────────────────────────────────
    ax4 = axes[1, 1]
    ministry_counts = (
        sample[sample["anomaly_flag"]]
        .groupby("ministry_code").size()
        .sort_values(ascending=False)
        .head(10)
    )
    colors_bar = [RED if i == 0 else BLUE for i in range(len(ministry_counts))]
    ax4.barh(ministry_counts.index[::-1], ministry_counts.values[::-1], color=colors_bar[::-1])
    ax4.set_xlabel("Number of Anomalies")
    ax4.set_title("Top 10 Ministries by Anomaly Count", fontweight="bold")
    for i, (v, lbl) in enumerate(zip(ministry_counts.values[::-1], ministry_counts.index[::-1])):
        ax4.text(v + 5, i, f"{v:,}", va="center", fontsize=9)

    plt.suptitle("Model 4 — Payroll Anomaly Detection", fontsize=14, fontweight="bold")
    plt.tight_layout()
    path = PLOTS_DIR / "05_anomaly_detection.png"
    plt.savefig(path, bbox_inches="tight")
    plt.close()
    print(f"    Saved: {path}")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  Generating all ML visualizations...")
    print("=" * 55 + "\n")

    plot_payroll_forecast()
    plot_indemnity_forecast()
    plot_salary_model()
    plot_shap()
    plot_anomaly()

    print(f"\n  All plots saved to: {PLOTS_DIR}")
    print("  Files:")
    for f in sorted(PLOTS_DIR.glob("*.png")):
        size_kb = f.stat().st_size // 1024
        print(f"    {f.name} ({size_kb} KB)")


if __name__ == "__main__":
    main()
