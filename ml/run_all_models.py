"""
ml/run_all_models.py
====================
Runs all 4 ML models in sequence and saves a combined results summary.

Run:
    python -m ml.run_all_models
"""
from __future__ import annotations

import json
import time
from pathlib import Path

MODELS_DIR = Path(__file__).resolve().parent / "models"
MODELS_DIR.mkdir(exist_ok=True)


def main():
    print("\n" + "=" * 55)
    print("  INSAF PAYROLL — ML PIPELINE")
    print("=" * 55 + "\n")

    results = {}
    t0 = time.time()

    from ml.model_forecast import train_payroll_forecast, train_indemnity_forecast
    results["payroll_forecast"]    = train_payroll_forecast()
    results["indemnity_forecast"]  = train_indemnity_forecast()

    from ml.model_salary import train_salary_model
    results["salary_prediction"]   = train_salary_model()

    from ml.model_anomaly import train_anomaly_model
    results["anomaly_detection"]   = train_anomaly_model()

    elapsed = time.time() - t0
    print(f"\n{'=' * 55}")
    print(f"  ALL MODELS COMPLETE — {elapsed:.1f}s")
    print(f"{'=' * 55}")
    print("\n  Summary:")
    print(f"    Model 1 — Payroll Forecast     CV R²: {results['payroll_forecast']['cv_r2_mean']:.4f}")
    print(f"    Model 2 — Indemnity Forecast   CV R²: {results['indemnity_forecast']['cv_r2_mean']:.4f}")
    print(f"    Model 3 — Salary Prediction    Test R²: {results['salary_prediction']['test_metrics']['r2']:.4f}")
    print(f"    Model 4 — Anomaly Detection    Flagged: {results['anomaly_detection']['combined_anomalies']:,} "
          f"({results['anomaly_detection']['anomaly_rate_pct']:.2f}%)")

    (MODELS_DIR / "all_results.json").write_text(
        json.dumps(results, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )
    print(f"\n  Results saved: {MODELS_DIR / 'all_results.json'}")


if __name__ == "__main__":
    main()
