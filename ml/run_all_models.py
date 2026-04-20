"""
ml/run_all_models.py
====================
Trains Model 1 and Model 4. Saves all results to ml/models/.

Run:
    python -m ml.run_all_models
"""
from ml.model_forecast import train_payroll_forecast
from ml.model_anomaly  import train_anomaly_model
import json
from pathlib import Path

MODELS_DIR = Path(__file__).resolve().parent / "models"


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("INSAF ML Pipeline — Model 1 + Model 4")
    print("=" * 60 + "\n")

    r1 = train_payroll_forecast()
    r4 = train_anomaly_model()

    summary = {
        "model_1_forecast": {
            "winner":      r1["winner"],
            "test_mape":   r1["model_comparison"][r1["winner"]]["mape"],
            "forecast_6m": r1["forecast_6m"],
        },
        "model_4_anomaly": {
            "winner":        r4["winner"],
            "anomaly_rate":  r4["final_flag"]["anomaly_rate"],
            "n_anomalies":   r4["final_flag"]["n_anomalies"],
        },
    }

    (MODELS_DIR / "all_results.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    print("\n\nAll models trained. Summary saved to ml/models/all_results.json")
