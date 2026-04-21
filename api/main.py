"""
api/main.py
===========
INSAF Payroll Intelligence Platform — FastAPI backend.

Endpoints:
    GET  /                        Health check
    POST /upload                  Upload payroll JSON file -> ETL -> DW
    GET  /forecast?n=6            Next N months payroll forecast
    GET  /anomalies?limit=50      Top anomalies from DW
    POST /anomalies/explain       LLM explanation for a single anomaly
    GET  /summary                 DW summary stats
    POST /chat                    RAG chatbot (Ollama + PostgreSQL DW)
    GET  /plots/{filename}        Serve ML plot images

Run:
    uvicorn api.main:app --reload --port 8000
"""
from __future__ import annotations

import json
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

# ── App setup ─────────────────────────────────────────────────────────────────
app = FastAPI(
    title="INSAF Payroll Intelligence Platform",
    description="API for payroll ETL, ML forecasting, salary prediction, and anomaly detection.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten in production
    allow_methods=["*"],
    allow_headers=["*"],
)

PLOTS_DIR  = Path(__file__).resolve().parent.parent / "ml" / "plots"
MODELS_DIR = Path(__file__).resolve().parent.parent / "ml" / "models"
DATA_DIR   = Path(__file__).resolve().parent.parent / "data" / "raw"


# ── Schemas ───────────────────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    question: str
    model:    str = "llama3.2"


class AnomalyExplainRequest(BaseModel):
    employee_sk:   int
    grade_code:    str            = ""
    nature_code:   str            = ""
    ministry_code: str            = ""
    month_num:     int            = 1
    year_num:      int            = 2026
    m_netpay:      float          = 0.0
    emp_mean:      float          = 0.0
    emp_std:       float          = 0.0
    emp_median:    float          = 0.0
    z_score:       float          = 0.0
    pct_deviation: float          = 0.0
    zscore_flag:   bool           = False
    if_flag:       bool           = False
    if_score:      float          = 0.0
    model:         str            = "llama3.2"


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
def health():
    """Check if the API is running."""
    return {"status": "ok", "platform": "INSAF Payroll Intelligence Platform"}


@app.get("/summary", tags=["Data"])
def get_summary():
    """
    Returns high-level stats from the DW:
    total records, date range, employee count, total payroll.
    """
    try:
        import psycopg2
        from etl.core.config import DB_CONFIG
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*)                      AS total_records,
                        COUNT(DISTINCT fp.employee_sk) AS total_employees,
                        SUM(fp.m_netpay)              AS total_netpay,
                        MIN(dt.year_num)              AS year_min,
                        MAX(dt.year_num)              AS year_max
                    FROM dw.fact_paie fp
                    JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
                    WHERE fp.employee_sk <> 0 AND dt.year_num > 0
                """)
                row = cur.fetchone()
        return {
            "total_payroll_records": row[0],
            "total_employees":       row[1],
            "total_netpay_tnd":      round(float(row[2] or 0), 2),
            "year_range":            f"{row[3]}–{row[4]}",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/upload", tags=["ETL"])
async def upload_payroll_file(
    file: UploadFile = File(...),
    file_type: str   = Query("paie", enum=["paie", "indem"],
                             description="Type of payroll file: 'paie' or 'indem'"),
    retrain: bool    = Query(False, description="Retrain ML models after loading"),
):
    """
    Upload a raw payroll JSON file.
    Triggers the ETL pipeline → loads into PostgreSQL DW.
    Optionally retrains ML models after load.

    - file_type=paie  → processes as payroll (paie2015.json format)
    - file_type=indem → processes as indemnity (ind2015.json format)
    """
    if not file.filename.endswith(".json"):
        raise HTTPException(status_code=400, detail="Only .json files are supported.")

    run_id = uuid.uuid4().hex[:8]

    # Save uploaded file to data/raw/
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    dest = DATA_DIR / file.filename
    with dest.open("wb") as f:
        shutil.copyfileobj(file.file, f)

    try:
        if file_type == "paie":
            from etl.pipeline_paie import run as run_paie
            report = run_paie(run_id=run_id)
        else:
            from etl.pipeline_indem import run as run_indem
            report = run_indem(run_id=run_id)

        from etl.load_dw import run as load_dw
        load_dw(reset=False)

        ml_status = "not_requested"
        if retrain:
            try:
                from ml.run_all_models import main as run_ml
                run_ml()
                ml_status = "retrained"
            except Exception as ml_err:
                ml_status = f"failed: {ml_err}"

        return {
            "status":    "success",
            "run_id":    run_id,
            "file":      file.filename,
            "file_type": file_type,
            "etl_stats": report.get("stats", {}),
            "ml_status": ml_status,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"ETL failed: {e}")


@app.get("/forecast", tags=["ML"])
def get_forecast(
    n: int = Query(6, ge=1, le=24, description="Number of months to forecast"),
):
    """
    Forecast total monthly payroll for the next N months.
    Uses the trained Random Forest model with lag features.
    """
    try:
        from ml.predict import predict_payroll_next_months
        predictions = predict_payroll_next_months(n_months=n)
        return {
            "model":       "payroll_forecast",
            "n_months":    n,
            "forecast":    predictions,
            "currency":    "TND",
        }
    except FileNotFoundError:
        raise HTTPException(
            status_code=503,
            detail="Forecast model not found. Run 'python -m ml.run_all_models' first."
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/anomalies", tags=["ML"])
def get_anomalies(
    limit: int = Query(50, ge=1, le=500),
    ministry:  Optional[str] = Query(None, description="Filter by ministry code"),
    year:      Optional[int] = Query(None, description="Filter by year"),
):
    """
    Returns top anomalies detected in payroll data.
    Reads from the pre-computed anomaly_report.csv saved during training.
    """
    report_path = MODELS_DIR / "anomaly_report.csv"
    if not report_path.exists():
        raise HTTPException(
            status_code=503,
            detail="Anomaly report not found. Run 'python -m ml.run_all_models' first."
        )

    import pandas as pd
    df = pd.read_csv(report_path)

    if ministry:
        df = df[df["ministry_code"] == ministry]
    if year:
        df = df[df["year_num"] == year]

    df = df.sort_values("z_score", key=abs, ascending=False).head(limit)

    return {
        "total_anomalies_in_report": int(len(pd.read_csv(report_path))),
        "returned":  int(len(df)),
        "filters":   {"ministry": ministry, "year": year},
        "anomalies": df.to_dict(orient="records"),
    }


@app.post("/anomalies/explain", tags=["ML"])
def explain_anomaly_record(req: AnomalyExplainRequest):
    """
    Generate a plain-language LLM explanation for a single anomalous payroll record.
    Uses Ollama (llama3.2) running locally — must be started before calling.
    """
    try:
        import pandas as pd
        from ml.llm_explainer import explain_anomaly
        row  = pd.Series(req.model_dump())
        expl = explain_anomaly(row, model=req.model)
        return {
            "employee_sk": req.employee_sk,
            "explanation": expl,
            "model":       req.model,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/chat", tags=["RAG Chatbot"])
def chat_endpoint(req: ChatRequest):
    """
    RAG Chatbot — ask any question about the INSAF payroll data.

    The system retrieves relevant data from PostgreSQL DW, then passes it
    to Ollama (llama3.2) to generate a grounded, factual answer.

    Examples:
    - "What is the total payroll budget for 2025?"
    - "Which ministry has the most employees?"
    - "How many anomalies were detected?"
    - "What is the average salary per grade?"

    Requires: Ollama running locally with llama3.2 pulled.
    """
    try:
        from api.chatbot import chat
        result = chat(question=req.question, model=req.model)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/plots", tags=["Visualizations"])
def list_plots():
    """List all available ML plot images."""
    plots = []
    for f in sorted(PLOTS_DIR.glob("*.png")):
        plots.append({
            "filename": f.name,
            "url":      f"/plots/{f.name}",
            "size_kb":  f.stat().st_size // 1024,
        })
    return {"plots": plots}


@app.get("/plots/{filename}", tags=["Visualizations"])
def get_plot(filename: str):
    """Serve a specific ML plot image."""
    path = PLOTS_DIR / filename
    if not path.exists() or not filename.endswith(".png"):
        raise HTTPException(status_code=404, detail="Plot not found.")
    return FileResponse(path, media_type="image/png")
