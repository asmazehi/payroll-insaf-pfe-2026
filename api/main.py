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

import asyncio
import json
import os
import shutil
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Optional

from fastapi import BackgroundTasks, FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
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

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PLOTS_DIR    = PROJECT_ROOT / "ml" / "plots"
MODELS_DIR   = PROJECT_ROOT / "ml" / "models"

# Staging dirs live OUTSIDE the repo — raw kept 7 days, clean deleted after load
_STAGING_BASE = Path(os.getenv("LOCALAPPDATA", tempfile.gettempdir())) / "insaf" / "staging"
STAGING_RAW   = _STAGING_BASE / "raw"
STAGING_CLEAN = _STAGING_BASE / "clean"
STAGING_RAW.mkdir(parents=True, exist_ok=True)
STAGING_CLEAN.mkdir(parents=True, exist_ok=True)

RAW_BACKUP_DAYS = 7

# ── Progress tracking (in-memory, per run) ────────────────────────────────────
_progress: dict[str, dict] = {}
_pipeline_executor = ThreadPoolExecutor(max_workers=2)


def _prog(run_id: str, stage: str, pct: int, msg: str, **extra) -> None:
    _progress[run_id] = {
        "stage": stage, "pct": pct, "msg": msg,
        "ts": datetime.utcnow().isoformat(), **extra
    }


def _cleanup_old_raw(days: int = RAW_BACKUP_DAYS) -> None:
    cutoff = datetime.utcnow() - timedelta(days=days)
    for f in STAGING_RAW.iterdir():
        if f.is_file() and datetime.utcfromtimestamp(f.stat().st_mtime) < cutoff:
            f.unlink(missing_ok=True)


def _detect_file_type(path: Path) -> str | None:
    """Read first record and return 'paie' if pa_type==1, 'indem' if pa_type==3, else None."""
    try:
        from etl.ingestion.readers import stream_records
        for rec in stream_records(path):
            pa_type = str(rec.get("pa_type") or rec.get("PA_TYPE") or "").strip()
            if pa_type == "1":
                return "paie"
            if pa_type == "3":
                return "indem"
            # If no pa_type, try column-based heuristic
            keys = {k.lower() for k in rec}
            if "pa_netpay" in keys or "pa_salbrut" in keys:
                return "paie"
            if "pa_cind" in keys or "pa_natu" in keys and "pa_salbrut" not in keys:
                return "indem"
            break
    except Exception:
        pass
    return None


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


# ── SSE progress endpoint ─────────────────────────────────────────────────────

@app.get("/progress/{run_id}", tags=["ETL"])
async def stream_progress(run_id: str):
    """Server-Sent Events stream — push pipeline progress to the browser."""
    async def _gen():
        for _ in range(7_200):          # max 2 hours of 1s ticks
            data = _progress.get(run_id, {"stage": "waiting", "pct": 0, "msg": "Waiting for pipeline…"})
            yield f"data: {json.dumps(data)}\n\n"
            if data.get("stage") in ("done", "error"):
                break
            await asyncio.sleep(1)

    return StreamingResponse(
        _gen(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no",
                 "Access-Control-Allow-Origin": "*"},
    )


# ── Background pipeline runner ────────────────────────────────────────────────

def _run_pipeline_sync(run_id: str, dest: Path, resolved_type: str,
                       clean_dir: Path, reset: bool, retrain: bool,
                       limit: int | None = None) -> None:
    """Blocking ETL + DW load — runs in a thread pool."""
    try:
        def cb(pct: int, msg: str, **kw):
            _prog(run_id, "etl", pct, msg, **kw)

        _prog(run_id, "etl_start", 12,
              f"Starting ETL pipeline…{' (test mode: first {:,} rows)'.format(limit) if limit else ''}")

        if resolved_type == "paie":
            from etl.pipeline_paie import run as run_paie
            etl_report = run_paie(source=dest, run_id=run_id, out_dir=clean_dir,
                                  progress_cb=cb, limit=limit)
        else:
            from etl.pipeline_indem import run as run_indem
            etl_report = run_indem(source=dest, run_id=run_id, out_dir=clean_dir,
                                   progress_cb=cb, limit=limit)

        qg = etl_report.get("quality_gate", {})
        _prog(run_id, "quality_gate", 75, "Quality gate checks…", qg_status=qg.get("status"))

        if qg.get("status", "").startswith("FAIL") and not qg.get("status", "").endswith("WARNINGS"):
            _etl_job_update(run_id, "FAIL", None, qg.get("status"), str(qg.get("errors")))
            _prog(run_id, "error", 75, f"Quality gate FAILED: {qg.get('errors')}")
            return

        _prog(run_id, "dw_dims", 80, "Loading dimension tables into DW…")
        from etl.load_dw import run as load_dw

        def dw_cb(pct: int, msg: str, **kw):
            _prog(run_id, "dw_facts" if pct > 85 else "dw_dims", pct, msg, **kw)

        dw_result = load_dw(reset=reset, clean_dir=clean_dir, progress_cb=dw_cb)
        rows_written = sum(dw_result.get("records_loaded", {}).values()) if dw_result else None

        ml_status = "not_requested"
        if retrain:
            _prog(run_id, "ml", 97, "Retraining ML models…")
            try:
                from ml.run_all_models import main as run_ml
                run_ml()
                ml_status = "retrained_ok"
            except Exception as ml_err:
                ml_status = f"failed: {ml_err}"

        _etl_job_update(run_id, "PASS", rows_written, qg.get("status", "PASS"), None)
        _prog(run_id, "done", 100, "Pipeline complete!",
              rows=rows_written, ml_status=ml_status,
              etl_stats=etl_report.get("stats", {}),
              dw_counts=dw_result.get("table_counts", {}),
              records_loaded=dw_result.get("records_loaded", {}),
              quality_gate=qg)

    except Exception as exc:
        _etl_job_update(run_id, "FAIL", None, None, str(exc))
        _prog(run_id, "error", -1, f"Pipeline failed: {exc}")
    finally:
        shutil.rmtree(clean_dir, ignore_errors=True)


_ALLOWED_EXTENSIONS = {".json", ".jsonl", ".csv", ".xlsx", ".xls"}


def _etl_job_insert(run_id: str, file_name: str, file_type: str, uploaded_by: str | None):
    try:
        import psycopg2
        from etl.core.config import DB_CONFIG
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO public.etl_jobs (run_id, file_name, file_type, status, uploaded_by)
                    VALUES (%s, %s, %s, 'RUNNING', %s)
                    ON CONFLICT (run_id) DO NOTHING
                """, (run_id, file_name, file_type, uploaded_by))
    except Exception:
        pass  # job tracking is non-critical


def _etl_job_update(run_id: str, status: str, rows_written: int | None,
                    qg_status: str | None, error_detail: str | None):
    try:
        import psycopg2
        from etl.core.config import DB_CONFIG
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public.etl_jobs
                    SET status = %s, finished_at = NOW(),
                        rows_written = %s, qg_status = %s, error_detail = %s
                    WHERE run_id = %s
                """, (status, rows_written, qg_status, error_detail, run_id))
    except Exception:
        pass


@app.post("/ingest-path", tags=["ETL"])
async def ingest_from_path(
    background_tasks: BackgroundTasks,
    file_path: str           = Query(..., description="Absolute path to the file on the server"),
    file_type: Optional[str] = Query(None, enum=["paie", "indem"],
                                     description="'paie' or 'indem' — omit to auto-detect"),
    retrain: bool            = Query(False),
    reset: bool              = Query(False),
    limit: Optional[int]     = Query(None, ge=1,
                                     description="Stop after N written rows (test mode)"),
    uploaded_by: Optional[str] = Query(None),
):
    """
    Run the full ETL pipeline on a file that already exists on the server.
    Avoids HTTP upload of large files — FastAPI reads the file directly from disk.
    """
    source = Path(file_path)
    # Allow repo-relative paths like /data/newRawData/paie.json on Windows
    if not source.exists():
        relative = source.parts[1:] if source.parts and source.parts[0] in ('/', '\\') else source.parts
        source = PROJECT_ROOT.joinpath(*relative)
    if not source.exists():
        raise HTTPException(status_code=404, detail=f"File not found: {file_path}")
    if source.suffix.lower() not in _ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400,
                            detail=f"Unsupported file type '{source.suffix}'. "
                                   f"Accepted: {', '.join(_ALLOWED_EXTENSIONS)}")

    run_id     = uuid.uuid4().hex[:8]
    started_at = datetime.utcnow().isoformat()

    resolved_type = file_type or _detect_file_type(source)
    if not resolved_type:
        raise HTTPException(status_code=422,
                            detail="Could not auto-detect file type. "
                                   "Pass ?file_type=paie or ?file_type=indem explicitly.")

    _etl_job_insert(run_id, source.name, resolved_type, uploaded_by)

    clean_dir = STAGING_CLEAN / run_id
    clean_dir.mkdir(parents=True, exist_ok=True)

    size_mb = source.stat().st_size // 1_048_576
    limit_note = f" · test limit: first {limit:,} rows" if limit else ""
    _prog(run_id, "saved", 8, f"File found ({size_mb} MB){limit_note} — starting pipeline…")

    loop = asyncio.get_event_loop()
    background_tasks.add_task(
        loop.run_in_executor,
        _pipeline_executor,
        _run_pipeline_sync,
        run_id, source, resolved_type, clean_dir, reset, retrain, limit,
    )

    return JSONResponse(content={
        "run_id":        run_id,
        "started_at":    started_at,
        "file":          source.name,
        "original_name": source.name,
        "detected_type": resolved_type,
        "status":        "processing",
        "limit":         limit,
    })


@app.post("/upload", tags=["ETL"])
async def upload_payroll_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    file_type: Optional[str] = Query(None, enum=["paie", "indem", "auto"],
                                     description="'paie', 'indem', or omit/auto to detect from content"),
    retrain: bool         = Query(False, description="Retrain ML models after loading"),
    reset: bool           = Query(False, description="Truncate fact tables before loading (full reload)"),
    limit: Optional[int]  = Query(None, ge=1, description="Stop after N written rows (test mode)"),
    uploaded_by: Optional[str] = Query(None, description="Username of the uploader"),
):
    """
    Upload a payroll data file (JSON / JSONL / CSV / Excel).

    Full automated pipeline:
      1. Save file to data/raw/  (timestamped — never overwrites previous uploads)
      2. Run ETL pipeline        (encoding fix, normalise, match references, DQ flags)
      3. Load into PostgreSQL DW (UPSERT — idempotent, no duplicates)
      4. Optionally retrain all ML models (6 forecast + 5 anomaly)

    Returns run metadata, ETL stats, quality gate result, and DW row counts.
    """
    suffix = Path(file.filename).suffix.lower()
    if suffix not in _ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{suffix}'. Accepted: {', '.join(_ALLOWED_EXTENSIONS)}"
        )

    run_id     = uuid.uuid4().hex[:8]
    started_at = datetime.utcnow().isoformat()

    # ── Save raw to staging (outside repo) — kept RAW_BACKUP_DAYS days ──────
    _cleanup_old_raw()
    stem = Path(file.filename).stem
    dest = STAGING_RAW / f"{stem}_{run_id}{suffix}"
    with dest.open("wb") as out:
        shutil.copyfileobj(file.file, out)

    # ── Auto-detect file type from content if not provided ───────────────────
    resolved_type = file_type if file_type and file_type != "auto" else _detect_file_type(dest)
    if not resolved_type:
        dest.unlink(missing_ok=True)
        raise HTTPException(status_code=422,
                            detail="Could not auto-detect file type. "
                                   "Pass ?file_type=paie or ?file_type=indem explicitly.")

    _etl_job_insert(run_id, dest.name, resolved_type, uploaded_by)

    # ── Staging clean dir for this run — deleted by background runner ─────────
    clean_dir = STAGING_CLEAN / run_id
    clean_dir.mkdir(parents=True, exist_ok=True)

    # ── Initialise progress and launch ETL in background ─────────────────────
    limit_note = f" · test limit: first {limit:,} rows" if limit else ""
    _prog(run_id, "saved", 8, f"File saved ({dest.stat().st_size // 1_048_576} MB){limit_note} — starting pipeline…")

    loop = asyncio.get_event_loop()
    background_tasks.add_task(
        loop.run_in_executor,
        _pipeline_executor,
        _run_pipeline_sync,
        run_id, dest, resolved_type, clean_dir, reset, retrain, limit,
    )

    # Return immediately — frontend polls /progress/{run_id} via SSE
    return JSONResponse(content={
        "run_id":        run_id,
        "started_at":    started_at,
        "file":          dest.name,
        "original_name": file.filename,
        "detected_type": resolved_type,
        "status":        "processing",
    })


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
