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
import time
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

# Simple in-memory cache for expensive DB queries
_cache: dict[str, tuple[float, object]] = {}
_CACHE_TTL = 3600  # 1 hour

def _cache_get(key: str):
    entry = _cache.get(key)
    if entry and time.time() - entry[0] < _CACHE_TTL:
        return entry[1]
    return None

def _cache_set(key: str, value: object):
    _cache[key] = (time.time(), value)

def _load_anomaly_report():
    """Load anomaly_report.csv with 1-hour in-memory cache."""
    cached = _cache_get("anomaly_report")
    if cached is not None:
        return cached
    import pandas as pd
    report_path = Path(__file__).resolve().parent.parent / "ml" / "models" / "anomaly_report.csv"
    if not report_path.exists():
        return None
    df = pd.read_csv(report_path)
    _cache_set("anomaly_report", df)
    return df


def _classify_anomaly(row: dict) -> dict:
    """Rule-based anomaly diagnosis — type, plain-language explanation, recommended action."""
    z       = float(row.get("z_score") or 0)
    pct_dev = float(row.get("pct_deviation") or 0)
    pct_chg = row.get("pct_change_vs_prev")   # None when temporal context unavailable
    next1   = row.get("pay_next_1m")
    next2   = row.get("pay_next_2m")
    mean    = float(row.get("emp_mean") or 0)
    netpay  = float(row.get("m_netpay") or 0)

    abs_z    = abs(z)
    is_spike = z > 0
    is_drop  = z < 0
    abs_pct  = abs(pct_dev)

    # Does the deviation persist into the next two months?
    persistent = False
    if mean > 0 and next1 is not None and next2 is not None:
        n1, n2 = float(next1), float(next2)
        if is_spike:
            persistent = n1 > mean * 1.15 and n2 > mean * 1.15
        elif is_drop:
            persistent = n1 < mean * 0.85 and n2 < mean * 0.85

    chg_str = f" ({pct_chg:+.1f}% vs previous month)" if pct_chg is not None else ""

    if abs_z >= 3.5 and is_spike:
        atype  = "extreme_spike"
        expl   = (f"Net pay of {netpay:,.0f} TND is {abs_pct:.1f}% above the employee's "
                  f"historical average ({mean:,.0f} TND) - a statistically extreme spike (z={z:.2f}).")
        action = ("Verify for unauthorized bonus, payroll error, or duplicate payment. "
                  "Cross-check with HR approval chain and supporting documents.")
    elif abs_z >= 3.5 and is_drop:
        atype  = "extreme_drop"
        expl   = (f"Net pay of {netpay:,.0f} TND is {abs_pct:.1f}% below the historical "
                  f"average ({mean:,.0f} TND) - a statistically extreme reduction (z={z:.2f}).")
        action = ("Investigate potential unprocessed salary elements, illegal deductions, "
                  "or incorrect payroll computation for this period.")
    elif is_spike and persistent:
        atype  = "persistent_raise"
        expl   = (f"Net pay has been elevated for multiple consecutive months "
                  f"({abs_pct:.1f}% above mean), suggesting a structural, ongoing change.")
        action = ("Confirm whether a grade promotion or salary adjustment was officially "
                  "approved and properly documented in the HR system.")
    elif is_drop and persistent:
        atype  = "persistent_drop"
        expl   = (f"Net pay has been significantly below average for multiple consecutive months "
                  f"({abs_pct:.1f}% below mean), indicating a sustained reduction.")
        action = ("Verify if a demotion, disciplinary action, or benefit removal was applied "
                  "and correctly authorized through the proper channels.")
    elif is_spike:
        atype  = "one_time_spike"
        expl   = (f"A one-time net pay spike of {abs_pct:.1f}% above the employee's mean "
                  f"was detected{chg_str}. Pay appears normal in adjacent months.")
        action = ("Check for retroactive payments, one-time indemnities, overtime pay, "
                  "or data entry errors specific to this pay period.")
    elif is_drop:
        atype  = "one_time_drop"
        expl   = (f"A one-time net pay drop of {abs_pct:.1f}% below the employee's mean "
                  f"was observed{chg_str}. Pay appears normal in adjacent months.")
        action = ("Check for missed allowances, erroneous deductions, "
                  "or partial-month payment in this period.")
    else:
        atype  = "pattern_anomaly"
        expl   = (f"The payment pattern is atypical for this employee based on ML analysis "
                  f"(z={z:.2f}), though the absolute deviation may be modest.")
        action = ("Review the employee's pay history for this period "
                  "and verify the computation against applicable payroll rules.")

    return {
        "anomaly_type":       atype,
        "explanation":        expl,
        "recommended_action": action,
    }

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
    history:  list[dict] = []


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
    Full forecast payload: winner model, all model metrics, historical series,
    6-month forecast with confidence band.
    """
    cache_key = f"forecast_{n}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        import json as _json
        from ml.predict import predict_payroll_next_months
        from ml.data_loader import load_monthly_payroll

        predictions = predict_payroll_next_months(n_months=n)

        df = load_monthly_payroll()
        df = df.sort_values("month_start_date").reset_index(drop=True)

        historical = [
            {
                "date":           row["month_start_date"].strftime("%Y-%m"),
                "actual_netpay":  round(float(row["total_netpay"]), 2),
                "employee_count": int(row["employee_count"]),
                "avg_netpay":     round(float(row["avg_netpay"]), 2),
            }
            for _, row in df.iterrows()
        ]

        results_path = MODELS_DIR / "payroll_forecast_results.json"
        model_comparison: dict = {}
        winner = "sarima"
        train_months = 0
        test_months  = 0
        if results_path.exists():
            res = _json.loads(results_path.read_text(encoding="utf-8"))
            model_comparison = res.get("model_comparison", {})
            winner       = res.get("winner", "sarima")
            train_months = res.get("train_months", 0)
            test_months  = res.get("test_months", 0)

        winner_m  = model_comparison.get(winner, {})
        avg_hist  = float(df["total_netpay"].mean())
        avg_fore  = sum(p["predicted_netpay"] for p in predictions) / max(len(predictions), 1)
        rmse      = winner_m.get("rmse", avg_hist * 0.05)

        forecast_with_ci = [
            {
                **p,
                "lower": round(max(p["predicted_netpay"] - 1.96 * rmse, 0), 2),
                "upper": round(p["predicted_netpay"] + 1.96 * rmse, 2),
            }
            for p in predictions
        ]

        total_months = train_months + test_months
        last_data_date = historical[-1]["date"] if historical else None

        result = {
            "model":            winner,
            "n_months":         n,
            "mape":             winner_m.get("mape"),
            "smape":            winner_m.get("smape"),
            "mase":             winner_m.get("mase"),
            "da":               winner_m.get("da"),
            "mae":              winner_m.get("mae"),
            "rmse":             winner_m.get("rmse"),
            "train_months":     train_months,
            "test_months":      test_months,
            "total_months":     total_months,
            "last_data_date":   last_data_date,
            "avg_historical":   round(avg_hist, 2),
            "avg_forecast":     round(avg_fore, 2),
            "model_comparison": model_comparison,
            "forecast":         forecast_with_ci,
            "historical":       historical,
            "currency":         "TND",
        }
        _cache_set(cache_key, result)
        return result
    except FileNotFoundError:
        raise HTTPException(status_code=503,
            detail="Forecast model not found. Run 'python -m ml.run_all_models' first.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/forecast/dimensions", tags=["ML"])
def get_forecast_dimensions(ministry: Optional[str] = Query(None)):
    """
    Return ministries and grades for filter dropdowns.
    When ?ministry=X is provided, grades are scoped to that ministry only.
    Both fr and ar labels are always returned; the client picks based on lang.
    Results are cached for 5 minutes to avoid slow JOIN queries.
    """
    cache_key = f"dims:{ministry or ''}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        import psycopg2
        from etl.core.config import DB_CONFIG
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Ministries: join with dim_etablissement for Arabic names
                min_cached = _cache_get("dims:ministries")
                if min_cached is not None:
                    ministries = min_cached
                else:
                    cur.execute("""
                        SELECT m.ministry_code,
                               m.ministry_name                       AS name_fr,
                               COALESCE(de.libletaba, de.libcetaba)  AS name_ar
                        FROM dw.mv_payroll_by_ministry m
                        LEFT JOIN dw.dim_etablissement de
                               ON de.codetab = m.ministry_code
                        WHERE m.ministry_code IS NOT NULL
                        ORDER BY m.total_netpay DESC
                    """)
                    ministries = [
                        {"code": r[0], "name_fr": r[1] or r[0], "name_ar": r[2] or r[1] or r[0]}
                        for r in cur.fetchall()
                    ]
                    _cache_set("dims:ministries", ministries)

                if ministry:
                    cur.execute("""
                        SELECT grade_code, grade_label_fr, grade_label_ar, category, cnt
                        FROM dw.mv_grades_by_ministry
                        WHERE ministry_code = %s
                        ORDER BY cnt DESC
                        LIMIT 80
                    """, (ministry,))
                else:
                    cur.execute("""
                        SELECT dg.grade_code, dg.grade_label_fr, dg.grade_label_ar,
                               dg.category
                        FROM dw.mv_grade_distribution mgd
                        JOIN dw.dim_grade dg ON dg.grade_code = mgd.grade_code
                        WHERE mgd.grade_code IS NOT NULL
                        ORDER BY mgd.total_netpay DESC
                        LIMIT 60
                    """)
                grades = [
                    {
                        "code":     r[0],
                        "label_fr": r[1] or r[0],
                        "label_ar": r[2] or r[1] or r[0],
                        "category": r[3],
                    }
                    for r in cur.fetchall()
                ]

        result = {"ministries": ministries, "grades": grades}
        _cache_set(cache_key, result)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/forecast/historical", tags=["ML"])
def get_forecast_historical(
    ministry: Optional[str] = Query(None),
    grade:    Optional[str] = Query(None),
):
    """
    Historical monthly payroll filtered by ministry or grade.
    Used by the forecast page to show filtered trend context.
    """
    try:
        import psycopg2
        from etl.core.config import DB_CONFIG

        conditions = ["fp.employee_sk <> 0", "dt.year_num > 0", "fp.m_netpay IS NOT NULL"]
        params: list = []

        if ministry:
            conditions.append("fp.codetab = %s")
            params.append(ministry)
        if grade:
            conditions.append("dg.grade_code = %s")
            params.append(grade)

        join_grade = "JOIN dw.dim_grade dg ON dg.grade_sk = fp.grade_sk" if grade else ""

        sql = f"""
            SELECT
                dt.year_num, dt.month_num, dt.month_start_date,
                COUNT(*)             AS employee_count,
                SUM(fp.m_netpay)     AS total_netpay,
                AVG(fp.m_netpay)     AS avg_netpay
            FROM dw.fact_paie fp
            JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
            {join_grade}
            WHERE {' AND '.join(conditions)}
            GROUP BY dt.year_num, dt.month_num, dt.month_start_date
            ORDER BY dt.year_num, dt.month_num
        """

        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]

        import pandas as pd
        df = pd.DataFrame(rows, columns=cols)
        df["month_start_date"] = pd.to_datetime(df["month_start_date"])

        return {
            "ministry": ministry,
            "grade":    grade,
            "data": [
                {
                    "date":           row["month_start_date"].strftime("%Y-%m"),
                    "actual_netpay":  round(float(row["total_netpay"]), 2),
                    "employee_count": int(row["employee_count"]),
                    "avg_netpay":     round(float(row["avg_netpay"]), 2),
                }
                for _, row in df.iterrows()
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/forecast/employee", tags=["ML"])
def get_employee_forecast(employee_id: str = Query(..., description="Employee national ID (CIN)")):
    """
    Return historical monthly net pay for a single employee + a 6-month
    seasonal-naive projection based on their own pay history.
    Looks up the employee by national ID (employee_id) via dim_employee.
    """
    try:
        import psycopg2
        import pandas as pd
        from etl.core.config import DB_CONFIG

        employee_id = employee_id.strip()

        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Try CIN lookup first; if input is purely numeric also try as employee_sk
                cur.execute("""
                    SELECT de.employee_sk, de.last_name, de.first_name,
                           de.birth_date, de.hire_date, de.gender
                    FROM dw.dim_employee de
                    WHERE de.employee_id = %s AND de.is_unknown = FALSE
                    LIMIT 1
                """, (employee_id,))
                emp_row = cur.fetchone()

                # Fallback: treat numeric input as employee_sk directly
                if not emp_row and employee_id.isdigit():
                    sk = int(employee_id)
                    cur.execute("""
                        SELECT de.employee_sk, de.last_name, de.first_name,
                               de.birth_date, de.hire_date, de.gender
                        FROM dw.dim_employee de
                        WHERE de.employee_sk = %s AND de.is_unknown = FALSE
                        LIMIT 1
                    """, (sk,))
                    emp_row = cur.fetchone()

                    # If still not in dim_employee, synthesise a minimal row from fact_paie
                    if not emp_row:
                        cur.execute(
                            "SELECT %s, NULL, NULL, NULL, NULL, NULL "
                            "FROM dw.fact_paie WHERE employee_sk = %s LIMIT 1",
                            (sk, sk)
                        )
                        emp_row = cur.fetchone()

        if not emp_row:
            raise HTTPException(status_code=404, detail=f"Employee '{employee_id}' not found")

        employee_sk, last_name, first_name, birth_date, hire_date, gender = emp_row

        # Compute age from birth_date
        age = None
        if birth_date:
            today = datetime.today().date()
            age = today.year - birth_date.year - (
                (today.month, today.day) < (birth_date.month, birth_date.day)
            )

        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT dt.year_num, dt.month_num, dt.month_start_date,
                           fp.m_netpay,
                           dg.grade_code, dg.grade_label_fr,
                           fp.codetab
                    FROM dw.fact_paie fp
                    JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
                    LEFT JOIN dw.dim_grade dg ON dg.grade_sk = fp.grade_sk
                    WHERE fp.employee_sk = %s
                      AND fp.m_netpay IS NOT NULL
                      AND dt.year_num > 0
                    ORDER BY dt.year_num, dt.month_num
                """, (employee_sk,))
                rows = cur.fetchall()

        if not rows:
            raise HTTPException(status_code=404, detail=f"No payroll data found for employee '{employee_id}'")

        cols = ["year_num", "month_num", "month_start_date", "m_netpay",
                "grade_code", "grade_label_fr", "codetab"]
        df = pd.DataFrame(rows, columns=cols)
        df["month_start_date"] = pd.to_datetime(df["month_start_date"])
        df = df.sort_values("month_start_date").reset_index(drop=True)
        df["m_netpay"] = df["m_netpay"].astype(float)

        # Remove extreme outliers (values > 10× median) caused by ETL artefacts
        # where aggregated ministry totals were accidentally assigned to one employee
        median_pay = float(df["m_netpay"].median())
        if median_pay > 0:
            df = df[df["m_netpay"] <= median_pay * 10].reset_index(drop=True)

        historical = [
            {
                "date":   row["month_start_date"].strftime("%Y-%m"),
                "netpay": round(float(row["m_netpay"]), 2),
            }
            for _, row in df.iterrows()
        ]

        last_date = df["month_start_date"].max()

        # Year-over-year approach:
        # 1. Compute annual growth rate: avg of last 12 months vs previous 12 months
        last_12 = df.tail(12)["m_netpay"].astype(float)
        if len(df) >= 24:
            prev_12 = df.iloc[-24:-12]["m_netpay"].astype(float)
        else:
            prev_12 = df.head(max(1, len(df) - 12))["m_netpay"].astype(float)

        prev_mean = float(prev_12.mean()) if len(prev_12) > 0 else float(last_12.mean())
        yoy_rate  = float(last_12.mean()) / prev_mean if prev_mean > 0 else 1.0
        yoy_rate  = min(max(yoy_rate, 1.0), 1.20)   # floor at 0%, cap at 20% annual

        # 2. Build lookup: most recent actual value seen for each calendar month
        month_base: dict[int, float] = {}
        for _, row in df.iterrows():
            month_base[int(row["month_num"])] = float(row["m_netpay"])

        # CI width: use std of last 24 non-null pay values; fall back to 8% of mean
        recent_vals = df["m_netpay"].dropna().tail(24)
        recent_std  = float(recent_vals.std()) if len(recent_vals) >= 2 else 0.0
        if pd.isna(recent_std) or recent_std < 1:
            recent_std = float(last_12.mean()) * 0.08

        # 3. Forecast = last year's same month × YoY growth rate
        forecast = []
        for i in range(1, 7):
            next_date = last_date + pd.DateOffset(months=i)
            base = month_base.get(next_date.month, float(last_12.mean()))
            pred = round(base * yoy_rate, 2)
            forecast.append({
                "date":   next_date.strftime("%Y-%m"),
                "netpay": pred,
                "lower":  round(max(pred - 1.645 * recent_std, 0), 2),
                "upper":  round(pred + 1.645 * recent_std, 2),
            })

        grade_code  = df["grade_code"].dropna().iloc[-1]  if not df["grade_code"].dropna().empty  else None
        grade_label = df["grade_label_fr"].dropna().iloc[-1] if not df["grade_label_fr"].dropna().empty else None
        ministry    = df["codetab"].dropna().iloc[-1]     if not df["codetab"].dropna().empty     else None

        return {
            "employee_id":    employee_id,
            "employee_sk":    employee_sk,
            "last_name":      last_name,
            "first_name":     first_name,
            "birth_date":     birth_date.isoformat() if birth_date else None,
            "age":            age,
            "hire_date":      hire_date.isoformat() if hire_date else None,
            "gender":         gender,
            "grade_code":     grade_code,
            "grade_label":    grade_label,
            "ministry":       ministry,
            "months_of_data": len(historical),
            "avg_netpay":     round(float(df["m_netpay"].mean()), 2),
            "yoy_rate":       round(yoy_rate, 4),
            "historical":     historical,
            "forecast":       forecast,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/forecast/feature-importance", tags=["ML"])
def get_feature_importance():
    """
    Returns feature importances for the winning ML model.
    Works only if winner is RF or XGBoost (tree-based).
    SARIMA/Prophet return an empty list with an explanatory note.
    """
    try:
        import json as _json
        import joblib

        # Prefer pre-saved importances from results JSON (populated after retrain)
        results_path = MODELS_DIR / "payroll_forecast_results.json"
        if results_path.exists():
            res = _json.loads(results_path.read_text(encoding="utf-8"))
            if res.get("feature_importances"):
                return {
                    "importances": res["feature_importances"],
                    "model":       res.get("winner", ""),
                    "source":      "pretrained",
                }

        # Fallback: load winner pkl and extract importances dynamically
        winner_name = joblib.load(MODELS_DIR / "payroll_forecast_winner.pkl")
        model       = joblib.load(MODELS_DIR / "payroll_forecast.pkl")
        features    = joblib.load(MODELS_DIR / "payroll_forecast_features.pkl")

        if hasattr(model, "feature_importances_"):
            raw = model.feature_importances_
            total = float(raw.sum()) or 1.0
            items = sorted(
                [{"feature": f, "importance": round(float(v / total), 6)}
                 for f, v in zip(features, raw)],
                key=lambda x: x["importance"], reverse=True,
            )
            return {"importances": items, "model": winner_name, "source": "pkl"}

        return {
            "importances": [],
            "model":       winner_name,
            "source":      "none",
            "note":        f"{winner_name} is a statistical model — feature importances are not applicable.",
        }
    except Exception as e:
        return {"importances": [], "model": "", "source": "error", "note": str(e)}


@app.get("/anomalies", tags=["ML"])
def get_anomalies(
    limit: int = Query(100, ge=1, le=500),
    ministry:  Optional[str] = Query(None, description="Filter by ministry code"),
    year:      Optional[int] = Query(None, description="Filter by year"),
):
    """
    Returns a balanced sample of anomalies (top N per severity tier) so all three
    severity levels are represented in the table.  KPI counts come from the FULL
    dataset via severity_summary — not from the returned subset.
    """
    import numpy as np
    import pandas as pd
    full_df = _load_anomaly_report()
    if full_df is None:
        raise HTTPException(
            status_code=503,
            detail="Anomaly report not found. Run 'python -m ml.run_all_models' first."
        )

    total    = int(len(full_df))
    z_all    = full_df["z_score"].abs()
    severity_summary = {
        "high":   int((z_all >= 3.5).sum()),
        "medium": int(((z_all >= 2.5) & (z_all < 3.5)).sum()),
        "low":    int((z_all < 2.5).sum()),
    }

    # Read correct rate from the model's own results JSON (avoids dividing flagged/flagged)
    try:
        _results_path = MODELS_DIR / "anomaly_results.json"
        _meta = json.loads(_results_path.read_text(encoding="utf-8"))
        anomaly_rate_pct = round(_meta["final_flag"]["anomaly_rate"], 2)
        total_sampled_records   = int(_meta.get("total_records",   total))
        total_sampled_employees = int(_meta.get("total_employees", 0))
    except Exception:
        anomaly_rate_pct        = 0.0
        total_sampled_records   = total
        total_sampled_employees = int(full_df["employee_sk"].nunique())

    # Apply optional filters
    df = full_df.copy()
    if ministry:
        df = df[df["ministry_code"] == ministry]
    if year:
        df = df[df["year_num"] == year]

    # Balanced fetch: top N records from each severity tier
    per_tier   = max(limit // 3, 1)
    z_abs      = df["z_score"].abs()
    high_rows  = df[z_abs >= 3.5].sort_values("z_score", key=abs, ascending=False).head(per_tier)
    med_rows   = df[(z_abs >= 2.5) & (z_abs < 3.5)].sort_values("z_score", key=abs, ascending=False).head(per_tier)
    low_rows   = df[z_abs < 2.5].sort_values("z_score", key=abs, ascending=False).head(per_tier)

    result_df  = (
        pd.concat([high_rows, med_rows, low_rows])
        .sort_values("z_score", key=abs, ascending=False)
    )

    # Detect whether the CSV has the new columns (post-retrain)
    has_new_cols = "detection_method" in df.columns and df["detection_method"].notna().any()

    # Enrich each record with rule-based anomaly diagnosis
    records = json.loads(result_df.to_json(orient="records"))
    for rec in records:
        rec.update(_classify_anomaly(rec))

    return {
        "total_anomalies_in_report": total,
        "total_sampled_records":     total_sampled_records,
        "total_sampled_employees":   total_sampled_employees,
        "severity_summary":          severity_summary,
        "anomaly_rate_pct":          anomaly_rate_pct,
        "has_new_cols":              bool(has_new_cols),
        "returned":                  int(len(result_df)),
        "filters":                   {"ministry": ministry, "year": year},
        "anomalies":                 records,
    }


@app.get("/anomalies/by-ministry", tags=["ML"])
def get_anomalies_by_ministry():
    """Aggregated anomaly counts grouped by ministry."""
    import numpy as np
    df = _load_anomaly_report()
    if df is None:
        raise HTTPException(503, "Anomaly report not found.")

    rows = []
    for ministry, g in df.groupby("ministry_code"):
        z_abs = g["z_score"].abs()
        rows.append({
            "ministry_code":   str(ministry),
            "ministry_name":   str(g["ministry_name_fr"].iloc[0]) if "ministry_name_fr" in g.columns else str(ministry),
            "total_anomalies": int(len(g)),
            "high":            int((z_abs >= 3.5).sum()),
            "medium":          int(((z_abs >= 2.5) & (z_abs < 3.5)).sum()),
            "low":             int((z_abs < 2.5).sum()),
            "avg_z_score":     round(float(z_abs.mean()), 3),
            "max_z_score":     round(float(z_abs.max()), 3),
        })
    rows.sort(key=lambda r: -r["total_anomalies"])
    return rows


@app.get("/anomalies/by-grade", tags=["ML"])
def get_anomalies_by_grade():
    """Aggregated anomaly counts grouped by grade."""
    import numpy as np
    df = _load_anomaly_report()
    if df is None:
        raise HTTPException(503, "Anomaly report not found.")

    rows = []
    for grade, g in df.groupby("grade_code"):
        z_abs = g["z_score"].abs()
        rows.append({
            "grade_code":      str(grade),
            "total_anomalies": int(len(g)),
            "high":            int((z_abs >= 3.5).sum()),
            "medium":          int(((z_abs >= 2.5) & (z_abs < 3.5)).sum()),
            "low":             int((z_abs < 2.5).sum()),
            "avg_z_score":     round(float(z_abs.mean()), 3),
            "max_z_score":     round(float(z_abs.max()), 3),
        })
    rows.sort(key=lambda r: -r["total_anomalies"])
    return rows[:60]


@app.get("/anomalies/temporal-context", tags=["ML"])
def get_anomaly_temporal_context(
    employee_sk: int = Query(...),
    year_num:    int = Query(...),
    month_num:   int = Query(...),
):
    """
    Fetch the 5 months surrounding an anomaly directly from the DW.
    Returns pay_prev_2m, pay_prev_1m, current, pay_next_1m, pay_next_2m, pay_next_3m.
    """
    try:
        import psycopg2
        from etl.core.config import DB_CONFIG

        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT dt.year_num, dt.month_num, fp.m_netpay
                    FROM dw.fact_paie fp
                    JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
                    WHERE fp.employee_sk = %s
                      AND fp.m_netpay IS NOT NULL
                      AND dt.year_num > 0
                      AND (
                        (dt.year_num * 12 + dt.month_num)
                        BETWEEN (%s * 12 + %s - 2) AND (%s * 12 + %s + 3)
                      )
                    ORDER BY dt.year_num, dt.month_num
                """, (employee_sk, year_num, month_num, year_num, month_num))
                rows = cur.fetchall()

        pay_map = {(r[0], r[1]): float(r[2]) for r in rows}

        def _offset(base_y: int, base_m: int, delta: int):
            total = base_y * 12 + base_m + delta
            return (total // 12, total % 12 or 12)

        def _pay(delta: int):
            k = _offset(year_num, month_num, delta)
            v = pay_map.get(k)
            return v

        current = _pay(0)
        if current is None:
            raise HTTPException(404, "No data for this employee/period")

        prev_chg = None
        p1 = _pay(-1)
        if p1 and p1 > 0:
            prev_chg = round((current - p1) / p1 * 100, 2)

        return {
            "pay_prev_2m":        _pay(-2),
            "pay_prev_1m":        p1,
            "pay_current":        current,
            "pay_next_1m":        _pay(1),
            "pay_next_2m":        _pay(2),
            "pay_next_3m":        _pay(3),
            "pct_change_vs_prev": prev_chg,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
        result = chat(question=req.question, model=req.model, history=req.history)
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
