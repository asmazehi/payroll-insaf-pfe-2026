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
import logging
import os
import shutil
import tempfile

# Load .env (GROK_API_KEY etc.) before anything else
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed — env vars must be set manually
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

log = logging.getLogger(__name__)

# ── App setup ─────────────────────────────────────────────────────────────────
app = FastAPI(
    title="INSAF Payroll Intelligence Platform",
    description="API for payroll ETL, ML forecasting, salary prediction, and anomaly detection.",
    version="1.0.0",
)

_ALLOWED_ORIGINS = [
    "http://localhost:4200",   # Angular dev
    "http://localhost:8081",   # Spring Boot (internal calls)
    "http://127.0.0.1:4200",
    "http://127.0.0.1:8081",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "Accept"],
    allow_credentials=True,
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


def _resolve_ministry_codetabs(ministry: str) -> list[str]:
    """
    Expand a top-level ministry code to all its sub-establishment codetabs.
    Uses dw.v_ministry_codetabs: returns [ministry] itself + all establishments
    whose codtutel = ministry OR (natorg='8' AND ministry='W00').
    Caches results for 1 hour to avoid repeated DB lookups on every API call.
    """
    cache_key = f"ministry_codetabs:{ministry}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        from etl.core.config import DB_CONFIG
        import psycopg2
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT sub_codetab FROM dw.v_ministry_codetabs WHERE ministry_codetab = %s",
                    (ministry,)
                )
                codetabs = [r[0] for r in cur.fetchall()]
        if not codetabs:
            codetabs = [ministry]
    except Exception:
        codetabs = [ministry]

    _cache_set(cache_key, codetabs)
    return codetabs


def _build_ministry_name_map() -> dict[str, str]:
    """
    Build a codetab → parent ministry name mapping from the DB.
    Used to enrich the anomaly report so charts show ministry names
    (e.g. 'Ministère de la Jeunesse et des Sports') instead of
    establishment codes (e.g. 'A52 – Football Federation').
    Cached for 1 hour.
    """
    cached = _cache_get("ministry_name_map")
    if cached is not None:
        return cached

    try:
        from etl.core.config import DB_CONFIG
        import psycopg2
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT v.sub_codetab,
                           COALESCE(de.libletabl, de.libcetabl, v.ministry_codetab) AS ministry_name
                    FROM dw.v_ministry_codetabs v
                    JOIN dw.dim_etablissement de ON de.codetab = v.ministry_codetab
                """)
                mapping = {r[0]: r[1] for r in cur.fetchall()}
    except Exception:
        mapping = {}

    _cache_set("ministry_name_map", mapping)
    return mapping


def _load_reviews() -> dict:
    """Load review data from DB. Cached for 60s to avoid a DB hit on every anomaly request."""
    entry = _cache.get("reviews")
    cached = entry[1] if entry and time.time() - entry[0] < 60 else None
    if cached is not None:
        return cached
    try:
        from etl.core.config import DB_CONFIG
        import psycopg2
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT employee_sk, year_num, month_num, status, notes, reviewed_by, reviewed_at, dismissed_at
                    FROM public.anomaly_reviews
                """)
                result = {
                    (int(r[0]), int(r[1]), int(r[2])): {
                        "status":       r[3],
                        "notes":        r[4],
                        "reviewed_by":  r[5],
                        "reviewed_at":  str(r[6]) if r[6] else None,
                        "dismissed_at": str(r[7]) if r[7] else None,
                    }
                    for r in cur.fetchall()
                }
    except Exception:
        result = {}
    _cache["reviews"] = (time.time(), result)
    return result


def _invalidate_reviews_cache() -> None:
    _cache.pop("reviews", None)


def _load_anomaly_report():
    """Load anomaly_report.csv with 1-hour in-memory cache.
    Enriches ministry_code with parent ministry name so charts show the
    top-level ministry (e.g. 'Ministère de la Jeunesse et des Sports')
    instead of individual establishment labels (e.g. 'Football Federation').
    """
    cached = _cache_get("anomaly_report")
    if cached is not None:
        return cached
    import pandas as pd
    report_path = Path(__file__).resolve().parent.parent / "ml" / "models" / "anomaly_report.csv"
    if not report_path.exists():
        return None
    df = pd.read_csv(report_path)

    # Enrich with parent ministry code and name
    name_map = _build_ministry_name_map()
    if name_map and "ministry_code" in df.columns:
        # Build codetab -> parent_ministry_code mapping
        try:
            from etl.core.config import DB_CONFIG
            import psycopg2
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT sub_codetab, ministry_codetab FROM dw.v_ministry_codetabs"
                    )
                    parent_map = {r[0]: r[1] for r in cur.fetchall()}
        except Exception:
            parent_map = {}

        df["parent_ministry_code"] = df["ministry_code"].map(
            lambda c: parent_map.get(str(c), str(c)) if c is not None else None
        )
        df["parent_ministry_name"] = df["parent_ministry_code"].map(
            lambda c: name_map.get(str(c), str(c)) if c is not None else None
        )

    _cache_set("anomaly_report", df)
    return df


_T = {
    "en": {
        "expl_spike":      "Net pay of {netpay:,.0f} TND is {pct:.1f}% above the employee's historical average ({mean:,.0f} TND) — a statistically extreme spike (z={z:.2f}).",
        "expl_drop":       "Net pay of {netpay:,.0f} TND is {pct:.1f}% below the historical average ({mean:,.0f} TND) — a statistically extreme reduction (z={z:.2f}).",
        "expl_pers_raise": "Net pay has been elevated for multiple consecutive months ({pct:.1f}% above mean), suggesting a structural, ongoing change.",
        "expl_pers_drop":  "Net pay has been significantly below average for multiple consecutive months ({pct:.1f}% below mean), indicating a sustained reduction.",
        "expl_one_spike":  "A one-time net pay spike of {pct:.1f}% above the employee's mean was detected{chg}. Pay appears normal in adjacent months.",
        "expl_one_drop":   "A one-time net pay drop of {pct:.1f}% below the employee's mean was observed{chg}. Pay appears normal in adjacent months.",
        "expl_pattern":    "The payment pattern is atypical for this employee based on ML analysis (z={z:.2f}), though the absolute deviation may be modest.",
        "act_spike":       "Cross-check the payment record against the HR authorization document and verify with the payroll officer responsible for this ministry.",
        "act_drop":        "Investigate potential unprocessed salary elements, illegal deductions, or incorrect payroll computation for this period.",
        "act_pers_raise":  "Confirm whether a grade promotion or salary adjustment was officially approved and properly documented in the HR system.",
        "act_pers_drop":   "Verify if a demotion, disciplinary action, or benefit removal was applied and correctly authorized through the proper channels.",
        "act_one_spike":   "Check for retroactive payments, one-time indemnities, overtime pay, or data entry errors specific to this pay period.",
        "act_one_drop":    "Check for missed allowances, erroneous deductions, or partial-month payment in this period.",
        "act_pattern":     "Review the employee's pay history for this period and verify the computation against applicable payroll rules.",
        "pc_retro":        "Most likely retroactive salary adjustment: the salary increased from ~{pre:,.0f} to ~{post:,.0f} TND/month (+{raise_amt:,.0f} TND). This spike probably includes {months} months of back-pay ({raise_amt:,.0f} × {months} ≈ {total:,.0f} TND) paid as a lump sum in this period.",
        "pc_retro_large":  "Salary did rise from ~{pre:,.0f} to ~{post:,.0f} TND after this month, but the spike ({extra:,.0f} TND above new salary) is too large to be explained by retroactive pay alone. Likely also includes a one-time allowance or a data entry error.",
        "pc_duplicate":    "Possible duplicate payment: {netpay:,.0f} TND ≈ {ratio:.1f}× the usual salary ({pre:,.0f} TND). No salary change in the following months. The employee may have been paid twice for this period.",
        "pc_data_error":   "Most likely a data entry error: pay returned to {next:,.0f} TND the following month with no lasting change, and the z-score ({z:.1f}) is extreme even by anomaly standards. A wrong amount was probably keyed into the payroll system for this period.",
        "pc_onetime":      "One-time payment (~{extra:,.0f} TND above baseline) with no lasting salary change. Likely a one-off indemnity, overtime batch, or end-of-year allowance paid outside the normal cycle.",
        "pc_drop":         "Pay returned to normal the following month, suggesting a one-time deduction — possibly an overpayment recovery, absence penalty, or incorrect deduction that was later corrected.",
        "chg_str":         " ({pct:+.1f}% vs previous month)",
    },
    "fr": {
        "expl_spike":      "Le salaire net de {netpay:,.0f} TND est supérieur de {pct:.1f}% à la moyenne historique de l'employé ({mean:,.0f} TND) — un pic statistiquement extrême (z={z:.2f}).",
        "expl_drop":       "Le salaire net de {netpay:,.0f} TND est inférieur de {pct:.1f}% à la moyenne historique ({mean:,.0f} TND) — une réduction statistiquement extrême (z={z:.2f}).",
        "expl_pers_raise": "Le salaire net est élevé depuis plusieurs mois consécutifs ({pct:.1f}% au-dessus de la moyenne), suggérant un changement structurel.",
        "expl_pers_drop":  "Le salaire net est significativement inférieur à la moyenne depuis plusieurs mois consécutifs ({pct:.1f}% en dessous), indiquant une réduction durable.",
        "expl_one_spike":  "Un pic ponctuel de {pct:.1f}% au-dessus de la moyenne de l'employé a été détecté{chg}. Le salaire semble normal les mois adjacents.",
        "expl_one_drop":   "Une baisse ponctuelle de {pct:.1f}% en dessous de la moyenne de l'employé a été observée{chg}. Le salaire semble normal les mois adjacents.",
        "expl_pattern":    "Le schéma de paiement est atypique pour cet employé selon l'analyse ML (z={z:.2f}), bien que l'écart absolu soit modéré.",
        "act_spike":       "Vérifier le dossier de paiement par rapport au document d'autorisation RH et confirmer avec le responsable de la paie du ministère concerné.",
        "act_drop":        "Investiguer les éléments salariaux non traités, les déductions illégales ou le calcul de paie incorrect pour cette période.",
        "act_pers_raise":  "Confirmer si une promotion de grade ou un ajustement salarial a été officiellement approuvé et correctement documenté dans le système RH.",
        "act_pers_drop":   "Vérifier si une rétrogradation, une mesure disciplinaire ou une suppression d'avantage a été appliquée et autorisée par les voies appropriées.",
        "act_one_spike":   "Vérifier les paiements rétroactifs, les indemnités ponctuelles, les heures supplémentaires ou les erreurs de saisie spécifiques à cette période.",
        "act_one_drop":    "Vérifier les allocations manquantes, les déductions erronées ou le paiement partiel du mois dans cette période.",
        "act_pattern":     "Examiner l'historique salarial de l'employé pour cette période et vérifier le calcul selon les règles de paie applicables.",
        "pc_retro":        "Très probablement un rappel salarial rétroactif : le salaire est passé de ~{pre:,.0f} à ~{post:,.0f} TND/mois (+{raise_amt:,.0f} TND). Ce pic inclut probablement {months} mois de rappel ({raise_amt:,.0f} × {months} ≈ {total:,.0f} TND) versés en une seule fois.",
        "pc_retro_large":  "Le salaire a bien augmenté de ~{pre:,.0f} à ~{post:,.0f} TND après ce mois, mais le montant du pic ({extra:,.0f} TND au-dessus du nouveau salaire) est trop élevé pour être expliqué uniquement par un rappel. Peut également inclure une indemnité ponctuelle ou une erreur de saisie.",
        "pc_duplicate":    "Possible doublon de paiement : {netpay:,.0f} TND ≈ {ratio:.1f}× le salaire habituel ({pre:,.0f} TND). Aucun changement salarial les mois suivants. L'employé a peut-être été payé deux fois pour cette période.",
        "pc_data_error":   "Très probablement une erreur de saisie : le salaire est revenu à {next:,.0f} TND le mois suivant sans changement durable, et le z-score ({z:.1f}) est extrême même par rapport aux normes des anomalies. Un mauvais montant a probablement été saisi dans le système de paie.",
        "pc_onetime":      "Paiement ponctuel (~{extra:,.0f} TND au-dessus de la base) sans changement salarial durable. Probablement une indemnité exceptionnelle, un lot d'heures supplémentaires ou une prime de fin d'année hors cycle normal.",
        "pc_drop":         "Le salaire est revenu à la normale le mois suivant, suggérant une déduction ponctuelle — peut-être un remboursement de trop-perçu, une pénalité d'absence ou une déduction incorrecte corrigée ultérieurement.",
        "chg_str":         " ({pct:+.1f}% par rapport au mois précédent)",
    },
    "ar": {
        "expl_spike":      "صافي الأجر {netpay:,.0f} دينار يفوق المتوسط التاريخي للموظف ({mean:,.0f} دينار) بنسبة {pct:.1f}% — ارتفاع إحصائي استثنائي (z={z:.2f}).",
        "expl_drop":       "صافي الأجر {netpay:,.0f} دينار أقل من المتوسط التاريخي ({mean:,.0f} دينار) بنسبة {pct:.1f}% — انخفاض إحصائي استثنائي (z={z:.2f}).",
        "expl_pers_raise": "ارتفع صافي الأجر لعدة أشهر متتالية ({pct:.1f}% فوق المتوسط)، مما يشير إلى تغيير هيكلي مستمر.",
        "expl_pers_drop":  "انخفض صافي الأجر بشكل ملحوظ لعدة أشهر متتالية ({pct:.1f}% دون المتوسط)، مما يدل على انخفاض مستمر.",
        "expl_one_spike":  "تم رصد ارتفاع مؤقت بنسبة {pct:.1f}% فوق متوسط الموظف{chg}. يبدو الأجر طبيعياً في الأشهر المجاورة.",
        "expl_one_drop":   "تم رصد انخفاض مؤقت بنسبة {pct:.1f}% دون متوسط الموظف{chg}. يبدو الأجر طبيعياً في الأشهر المجاورة.",
        "expl_pattern":    "النمط المرتبط بالمدفوعات غير عادي لهذا الموظف وفق تحليل الذكاء الاصطناعي (z={z:.2f}).",
        "act_spike":       "مراجعة سجل الدفع مقابل وثيقة الاعتماد الإداري والتحقق مع مسؤول الأجور في الوزارة المعنية.",
        "act_drop":        "التحقيق في العناصر الراتبية غير المعالجة أو الاقتطاعات غير المشروعة أو الأخطاء الحسابية في الأجر لهذه الفترة.",
        "act_pers_raise":  "التأكد من الموافقة الرسمية على ترقية الدرجة أو تعديل الراتب وتوثيقه في نظام الموارد البشرية.",
        "act_pers_drop":   "التحقق مما إذا كان قد تم تطبيق تخفيض أو إجراء تأديبي أو إلغاء مزايا بشكل مرخص.",
        "act_one_spike":   "البحث عن مدفوعات بأثر رجعي أو مكافآت استثنائية أو أخطاء إدخال بيانات خاصة بهذه الفترة.",
        "act_one_drop":    "التحقق من العلاوات المفقودة أو الاقتطاعات الخاطئة أو الدفع الجزئي للشهر.",
        "act_pattern":     "مراجعة سجل الأجور للموظف خلال هذه الفترة والتحقق من الحساب وفق قواعد الأجر المعمول بها.",
        "pc_retro":        "الأرجح أنه تسوية راتب بأثر رجعي: ارتفع الراتب من ~{pre:,.0f} إلى ~{post:,.0f} دينار/شهر (+{raise_amt:,.0f} دينار). يشمل هذا الارتفاع على الأرجح {months} أشهر من الفروقات ({raise_amt:,.0f} × {months} ≈ {total:,.0f} دينار) صُرفت دفعةً واحدة.",
        "pc_retro_large":  "ارتفع الراتب من ~{pre:,.0f} إلى ~{post:,.0f} دينار بعد هذا الشهر، لكن مبلغ الارتفاع ({extra:,.0f} دينار فوق الراتب الجديد) أكبر من أن يفسره التسوية بأثر رجعي وحدها. قد يتضمن أيضاً تعويضاً استثنائياً أو خطأً في إدخال البيانات.",
        "pc_duplicate":    "يُحتمل أنه دفع مكرر: {netpay:,.0f} دينار ≈ {ratio:.1f}× الراتب المعتاد ({pre:,.0f} دينار). لا يوجد تغيير في الراتب خلال الأشهر التالية. ربما صُرف للموظف أجران عن هذه الفترة.",
        "pc_data_error":   "الأرجح أنه خطأ في إدخال البيانات: عاد الأجر إلى {next:,.0f} دينار في الشهر التالي دون أي تغيير دائم، ومعامل z ({z:.1f}) بالغ الارتفاع. على الأرجح أُدخل مبلغ خاطئ في نظام الأجور.",
        "pc_onetime":      "دفعة استثنائية (~{extra:,.0f} دينار فوق الأساس) دون تغيير دائم في الراتب. يُرجَّح أنها تعويض استثنائي أو دفعة ساعات إضافية أو علاوة نهاية السنة خارج الدورة المعتادة.",
        "pc_drop":         "عاد الأجر إلى مستواه الطبيعي في الشهر التالي، مما يشير إلى اقتطاع مؤقت — ربما تحصيل مبالغ مدفوعة زيادةً أو جزاء غياب أو اقتطاع خاطئ تم تصحيحه لاحقاً.",
        "chg_str":         " ({pct:+.1f}% مقارنةً بالشهر السابق)",
    },
}

def _classify_anomaly(row: dict, lang: str = "en") -> dict:
    """Rule-based anomaly diagnosis — type, explanation, probable cause, recommended action."""
    t = _T.get(lang, _T["en"])   # fallback to English

    z       = float(row.get("z_score") or 0)
    pct_dev = float(row.get("pct_deviation") or 0)
    pct_chg = row.get("pct_change_vs_prev")
    prev1   = row.get("pay_prev_1m")
    next1   = row.get("pay_next_1m")
    next2   = row.get("pay_next_2m")
    next3   = row.get("pay_next_3m")
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

    chg_str = t["chg_str"].format(pct=pct_chg) if pct_chg is not None else ""

    # ── Probable cause: data-driven inference from temporal context ──────────
    probable_cause = None

    if is_spike and next1 is not None and next2 is not None:
        n1, n2 = float(next1), float(next2)
        pre_ref = float(prev1) if prev1 is not None else mean
        post_avg = (n1 + n2) / 2

        salary_raised = post_avg > pre_ref * 1.08
        raise_per_month = post_avg - pre_ref if salary_raised else 0
        extra_above_new = netpay - post_avg if salary_raised else netpay - mean
        extra_above_old = netpay - pre_ref

        if salary_raised and raise_per_month > 30:
            months_bp = round(extra_above_new / raise_per_month) if raise_per_month > 0 else 0
            reconstructed = post_avg + months_bp * raise_per_month
            fit_pct = abs(reconstructed - netpay) / netpay * 100
            if 3 <= months_bp <= 36 and fit_pct < 15:
                probable_cause = t["pc_retro"].format(pre=pre_ref, post=post_avg,
                    raise_amt=raise_per_month, months=months_bp, total=raise_per_month * months_bp)
            else:
                probable_cause = t["pc_retro_large"].format(pre=pre_ref, post=post_avg, extra=extra_above_new)
        else:
            ratio = netpay / pre_ref if pre_ref > 0 else 0
            if 1.8 <= ratio <= 2.2:
                probable_cause = t["pc_duplicate"].format(netpay=netpay, ratio=ratio, pre=pre_ref)
            elif abs_z >= 8:
                probable_cause = t["pc_data_error"].format(next=n1, z=z)
            else:
                probable_cause = t["pc_onetime"].format(extra=extra_above_old)

    elif is_drop and next1 is not None and not persistent:
        probable_cause = t["pc_drop"]

    if abs_z >= 3.5 and is_spike:
        atype  = "extreme_spike"
        expl   = t["expl_spike"].format(netpay=netpay, pct=abs_pct, mean=mean, z=z)
        action = t["act_spike"]
    elif abs_z >= 3.5 and is_drop:
        atype  = "extreme_drop"
        expl   = t["expl_drop"].format(netpay=netpay, pct=abs_pct, mean=mean, z=z)
        action = t["act_drop"]
    elif is_spike and persistent:
        atype  = "persistent_raise"
        expl   = t["expl_pers_raise"].format(pct=abs_pct)
        action = t["act_pers_raise"]
    elif is_drop and persistent:
        atype  = "persistent_drop"
        expl   = t["expl_pers_drop"].format(pct=abs_pct)
        action = t["act_pers_drop"]
    elif is_spike:
        atype  = "one_time_spike"
        expl   = t["expl_one_spike"].format(pct=abs_pct, chg=chg_str)
        action = t["act_one_spike"]
    elif is_drop:
        atype  = "one_time_drop"
        expl   = t["expl_one_drop"].format(pct=abs_pct, chg=chg_str)
        action = t["act_one_drop"]
    else:
        atype  = "pattern_anomaly"
        expl   = t["expl_pattern"].format(z=z)
        action = t["act_pattern"]

    return {
        "anomaly_type":       atype,
        "explanation":        expl,
        "probable_cause":     probable_cause,
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
    question:      str
    model:         str = "llama3.2:1b"
    history:       list[dict] = []
    ministry_code: Optional[str] = None  # None = admin (no filter), set = user scope


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
                # Use materialized view — sub-millisecond instead of 9s fact_paie scan
                cur.execute("""
                    SELECT
                        SUM(employee_count)                    AS total_records,
                        MAX(employee_count)                    AS total_employees,
                        ROUND(SUM(total_netpay)::numeric, 2)  AS total_netpay,
                        MIN(year_num)                          AS year_min,
                        MAX(year_num)                          AS year_max
                    FROM dw.mv_payroll_by_month
                    WHERE year_num > 0
                """)
                row = cur.fetchone()
        return {
            "total_payroll_records": int(row[0] or 0),
            "total_employees":       int(row[1] or 0),
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

def _check_disk(run_id: str, stage: str, min_gb: float = 8.0) -> None:
    """Abort the pipeline if free disk space is below min_gb."""
    import shutil as _shutil
    free = _shutil.disk_usage("/" if os.name != "nt" else "C:\\").free / 1024 ** 3
    if free < min_gb:
        msg = (f"DISK FULL ABORT at [{stage}] — only {free:.1f} GB free "
               f"(minimum {min_gb} GB required). Free up space and retry.")
        _prog(run_id, "error", -1, msg)
        raise RuntimeError(msg)
    if free < min_gb + 4:
        _prog(run_id, "etl", -1, f"⚠ Low disk: {free:.1f} GB free", warn=True)


def _run_pipeline_sync(run_id: str, dest: Path, resolved_type: str,
                       clean_dir: Path, reset: bool, retrain: bool,
                       limit: int | None = None,
                       year_min: int | None = None,
                       year_max: int | None = None,
                       full_retrain: bool = False) -> None:
    """Blocking ETL + DW load — runs in a thread pool."""
    try:
        def cb(pct: int, msg: str, **kw):
            _prog(run_id, "etl", pct, msg, **kw)

        year_note = f" · years {year_min}–{year_max}" if year_min else ""
        limit_note = f" · test limit: first {limit:,} rows" if limit else ""
        _prog(run_id, "etl_start", 12, f"Starting ETL pipeline…{year_note}{limit_note}")

        _check_disk(run_id, "etl_start")

        if resolved_type == "paie":
            from etl.pipeline_paie import run as run_paie
            kwargs = dict(source=dest, run_id=run_id, out_dir=clean_dir,
                          progress_cb=cb, limit=limit)
            if year_min is not None: kwargs["year_min"] = year_min
            if year_max is not None: kwargs["year_max"] = year_max
            etl_report = run_paie(**kwargs)
        else:
            from etl.pipeline_indem import run as run_indem
            etl_report = run_indem(source=dest, run_id=run_id, out_dir=clean_dir,
                                   progress_cb=cb, limit=limit)

        _check_disk(run_id, "before_dw_load")

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
            try:
                from ml.model_anomaly import _models_exist, score_incremental, train_anomaly_model
                from ml.model_forecast import train_payroll_forecast
                import traceback as _tb

                if full_retrain or not _models_exist():
                    # Full rebuild — triggered explicitly or first-ever run
                    _prog(run_id, "ml", 96, "Full model retrain (this takes ~30 min)…")
                    from ml.run_all_models import main as run_ml
                    run_ml()
                    ml_status = "full_retrain_ok"
                else:
                    # Incremental: score only new periods, reuse existing model
                    _prog(run_id, "ml", 96, "Scoring new anomalies (incremental)…")
                    n_new = score_incremental()
                    _prog(run_id, "ml", 98, "Updating forecast model…")
                    train_payroll_forecast()
                    ml_status = f"incremental_ok: {n_new} new anomalies scored"
            except Exception as ml_err:
                ml_status = f"failed: {ml_err}"
                log.error("ML retrain failed:\n%s", _tb.format_exc())

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
    retrain: bool            = Query(False, description="Score new anomalies with existing model (incremental)"),
    full_retrain: bool       = Query(False, description="Full model rebuild from scratch (slow, use rarely)"),
    reset: bool              = Query(False),
    limit: Optional[int]     = Query(None, ge=1,
                                     description="Stop after N written rows (test mode)"),
    year_min: Optional[int]  = Query(None, description="Only process records from this year onwards"),
    year_max: Optional[int]  = Query(None, description="Only process records up to this year"),
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
    year_note = f" · years {year_min}–{year_max}" if year_min else ""
    limit_note = f" · test limit: first {limit:,} rows" if limit else ""
    _prog(run_id, "saved", 8, f"File found ({size_mb} MB){year_note}{limit_note} — starting pipeline…")

    loop = asyncio.get_event_loop()
    background_tasks.add_task(
        loop.run_in_executor,
        _pipeline_executor,
        _run_pipeline_sync,
        run_id, source, resolved_type, clean_dir, reset, retrain, limit, year_min, year_max, full_retrain,
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

        # Use MAPE-relative CI: ±MAPE% of predicted value.
        # RMSE-based CI (±1.96*RMSE) produces huge absolute bands on aggregate payroll
        # because RMSE is in the tens-of-millions even at 5% MAPE.
        mape_frac = (winner_m.get("mape") or 5.0) / 100.0

        forecast_with_ci = [
            {
                **p,
                "lower": round(max(p["predicted_netpay"] * (1 - mape_frac), 0), 2),
                "upper": round(p["predicted_netpay"] * (1 + mape_frac), 2),
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
                # Ministries: all top-level ministries from dim_etablissement (natorg='1').
                # This shows all 41 real ministries regardless of whether they have payroll data,
                # which is correct for ministry-scoped users who need to see their dropdown.
                min_cached = _cache_get("dims:ministries")
                if min_cached is not None:
                    ministries = min_cached
                else:
                    cur.execute("""
                        SELECT codetab                               AS code,
                               COALESCE(libletabl, libcetabl, codetab) AS name_fr,
                               COALESCE(libletaba, libcetaba, codetab) AS name_ar
                        FROM dw.dim_etablissement
                        WHERE natorg = '1'
                          AND (codtutel IS NULL OR codtutel = codetab)
                        ORDER BY COALESCE(libletabl, libcetabl, codetab)
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
            codetabs = _resolve_ministry_codetabs(ministry)
            placeholders = ",".join(["%s"] * len(codetabs))
            conditions.append(f"fp.codetab IN ({placeholders})")
            params.extend(codetabs)
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
    lang:      str           = Query("en", description="Language for diagnosis text (en/fr/ar)"),
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

    # Apply optional filters — expand ministry code to all sub-establishments
    df = full_df.copy()
    max_year = int(full_df["year_num"].max())
    if ministry:
        codetabs = _resolve_ministry_codetabs(ministry)
        df = df[df["ministry_code"].isin(codetabs)]
    if year:
        df = df[df["year_num"] == year]
        filter_min_year = year
    else:
        # Default: only show the last 2 years (current year + previous year)
        filter_min_year = max_year - 1
        df = df[df["year_num"] >= filter_min_year]

    # KPI counts from the filtered (recent) dataset
    z_filtered = df["z_score"].abs()
    severity_summary = {
        "high":   int((z_filtered >= 3.5).sum()),
        "medium": int(((z_filtered >= 2.5) & (z_filtered < 3.5)).sum()),
        "low":    int((z_filtered < 2.5).sum()),
    }
    total_filtered = int(len(df))

    # Anomaly rate = anomalies in period / total records in same period
    # Use mv_ministry_details (pre-aggregated) for fast period record count.
    _pr_key = f"period_records:{ministry or ''}:{year or ''}:{filter_min_year}"
    _pr_cached = _cache_get(_pr_key)
    if _pr_cached is not None:
        period_records = _pr_cached
    else:
        try:
            from etl.core.config import DB_CONFIG
            import psycopg2
            with psycopg2.connect(**DB_CONFIG) as _conn:
                with _conn.cursor() as _cur:
                    if ministry:
                        codetabs = _resolve_ministry_codetabs(ministry)
                        placeholders = ",".join(["%s"] * len(codetabs))
                        if year:
                            _cur.execute(
                                f"SELECT COALESCE(SUM(record_count),1) FROM dw.mv_ministry_details "
                                f"WHERE year_num = %s AND codetab IN ({placeholders})",
                                (year, *codetabs)
                            )
                        else:
                            _cur.execute(
                                f"SELECT COALESCE(SUM(record_count),1) FROM dw.mv_ministry_details "
                                f"WHERE year_num >= %s AND codetab IN ({placeholders})",
                                (filter_min_year, *codetabs)
                            )
                    elif year:
                        _cur.execute(
                            "SELECT COALESCE(SUM(record_count),1) FROM dw.mv_ministry_details "
                            "WHERE year_num = %s", (year,)
                        )
                    else:
                        _cur.execute(
                            "SELECT COALESCE(SUM(record_count),1) FROM dw.mv_ministry_details "
                            "WHERE year_num >= %s", (filter_min_year,)
                        )
                    period_records = int(_cur.fetchone()[0])
            _cache_set(_pr_key, period_records)
        except Exception:
            all_years = int(full_df["year_num"].max()) - int(full_df["year_num"].min()) + 1
            filtered_years = 1 if year else 2
            period_records = max(int(total_sampled_records * filtered_years / max(all_years, 1)), 1)

    anomaly_rate_pct = round(total_filtered / period_records * 100, 2)

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

    # Load review statuses — vectorized (no iterrows on 374K rows)
    reviews = _load_reviews()
    # dismissed keys — excluded from the active list
    dismissed_keys = {k for k, v in reviews.items() if isinstance(v, dict) and v.get("dismissed_at")}

    review_stats = {"LEGITIMATE": 0, "ERROR": 0, "INVESTIGATING": 0}
    for k, v in reviews.items():
        if k in dismissed_keys:
            continue  # don't count dismissed in review stats
        s = v["status"] if isinstance(v, dict) else v
        if s in review_stats:
            review_stats[s] += 1

    if not reviews:
        unreviewed = total_filtered
    else:
        active_reviewed_keys = set(reviews.keys()) - dismissed_keys
        keys = set(zip(
            df["employee_sk"].astype(int).tolist(),
            df["year_num"].astype(int).tolist(),
            df["month_num"].astype(int).tolist()
        ))
        unreviewed = total_filtered - len(keys & active_reviewed_keys) - len(keys & dismissed_keys)

    # Enrich each record; skip dismissed anomalies entirely
    records = []
    for rec in json.loads(result_df.to_json(orient="records")):
        key = (int(rec.get("employee_sk", 0)), int(rec.get("year_num", 0)), int(rec.get("month_num", 0)))
        if key in dismissed_keys:
            continue  # hidden until restored or auto-purged
        rec.update(_classify_anomaly(rec, lang=lang))
        rev = reviews.get(key)
        rec["review_status"]  = rev["status"]      if rev else None
        rec["review_notes"]   = rev["notes"]        if rev else None
        rec["reviewed_by"]    = rev["reviewed_by"]  if rev else None
        rec["reviewed_at"]    = rev["reviewed_at"]  if rev else None
        records.append(rec)

    return {
        "total_anomalies_in_report": total_filtered,
        "unreviewed":                unreviewed,
        "review_stats":              review_stats,
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
def get_anomalies_by_ministry(ministry: Optional[str] = None):
    """Aggregated anomaly counts grouped by ministry."""
    import numpy as np
    df = _load_anomaly_report()
    if df is None:
        raise HTTPException(503, "Anomaly report not found.")
    # Restrict to last 2 years by default
    max_year = int(df["year_num"].max())
    df = df[df["year_num"] >= max_year - 1]
    if ministry:
        codetabs = _resolve_ministry_codetabs(ministry)
        df = df[df["ministry_code"].isin(codetabs)]

    # Group by parent ministry code so establishments are merged under their ministry
    group_col = "parent_ministry_code" if "parent_ministry_code" in df.columns else "ministry_code"
    rows = []
    for ministry, g in df.groupby(group_col):
        z_abs = g["z_score"].abs()
        # Prefer parent_ministry_name, then ministry_name_fr, then code
        if "parent_ministry_name" in g.columns and g["parent_ministry_name"].notna().any():
            name = str(g["parent_ministry_name"].dropna().iloc[0])
        elif "ministry_name_fr" in g.columns and g["ministry_name_fr"].notna().any():
            name = str(g["ministry_name_fr"].dropna().iloc[0])
        else:
            name = str(ministry)
        rows.append({
            "ministry_code":   str(ministry),
            "ministry_name":   name,
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
def get_anomalies_by_grade(ministry: Optional[str] = None):
    """Aggregated anomaly counts grouped by grade."""
    import numpy as np
    df = _load_anomaly_report()
    if df is None:
        raise HTTPException(503, "Anomaly report not found.")
    # Restrict to last 2 years by default
    max_year = int(df["year_num"].max())
    df = df[df["year_num"] >= max_year - 1]
    if ministry:
        codetabs = _resolve_ministry_codetabs(ministry)
        df = df[df["ministry_code"].isin(codetabs)]

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

        # Resolve human-readable ministry name for the system prompt
        ministry_name: Optional[str] = None
        if req.ministry_code:
            try:
                import psycopg2
                from etl.core.config import DB_CONFIG
                with psycopg2.connect(**DB_CONFIG) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT COALESCE(libletabl, libcetabl, codetab) "
                            "FROM dw.dim_etablissement WHERE codetab = %s LIMIT 1",
                            (req.ministry_code,)
                        )
                        row = cur.fetchone()
                        if row:
                            ministry_name = row[0]
            except Exception:
                ministry_name = req.ministry_code

        result = chat(
            question=req.question,
            model=req.model,
            history=req.history,
            ministry_code=req.ministry_code or None,
            ministry_name=ministry_name,
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/chat/stream", tags=["RAG Chatbot"])
async def chat_stream_endpoint(req: ChatRequest):
    """Streaming chat — returns SSE token stream from Ollama in real-time."""
    ministry_name: Optional[str] = None
    if req.ministry_code:
        try:
            import psycopg2
            from etl.core.config import DB_CONFIG
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COALESCE(libletabl, libcetabl, codetab) "
                        "FROM dw.dim_etablissement WHERE codetab = %s LIMIT 1",
                        (req.ministry_code,)
                    )
                    row = cur.fetchone()
                    if row:
                        ministry_name = row[0]
        except Exception:
            ministry_name = req.ministry_code

    from api.chatbot import chat_stream

    def generate():
        yield from chat_stream(
            question=req.question,
            model=req.model,
            history=req.history,
            ministry_code=req.ministry_code or None,
            ministry_name=ministry_name,
        )

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no",
                 "Connection": "keep-alive"},
    )


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
