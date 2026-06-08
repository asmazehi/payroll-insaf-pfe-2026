"""
api/chatbot.py  — v3
====================
Ollama-powered, ministry-scoped RAG Chatbot for INSAF Payroll Intelligence.

LLM: Ollama (llama3.2) — local, free, no external dependency.

Data scoping:
  - ministry_code=None  → admin view, all ministries
  - ministry_code=<mc>  → user view, filtered to that ministry + its sub-establishments

Input tolerance:
  - Fuzzy matching corrects typos in keywords and month names before processing.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import get_close_matches
from pathlib import Path
from threading import Lock
from typing import Any, Optional

import psycopg2
from psycopg2 import pool as pg_pool
import requests

# ── LLM config ─────────────────────────────────────────────────────────────────
OLLAMA_BASE  = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = "llama3.2:1b"

_MODELS_DIR = Path(__file__).resolve().parent.parent / "ml" / "models"

# ── Fuzzy input normalizer ──────────────────────────────────────────────────────
# Words the chatbot needs to recognise — typos get corrected to these before processing.
_FUZZY_VOCAB = [
    # months FR/EN/AR-transliterated
    "janvier","février","mars","avril","mai","juin","juillet","août",
    "septembre","octobre","novembre","décembre",
    "january","february","march","april","may","june","july","august",
    "september","october","november","december",
    # payroll concepts
    "grade","grades","salaire","salaires","salary","masse","salariale","payroll",
    "ministère","ministry","ministries","établissement","establishment",
    "anomalie","anomalies","anomaly","détection","detection",
    "prévision","prévisions","forecast","forecasting",
    "employé","employés","employee","employees","agent","agents","effectif",
    "total","moyenne","average","median","médiane","distribution","répartition",
    "tendance","trend","évolution","evolution","croissance","growth",
    "région","regions","region","régional","regional","gouvernorat","governorate",
    "déduction","déductions","deduction","deductions","cotisation","cotisations",
    "indemnité","indemnités","indemnity","indemnities","prime","primes","bonus",
    "mensuel","mensuelle","monthly","annuel","annuelle","annual","yearly",
    "budget","dépenses","expenses","coût","cost",
    "dernier","récent","recent","latest","actuel","current",
    "comparaison","comparison","classement","ranking",
]


def _normalize_question(text: str) -> str:
    """Correct typos in the question by fuzzy-matching each word against known vocabulary."""
    tokens = text.split()
    result = []
    for token in tokens:
        # Strip punctuation for matching, preserve original casing for non-matches
        clean = re.sub(r"[^\w]", "", token.lower())
        # Only attempt correction on words >= 5 chars (avoids false positives on short words)
        if len(clean) >= 5:
            matches = get_close_matches(clean, _FUZZY_VOCAB, n=1, cutoff=0.78)
            result.append(matches[0] if matches else token)
        else:
            result.append(token)
    return " ".join(result)


# ── Ministry filter ─────────────────────────────────────────────────────────────
_MINISTRY_SUBQ = "(SELECT sub_codetab FROM dw.v_ministry_codetabs WHERE ministry_codetab = %s)"


def _mc_sql(alias: str = "fp") -> str:
    """Returns the SQL AND clause for ministry filtering (with one %s placeholder)."""
    return f"AND {alias}.codetab IN {_MINISTRY_SUBQ}"


# ── System prompt ───────────────────────────────────────────────────────────────

def _build_system_prompt(ministry_name: Optional[str] = None) -> str:
    scope = f"Données de **{ministry_name}** uniquement." if ministry_name else "Accès admin — tous ministères."
    return (
        f"Tu es INSAF, analyste paie de la fonction publique tunisienne. {scope}\n"
        "Règles: réponds UNIQUEMENT à partir du DATA CONTEXT. Ne jamais inventer. "
        "Sois ultra-concis (2-3 phrases max). Mets les chiffres clés en gras. "
        "Monnaie: TND. Réponds dans la langue de la question (FR/AR/EN). "
        "Les données sont déjà affichées — ajoute UNIQUEMENT ton analyse/insight, ne répète pas les chiffres."
    )


# ── Entity extractor ────────────────────────────────────────────────────────────

_MONTH_MAP = {
    "january":1,"jan":1,"janvier":1,
    "february":2,"feb":2,"février":2,"fevrier":2,
    "march":3,"mar":3,"mars":3,
    "april":4,"apr":4,"avril":4,
    "may":5,"mai":5,
    "june":6,"jun":6,"juin":6,
    "july":7,"jul":7,"juillet":7,
    "august":8,"aug":8,"août":8,"aout":8,
    "september":9,"sep":9,"septembre":9,
    "october":10,"oct":10,"octobre":10,
    "november":11,"nov":11,"novembre":11,
    "december":12,"dec":12,"décembre":12,"decembre":12,
}


def _extract_entities(q: str) -> dict:
    q       = _normalize_question(q)
    q_lower = q.lower()
    entities: dict[str, Any] = {}

    years = [int(y) for y in re.findall(r'\b(20\d{2})\b', q)]
    if years:
        entities["years"] = sorted(set(years))

    for name, num in _MONTH_MAP.items():
        if re.search(rf'\b{re.escape(name)}\b', q_lower):
            entities.setdefault("months", [])
            if num not in entities["months"]:
                entities["months"].append(num)

    # Ministry code must contain at least one digit (e.g. H00, S00, W00)
    # to avoid matching French words like "par", "les", "des" after "ministère"
    m = re.search(r'\b(?:min(?:istry)?|ministry|ministère|ministr)[_\s\-]*([A-Z][0-9][0-9A-Z]{0,4})\b', q, re.I)
    if m:
        entities["ministry_code"] = m.group(1).upper()

    m = re.search(r'\bgrade[_\s]+([A-Z0-9\-]{1,8})\b', q, re.I)
    if m:
        entities["grade_code"] = m.group(1).upper()

    m = re.search(r'\b(?:employee|emp|agent|employe)[_\s]+(\d{4,9})\b', q, re.I)
    if not m:
        m = re.search(r'\bsk[_\s]*(\d{4,9})\b', q, re.I)
    if m:
        entities["employee_sk"] = int(m.group(1))

    m = re.search(r'\btop[\s\-]+(\d{1,3})\b|\b(\d{1,3})[\s\-]+(?:top|best|highest|biggest|largest)\b', q, re.I)
    if m:
        entities["top_n"] = int(m.group(1) or m.group(2))

    return entities


# ── DB connection pool ──────────────────────────────────────────────────────────

_db_pool: Optional[pg_pool.SimpleConnectionPool] = None
_pool_lock = Lock()


def _get_pool() -> pg_pool.SimpleConnectionPool:
    global _db_pool
    with _pool_lock:
        if _db_pool is None:
            from etl.core.config import DB_CONFIG
            _db_pool = pg_pool.SimpleConnectionPool(1, 6, **DB_CONFIG, connect_timeout=10)
    return _db_pool


# ── Query result cache (5-min TTL) ─────────────────────────────────────────────

_query_cache: dict = {}
_cache_lock = Lock()
_CACHE_TTL = 300


def _query(sql: str, params: tuple = (), limit: int = 8) -> list[dict]:
    pool = _get_pool()
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchmany(limit)
            return [dict(zip(cols, row)) for row in rows]
    finally:
        try:
            pool.putconn(conn)
        except Exception:
            pass


def _cached_query(sql: str, params: tuple = (), limit: int = 8) -> list[dict]:
    key = hashlib.md5(f"{sql}|{params}|{limit}".encode()).hexdigest()
    with _cache_lock:
        entry = _query_cache.get(key)
        if entry and time.time() - entry[0] < _CACHE_TTL:
            return entry[1]
    result = _query(sql, params, limit)
    with _cache_lock:
        _query_cache[key] = (time.time(), result)
    return result


def _fmt_row(row: dict) -> str:
    parts = []
    for k, v in row.items():
        if isinstance(v, float):
            parts.append(f"{k}: {v:,.2f}")
        elif isinstance(v, int) and v > 1000:
            parts.append(f"{k}: {v:,}")
        else:
            parts.append(f"{k}: {v}")
    return " | ".join(parts)


def _fmt_rows(rows: list[dict], title: str) -> str:
    if not rows:
        return f"[{title}: no data found]"
    lines = [f"**{title}** ({len(rows)} rows):"]
    for i, r in enumerate(rows, 1):
        lines.append(f"  {i}. {_fmt_row(r)}")
    return "\n".join(lines)


# ── Intent handlers (all accept optional ministry_code) ─────────────────────────

def _intent_total_payroll(e: dict, mc: Optional[str] = None) -> str:
    year_f = "AND year_num = ANY(%s)" if e.get("years") else ""
    if mc:
        params: list = [mc] + ([e["years"]] if e.get("years") else [])
        sql = f"""
            SELECT year_num, month_num,
                   SUM(employee_count)                    AS employees,
                   ROUND(SUM(total_netpay)::numeric, 0)   AS total_netpay,
                   ROUND(AVG(avg_netpay)::numeric, 0)     AS avg_netpay
            FROM dw.mv_ministry_details
            WHERE codetab IN {_MINISTRY_SUBQ} {year_f}
            GROUP BY year_num, month_num
            ORDER BY year_num DESC, month_num DESC
            LIMIT 24
        """
    else:
        params = [e["years"]] if e.get("years") else []
        sql = f"""
            SELECT year_num, month_num,
                   employee_count AS employees,
                   ROUND(total_netpay::numeric, 0) AS total_netpay,
                   ROUND(avg_netpay::numeric, 0)   AS avg_netpay,
                   total_deductions, total_cps, total_cpe
            FROM dw.mv_payroll_by_month
            WHERE year_num > 0 {year_f}
            ORDER BY year_num DESC, month_num DESC
            LIMIT 24
        """
    return _fmt_rows(_cached_query(sql, tuple(params)), "Monthly Payroll")


def _intent_yearly_summary(e: dict, mc: Optional[str] = None) -> str:
    if mc:
        params: tuple = (mc,)
        sql = f"""
            SELECT year_num,
                   ROUND(SUM(total_netpay)::numeric, 0)   AS total_netpay,
                   MAX(employee_count)                     AS employees,
                   ROUND(AVG(avg_netpay)::numeric, 0)     AS avg_netpay
            FROM dw.mv_ministry_details
            WHERE codetab IN {_MINISTRY_SUBQ}
            GROUP BY year_num
            ORDER BY year_num DESC
            LIMIT 10
        """
    else:
        params = ()
        sql = """
            SELECT year_num,
                   ROUND(SUM(total_netpay)::numeric, 0)   AS total_netpay,
                   MAX(employee_count)                     AS employees,
                   ROUND(AVG(avg_netpay)::numeric, 0)     AS avg_netpay
            FROM dw.mv_payroll_by_month
            WHERE year_num > 0
            GROUP BY year_num
            ORDER BY year_num DESC
            LIMIT 10
        """
    return _fmt_rows(_cached_query(sql, params), "Annual Payroll Summary")


def _intent_ministry_breakdown(e: dict, mc: Optional[str] = None) -> str:
    top_n = e.get("top_n", 15)
    if mc:
        params: tuple = (mc,)
        sql = f"""
            SELECT codetab,
                   ROUND(SUM(total_netpay)::numeric, 0)   AS total_netpay,
                   MAX(employee_count)                     AS employees,
                   ROUND(AVG(avg_netpay)::numeric, 0)     AS avg_netpay
            FROM dw.mv_ministry_details
            WHERE codetab IN {_MINISTRY_SUBQ}
            GROUP BY codetab
            ORDER BY total_netpay DESC
            LIMIT {top_n}
        """
        return _fmt_rows(_cached_query(sql, params), "Establishment Breakdown")
    # Admin: use pre-aggregated ministry view (instant)
    sql_a = f"""
        SELECT ministry_code, ministry_name,
               employee_count, ROUND(total_netpay::numeric, 0) AS total_netpay,
               ROUND(avg_netpay::numeric, 0) AS avg_netpay
        FROM dw.mv_payroll_by_ministry
        ORDER BY total_netpay DESC
        LIMIT {top_n}
    """
    return _fmt_rows(_cached_query(sql_a, ()), "Ministry Payroll Breakdown")


def _intent_grade_breakdown(e: dict, mc: Optional[str] = None) -> str:
    grade_f = "AND grade_code = %s" if e.get("grade_code") else ""
    if mc:
        params: list = [mc] + ([e["grade_code"]] if e.get("grade_code") else [])
        sql = f"""
            SELECT grade_code, grade_label_fr, category,
                   SUM(employee_count)                    AS employees,
                   ROUND(AVG(avg_netpay)::numeric, 0)    AS avg_netpay,
                   ROUND(SUM(total_netpay)::numeric, 0)  AS total_netpay
            FROM dw.mv_grade_by_ministry
            WHERE codetab IN {_MINISTRY_SUBQ} {grade_f}
            GROUP BY grade_code, grade_label_fr, category
            ORDER BY avg_netpay DESC
            LIMIT 20
        """
    else:
        params = [e["grade_code"]] if e.get("grade_code") else []
        where  = f"WHERE {grade_f.replace('AND ', '')}" if grade_f else ""
        sql = f"""
            SELECT grade_code, grade_label_fr, category,
                   employee_count,
                   ROUND(avg_netpay::numeric, 0)   AS avg_netpay,
                   ROUND(total_netpay::numeric, 0) AS total_netpay
            FROM dw.mv_grade_distribution
            {where}
            ORDER BY avg_netpay DESC
            LIMIT 20
        """
    return _fmt_rows(_cached_query(sql, tuple(params)), "Salary by Grade")


def _intent_employee_count(e: dict, mc: Optional[str] = None) -> str:
    if mc:
        params: tuple = (mc,)
        sql = f"""
            SELECT year_num,
                   MAX(employee_count) AS active_employees,
                   COUNT(DISTINCT month_num) AS months_covered
            FROM dw.mv_ministry_details
            WHERE codetab IN {_MINISTRY_SUBQ}
            GROUP BY year_num
            ORDER BY year_num DESC
            LIMIT 8
        """
    else:
        params = ()
        sql = """
            SELECT year_num,
                   MAX(employee_count) AS active_employees
            FROM dw.mv_payroll_by_month
            WHERE year_num > 0
            GROUP BY year_num
            ORDER BY year_num DESC
            LIMIT 8
        """
    return _fmt_rows(_cached_query(sql, params), "Employee Count by Year")


def _intent_employee_profile(e: dict, mc: Optional[str] = None) -> str:
    if not e.get("employee_sk"):
        return "[Employee profile: no employee ID detected in your question]"
    sql = """
        SELECT dt.year_num, dt.month_num,
               dg.grade_code, dg.grade_label_fr,
               do2.liborgl AS ministry,
               fp.m_netpay, fp.m_salbrut, fp.m_retrait, fp.m_cps
        FROM dw.fact_paie fp
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        JOIN dw.dim_grade dg ON dg.grade_sk = fp.grade_sk
        LEFT JOIN dw.dim_organisme do2 ON do2.organisme_sk = fp.organisme_sk
        WHERE fp.employee_sk = %s
        ORDER BY dt.year_num DESC, dt.month_num DESC
        LIMIT 24
    """
    rows = _cached_query(sql, (e["employee_sk"],))
    if not rows:
        return f"[Employee {e['employee_sk']}: no payroll records found]"
    return _fmt_rows(rows, f"Payroll history for employee {e['employee_sk']}")


def _intent_avg_salary(e: dict, mc: Optional[str] = None) -> str:
    # mv_grade_distribution / mv_grade_by_ministry: instant, pre-aggregated
    grade_f = "AND grade_code = %s" if e.get("grade_code") else ""
    if mc:
        params: list = [mc] + ([e["grade_code"]] if e.get("grade_code") else [])
        sql = f"""
            SELECT grade_code, grade_label_fr, category,
                   SUM(employee_count)                   AS employees,
                   ROUND(AVG(avg_netpay)::numeric, 0)   AS avg_netpay,
                   ROUND(SUM(total_netpay)::numeric, 0) AS total_netpay
            FROM dw.mv_grade_by_ministry
            WHERE codetab IN {_MINISTRY_SUBQ} {grade_f}
            GROUP BY grade_code, grade_label_fr, category
            ORDER BY avg_netpay DESC
            LIMIT 20
        """
    else:
        params = [e["grade_code"]] if e.get("grade_code") else []
        where  = f"WHERE {grade_f.replace('AND ', '')}" if grade_f else ""
        sql = f"""
            SELECT grade_code, grade_label_fr, category,
                   employee_count,
                   ROUND(avg_netpay::numeric, 0)   AS avg_netpay,
                   ROUND(total_netpay::numeric, 0) AS total_netpay
            FROM dw.mv_grade_distribution
            {where}
            ORDER BY avg_netpay DESC
            LIMIT 20
        """
    return _fmt_rows(_cached_query(sql, tuple(params)), "Average Salary by Grade")


def _intent_salary_distribution(e: dict, mc: Optional[str] = None) -> str:
    # Approximate salary distribution using grade avg_netpay bucketed — sub-millisecond from MV
    if mc:
        params: list = [mc]
        sql = f"""
            SELECT
                CASE
                    WHEN avg_netpay < 500  THEN '0-500 TND'
                    WHEN avg_netpay < 1000 THEN '500-1 000 TND'
                    WHEN avg_netpay < 1500 THEN '1 000-1 500 TND'
                    WHEN avg_netpay < 2000 THEN '1 500-2 000 TND'
                    WHEN avg_netpay < 3000 THEN '2 000-3 000 TND'
                    WHEN avg_netpay < 5000 THEN '3 000-5 000 TND'
                    ELSE '5 000+ TND'
                END AS salary_range,
                SUM(employee_count) AS employees,
                COUNT(*) AS nb_grades,
                ROUND(AVG(avg_netpay)::numeric, 0) AS avg_in_range
            FROM dw.mv_grade_by_ministry
            WHERE codetab IN {_MINISTRY_SUBQ}
            GROUP BY 1 ORDER BY MIN(avg_netpay)
        """
    else:
        params = []
        sql = """
            SELECT
                CASE
                    WHEN avg_netpay < 500  THEN '0-500 TND'
                    WHEN avg_netpay < 1000 THEN '500-1 000 TND'
                    WHEN avg_netpay < 1500 THEN '1 000-1 500 TND'
                    WHEN avg_netpay < 2000 THEN '1 500-2 000 TND'
                    WHEN avg_netpay < 3000 THEN '2 000-3 000 TND'
                    WHEN avg_netpay < 5000 THEN '3 000-5 000 TND'
                    ELSE '5 000+ TND'
                END AS salary_range,
                SUM(employee_count) AS employees,
                COUNT(*) AS nb_grades,
                ROUND(AVG(avg_netpay)::numeric, 0) AS avg_in_range
            FROM dw.mv_grade_distribution
            GROUP BY 1 ORDER BY MIN(avg_netpay)
        """
    return _fmt_rows(_cached_query(sql, tuple(params)), "Salary Distribution (by grade avg)")


def _intent_trends(e: dict, mc: Optional[str] = None) -> str:
    if mc:
        params: tuple = (mc,)
        sql = f"""
            SELECT year_num,
                   ROUND(SUM(total_netpay)::numeric, 0) AS total_netpay,
                   MAX(employee_count)                   AS employees,
                   ROUND(AVG(avg_netpay)::numeric, 0)   AS avg_netpay
            FROM dw.mv_ministry_details
            WHERE codetab IN {_MINISTRY_SUBQ}
            GROUP BY year_num ORDER BY year_num
        """
    else:
        params = ()
        sql = """
            SELECT year_num,
                   ROUND(SUM(total_netpay)::numeric, 0) AS total_netpay,
                   MAX(employee_count)                   AS employees,
                   ROUND(AVG(avg_netpay)::numeric, 0)   AS avg_netpay
            FROM dw.mv_payroll_by_month
            WHERE year_num > 0
            GROUP BY year_num ORDER BY year_num
        """
    rows = _cached_query(sql, params)
    for i, r in enumerate(rows):
        if i > 0:
            prev = float(rows[i-1].get("total_netpay") or 0)
            curr = float(r.get("total_netpay") or 0)
            if prev > 0:
                r["yoy_pct"] = round((curr - prev) / prev * 100, 2)
    return _fmt_rows(rows, "Year-over-Year Payroll Trend")


def _intent_indemnities(e: dict, mc: Optional[str] = None) -> str:
    # mv_indem_by_month: year_num, month_num, employee_count, total_indemnity, avg_indemnity
    year_f = "AND year_num = ANY(%s)" if e.get("years") else ""
    params: list = [e["years"]] if e.get("years") else []
    sql = f"""
        SELECT year_num,
               SUM(employee_count)                    AS employees,
               ROUND(SUM(total_indemnity)::numeric, 0) AS total_indemnity,
               ROUND(AVG(avg_indemnity)::numeric, 0)   AS avg_indemnity
        FROM dw.mv_indem_by_month
        WHERE year_num > 0 {year_f}
        GROUP BY year_num
        ORDER BY year_num DESC
        LIMIT 8
    """
    return _fmt_rows(_cached_query(sql, tuple(params)), "Indemnity/Allowance by Year")


def _intent_regional(e: dict, mc: Optional[str] = None) -> str:
    # No regional MV — use a fast approximation from mv_ministry_details (codetab has region info)
    # For now return a note and the ministry breakdown as proxy
    return _intent_ministry_breakdown(e, mc=mc)


def _intent_recent_month(e: dict, mc: Optional[str] = None) -> str:
    if mc:
        params: tuple = (mc,)
        sql = f"""
            SELECT year_num, month_num,
                   SUM(employee_count)                   AS employees,
                   ROUND(SUM(total_netpay)::numeric, 0)  AS total_netpay,
                   ROUND(AVG(avg_netpay)::numeric, 0)    AS avg_netpay,
                   ROUND(SUM(total_retrait)::numeric, 0) AS total_deductions
            FROM dw.mv_ministry_details
            WHERE codetab IN {_MINISTRY_SUBQ}
            GROUP BY year_num, month_num
            ORDER BY year_num DESC, month_num DESC
            LIMIT 3
        """
    else:
        params = ()
        sql = """
            SELECT year_num, month_num,
                   employee_count                         AS employees,
                   ROUND(total_netpay::numeric, 0)        AS total_netpay,
                   ROUND(avg_netpay::numeric, 0)          AS avg_netpay,
                   ROUND(total_deductions::numeric, 0)    AS total_deductions,
                   ROUND(total_cps::numeric, 0)           AS total_cps
            FROM dw.mv_payroll_by_month
            WHERE year_num > 0
            ORDER BY year_num DESC, month_num DESC
            LIMIT 3
        """
    return _fmt_rows(_cached_query(sql, params), "Most Recent Months")


def _intent_deductions(e: dict, mc: Optional[str] = None) -> str:
    year_f = "AND year_num = ANY(%s)" if e.get("years") else ""
    if mc:
        params: list = [mc] + ([e["years"]] if e.get("years") else [])
        sql = f"""
            SELECT year_num,
                   ROUND(SUM(total_retrait)::numeric, 0) AS total_deductions,
                   ROUND(AVG(total_retrait)::numeric, 0) AS avg_deductions_per_month
            FROM dw.mv_ministry_details
            WHERE codetab IN {_MINISTRY_SUBQ} {year_f}
            GROUP BY year_num ORDER BY year_num DESC LIMIT 5
        """
    else:
        params = [e["years"]] if e.get("years") else []
        sql = f"""
            SELECT year_num,
                   ROUND(SUM(total_deductions)::numeric, 0) AS total_deductions,
                   ROUND(SUM(total_cps)::numeric, 0)        AS total_cps,
                   ROUND(SUM(total_cpe)::numeric, 0)        AS total_cpe
            FROM dw.mv_payroll_by_month
            WHERE year_num > 0 {year_f}
            GROUP BY year_num ORDER BY year_num DESC LIMIT 5
        """
    return _fmt_rows(_cached_query(sql, tuple(params)), "Deductions Analysis")


def _intent_anomalies(e: dict, mc: Optional[str] = None) -> str:
    path = _MODELS_DIR / "anomaly_results.json"
    if not path.exists():
        return "[Anomaly results not found — model not yet trained]"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        winner = data.get("winner", "if")
        mc_key  = data.get("model_comparison", {})
        wm     = mc_key.get(winner, {})
        meta   = data.get("_meta", {}).get("final_flag", {})

        lines = [
            f"**Anomaly Detection Summary** (model: {data.get('model','?')}, winner: {winner.upper()})",
            f"- Total records analyzed: {data.get('total_records', '?'):,}",
            f"- Anomalies flagged: {wm.get('n_flagged', meta.get('n_flagged', '?')):,}",
            f"- Anomaly rate: {wm.get('rate_pct', meta.get('anomaly_rate', '?'))}%",
            f"- Z-score threshold: {data.get('zscore_threshold', 3.0)}",
        ]
        if mc:
            lines.append(f"\n*Note: detailed anomaly breakdown by ministry is available on the Anomalies page.*")

        sev = data.get("severity_summary") or (meta.get("severity_summary") if meta else None)
        if sev:
            lines += ["", "**Severity breakdown:**",
                      f"  - High (|z|≥3.5): {sev.get('high',0):,}",
                      f"  - Medium (|z|≥2.5): {sev.get('medium',0):,}",
                      f"  - Low: {sev.get('low',0):,}"]
        return "\n".join(lines)
    except Exception as exc:
        return f"[Anomaly data parse error: {exc}]"


def _intent_forecast(e: dict, mc: Optional[str] = None) -> str:
    path = _MODELS_DIR / "payroll_forecast_results.json"
    if not path.exists():
        return "[Forecast results not found — model not yet trained]"
    try:
        data     = json.loads(path.read_text(encoding="utf-8"))
        winner   = data.get("winner", "sarima")
        model_cmp = data.get("model_comparison", {})
        wm       = model_cmp.get(winner, {})
        forecasts = data.get("forecast_6m", [])

        lines = [
            f"**Payroll Forecast** (best model: {winner.upper()}, MAPE: {wm.get('mape','?')}%)",
        ]
        if mc:
            lines.append("*National forecast — ministry-level forecasting is available on the Forecast page.*")
        lines += ["", "**6-month national forecast (total net pay):**"]
        for f in forecasts:
            lines.append(f"  - {f['date']}: {f['predicted_netpay']:,.0f} TND")
        lines += ["", "**Model comparison (MAPE %):**"]
        for model, s in model_cmp.items():
            if s.get("mape", 0) > 0:
                lines.append(f"  - {model.upper()}: MAPE={s['mape']:.2f}%")
        return "\n".join(lines)
    except Exception as exc:
        return f"[Forecast data parse error: {exc}]"


def _intent_general_stats(e: dict, mc: Optional[str] = None) -> str:
    if mc:
        params: tuple = (mc,)
        sql = f"""
            SELECT MIN(year_num) AS first_year, MAX(year_num) AS last_year,
                   MAX(employee_count)                         AS total_employees,
                   ROUND(SUM(total_netpay)::numeric, 0)       AS total_netpay_all_time,
                   ROUND(AVG(avg_netpay)::numeric, 0)         AS avg_netpay
            FROM dw.mv_ministry_details
            WHERE codetab IN {_MINISTRY_SUBQ}
        """
    else:
        params = ()
        sql = """
            SELECT MIN(year_num) AS first_year, MAX(year_num) AS last_year,
                   MAX(employee_count)                         AS total_employees,
                   ROUND(SUM(total_netpay)::numeric, 0)       AS total_netpay_all_time,
                   ROUND(AVG(avg_netpay)::numeric, 0)         AS avg_netpay
            FROM dw.mv_payroll_by_month
            WHERE year_num > 0
        """
    return _fmt_rows(_cached_query(sql, params), "Payroll Statistics")


# ── Intent registry ─────────────────────────────────────────────────────────────

_INTENT_REGISTRY = [
    ([r"anomal|fraud|irregular|suspicious|unusual|flag|detect"],                                                      _intent_anomalies,          "anomalies"),
    ([r"forecast|predict|next month|future|projection|prévision"],                                                    _intent_forecast,           "forecast"),
    ([r"how many employ|number of employ|employee count|workforce|headcount|effectif|combien.*employ|employ.*combien|nombre.*employ|nombre.*agent|combien.*agent"], _intent_employee_count, "employee_count"),
    ([r"employee[_\s]+\d{4,9}|agent[_\s]+\d{4,9}|profile.*employee|employee.*profile"],                             _intent_employee_profile,   "employee_profile"),
    ([r"ministr|ministere|organisme|department|etablissement|establishment"],                                        _intent_ministry_breakdown, "ministry"),
    ([r"grade|échelon|echelon|categor|cadre"],                                                                       _intent_grade_breakdown,    "grade"),
    ([r"indemn|allowance|bonus|supplement|prime|allocation"],                                                        _intent_indemnities,        "indemnities"),
    ([r"region|governorate|gouvernorat|location|geographic|géograph"],                                               _intent_regional,           "regional"),
    ([r"deduct|retrait|cotis|cps|cpe|withhold|prélèv"],                                                             _intent_deductions,         "deductions"),
    ([r"average|avg|mean|median|typical|distribution|range|répartition|salaire moyen|moyenne.*salaire"],             _intent_avg_salary,         "avg_salary"),
    ([r"distribut|range|bracket|tranche|histogram|bucket"],                                                          _intent_salary_distribution,"distribution"),
    ([r"trend|growth|increas|decreas|evolut|over.*year|year.*over|progression|évolution|croissance"],                _intent_trends,             "trends"),
    ([r"total.*pay|budget|payroll.*total|masse salariale|total.*salary|bilan annuel|résumé annuel"],                 _intent_yearly_summary,     "yearly"),
    ([r"last month|recent|latest|current|this month|dernier|récent|dernier mois|mois.*dernier"],                    _intent_recent_month,       "recent"),
    ([r"monthly|by month|per month|mensuel|par mois|évolution mensuelle"],                                           _intent_total_payroll,      "monthly"),
]


# ── Fast Python answers (no Ollama needed for simple factual queries) ───────────

_MONTHS_FR = {1:"jan",2:"fév",3:"mar",4:"avr",5:"mai",6:"juin",
              7:"juil",8:"août",9:"sep",10:"oct",11:"nov",12:"déc"}

# All intents use the fast path — Ollama (30-50s on CPU) is never worth the wait
_FAST_INTENTS = {
    "employee_count", "general", "recent", "yearly", "monthly",
    "forecast", "anomalies", "distribution", "avg_salary", "grade",
    "trends", "ministry", "regional", "indemnities", "deductions",
    "employee_profile", "conversational",
}


def _parse_context_values(context: str) -> dict[str, list]:
    """Extract key: value pairs from _fmt_rows output into a dict of lists."""
    kv: dict[str, list] = {}
    for row_text in re.finditer(r'\d+\.\s*(.+)', context):
        for m in re.finditer(r'(\w+):\s*([\d,.]+)', row_text.group(1)):
            k, raw = m.group(1), m.group(2).replace(",", "")
            try:
                val: Any = float(raw) if "." in raw else int(raw)
            except ValueError:
                val = raw
            kv.setdefault(k, []).append(val)
    return kv


def _instant_answer(intent_name: str, context: str,
                    ministry_name: Optional[str] = None) -> Optional[str]:
    """Return a crisp, data-driven answer for simple intents — zero LLM wait."""
    if intent_name not in _FAST_INTENTS:
        return None
    # Forecast and anomalies are already well-formatted strings — return as-is
    if intent_name in ("forecast", "anomalies"):
        return context

    scope = f" pour **{ministry_name}**" if ministry_name else ""
    kv = _parse_context_values(context)

    def first(key: str, default=None):
        vals = kv.get(key, [])
        return vals[0] if vals else default

    def fmt_num(v) -> str:
        if v is None:
            return "?"
        try:
            return f"{float(v):,.0f}"
        except Exception:
            return str(v)

    if intent_name == "employee_count":
        emp = first("active_employees") or first("total_employees")
        yr  = first("year_num")
        if emp:
            yr_str = f" en {int(yr)}" if yr else ""
            return f"**{fmt_num(emp)} agents actifs**{yr_str}{scope}."

    if intent_name == "general":
        emp   = first("total_employees")
        total = first("total_netpay_all_time")
        avg   = first("avg_netpay")
        yr1   = first("first_year")
        yr2   = first("last_year")
        parts = []
        if emp:
            parts.append(f"**{fmt_num(emp)} employés** au total{scope}")
        if total:
            parts.append(f"masse salariale cumulée **{fmt_num(total)} TND**")
        if avg:
            parts.append(f"salaire net moyen **{fmt_num(avg)} TND**")
        if yr1 and yr2:
            parts.append(f"données de {int(yr1)} à {int(yr2)}")
        return " · ".join(parts) + "." if parts else None

    if intent_name == "recent":
        yr  = first("year_num")
        mn  = first("month_num")
        emp = first("employees")
        avg = first("avg_netpay")
        tot = first("total_netpay")
        if emp and avg:
            mo = _MONTHS_FR.get(int(mn), str(mn)) if mn else "?"
            yr_s = str(int(yr)) if yr else "?"
            scope_s = f"{scope}" if scope else ""
            return (
                f"En {mo} {yr_s}{scope_s} : **{fmt_num(emp)} agents**, "
                f"salaire moyen **{fmt_num(avg)} TND**"
                + (f", masse salariale **{fmt_num(tot)} TND**." if tot else ".")
            )

    if intent_name == "yearly":
        yr    = first("year_num")
        total = first("total_netpay")
        emp   = first("employees")
        if total and yr:
            return (
                f"En {int(yr)}{scope} : masse salariale **{fmt_num(total)} TND**"
                + (f" pour **{fmt_num(emp)} agents**." if emp else ".")
            )

    if intent_name == "monthly":
        yr  = first("year_num")
        mn  = first("month_num")
        tot = first("total_netpay")
        emp = first("employees")
        if tot and yr:
            mo = _MONTHS_FR.get(int(mn), str(mn)) if mn else "?"
            return (
                f"En {mo} {int(yr) if yr else '?'}{scope} : **{fmt_num(tot)} TND**"
                + (f" · {fmt_num(emp)} agents." if emp else ".")
            )

    # Generic fallback: return the formatted DB data directly — no LLM needed
    if context and not context.startswith("["):
        return context

    return None


# ── Intent name → handler map ────────────────────────────────────────────────────
_INTENT_MAP = {
    "anomaly":           _intent_anomalies,
    "forecast":          _intent_forecast,
    "employee_count":    _intent_employee_count,
    "employee_profile":  _intent_employee_profile,
    "ministry":          _intent_ministry_breakdown,
    "grade":             _intent_grade_breakdown,
    "indemnity":         _intent_indemnities,
    "regional":          _intent_regional,
    "deduction":         _intent_deductions,
    "avg_salary":        _intent_avg_salary,
    "distribution":      _intent_salary_distribution,
    "trend":             _intent_trends,
    "yearly":            _intent_yearly_summary,
    "recent":            _intent_recent_month,
    "monthly":           _intent_total_payroll,
    "general":           _intent_general_stats,
}

_EXTRACTION_PROMPT = """\
You are a query parser for a payroll analytics system. Extract structured data from the user's question.
Return ONLY valid JSON — no explanation, no markdown, no extra text.

Intent options (pick 1-2 most relevant):
anomaly, forecast, employee_count, employee_profile, ministry, grade, indemnity,
regional, deduction, avg_salary, distribution, trend, yearly, recent, monthly, general

JSON schema (use null for missing fields):
{{
  "intents": ["<intent1>", "<intent2 or omit>"],
  "years": [<year numbers>],
  "months": [<month numbers 1-12>],
  "grade_code": "<code or null>",
  "ministry_code": "<code or null>",
  "employee_sk": <integer or null>,
  "top_n": <integer or null>
}}

Question: {question}

JSON:"""


def _llm_extract(question: str, model: str = OLLAMA_MODEL) -> dict | None:
    """Use Ollama to extract intent + entities from natural language (handles typos/context)."""
    try:
        prompt = _EXTRACTION_PROMPT.format(question=question)
        r = requests.post(
            f"{OLLAMA_BASE}/api/generate",
            json={"model": model, "prompt": prompt, "stream": False,
                  "options": {"temperature": 0.0, "num_predict": 200}},
            timeout=30,
        )
        r.raise_for_status()
        raw = r.json().get("response", "").strip()
        # Extract JSON block in case model adds extra text
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        if m:
            return json.loads(m.group())
    except Exception:
        pass
    return None


_CONVERSATIONAL = re.compile(
    r"^\s*(hello|hi|hey|bonjour|salam|salut|مرحبا|أهلا|how are|ça va|comment vas|merci|thanks|thank you|ok|okay|oui|non|yes|no)\b",
    re.I
)


def _detect_and_retrieve(question: str, entities: dict,
                         ministry_code: Optional[str] = None,
                         model: str = OLLAMA_MODEL) -> tuple[str, str]:
    """Returns (context_string, top_intent_name)."""

    if _CONVERSATIONAL.match(question) and len(question.strip()) < 40:
        return "", "conversational"

    q = _normalize_question(question).lower()
    # Strip accents so patterns like "ministr" match "ministère" after normalization
    q_ascii = unicodedata.normalize('NFD', q).encode('ascii', 'ignore').decode('ascii')
    scored = [
        (sum(1 for p in patterns if re.search(p, q_ascii)), fn, name)
        for patterns, fn, name in _INTENT_REGISTRY
    ]
    scored.sort(key=lambda x: -x[0])

    top = [(fn, name) for score, fn, name in scored[:2] if score > 0]
    top_intent = top[0][1] if top else "general"

    if top:
        results: list[str] = []
        if len(top) == 1:
            fn, name = top[0]
            try:
                r = fn(entities, mc=ministry_code)
                if r and not r.startswith("["):
                    results.append(r)
            except Exception as exc:
                results.append(f"[{name} error: {exc}]")
        else:
            with ThreadPoolExecutor(max_workers=2) as ex:
                future_map = {ex.submit(fn, entities, ministry_code): name for fn, name in top}
                for fut in as_completed(future_map, timeout=10):
                    name = future_map[fut]
                    try:
                        r = fut.result()
                        if r and not r.startswith("["):
                            results.append(r)
                    except Exception as exc:
                        results.append(f"[{name} error: {exc}]")
        if results:
            return "\n\n".join(results), top_intent

    try:
        return _intent_general_stats(entities, mc=ministry_code), "general"
    except Exception as exc:
        return f"[General stats error: {exc}]", "general"


# ── LLM backends ────────────────────────────────────────────────────────────────



def _ollama_chat(context: str, question: str, history: list[dict],
                 system_prompt: str, model: str = OLLAMA_MODEL) -> str:
    hist_lines = []
    for turn in history[-2:]:
        role = "User" if turn.get("role") == "user" else "Assistant"
        hist_lines.append(f"{role}: {str(turn.get('text',''))[:200]}")
    hist_str = ("\nPrevious conversation:\n" + "\n".join(hist_lines)) if hist_lines else ""

    prompt = _build_prompt(system_prompt, hist_str, context, question)
    r = requests.post(
        f"{OLLAMA_BASE}/api/generate",
        json={"model": model, "prompt": prompt, "stream": False,
              "options": {"temperature": 0.1, "num_predict": 140, "num_ctx": 1024}},
        timeout=90,
    )
    r.raise_for_status()
    return r.json().get("response", "").strip()


def _build_prompt(system_prompt: str, hist_str: str, context: str, question: str) -> str:
    return (
        f"{system_prompt}\n\n"
        f"{hist_str}\n\n"
        f"DATA CONTEXT:\n{context}\n\n"
        f"USER QUESTION: {question}\n\n"
        f"ANSWER:"
    )


def chat_stream(question: str, model: str = OLLAMA_MODEL,
                history: list[dict] | None = None,
                ministry_code: Optional[str] = None,
                ministry_name: Optional[str] = None):
    """Generator that yields SSE-formatted token strings from Ollama."""
    import json as _json
    history = history or []

    # Instant reply for greetings — no DB, no LLM
    if _CONVERSATIONAL.match(question.strip()) and len(question.strip()) < 40:
        scope = f" Données de **{ministry_name}**." if ministry_name else ""
        msg = f"Bonjour ! Comment puis-je vous aider ?{scope}"
        yield f"data: {_json.dumps({'token': msg, 'done': True, 'entities': {}})}\n\n"
        return

    entities = _extract_entities(question)
    try:
        context, top_intent = _detect_and_retrieve(question, entities, ministry_code=ministry_code, model=model)
    except Exception as e:
        yield f"data: {_json.dumps({'token': '⚠️ Données indisponibles. Réessayez dans un moment.', 'done': True, 'entities': entities})}\n\n"
        return

    if context.startswith("[") and "error" in context.lower():
        yield f"data: {_json.dumps({'token': '⚠️ Les données ne sont pas disponibles. Réessayez ou reformulez votre question.', 'done': True, 'entities': entities})}\n\n"
        return

    # ── Emit data preview immediately (replaces typing indicator after DB query) ──
    if context and not context.startswith("["):
        yield f"data: {_json.dumps({'token': '', 'done': False, 'preview': context, 'entities': entities})}\n\n"

    # ── Fast path: Python-generated answer — skip Ollama entirely ───────────────
    fast = _instant_answer(top_intent, context, ministry_name=ministry_name)
    if fast:
        yield f"data: {_json.dumps({'token': fast, 'done': False})}\n\n"
        yield f"data: {_json.dumps({'token': '', 'done': True, 'entities': entities})}\n\n"
        return

    # ── Ollama path: used only for complex analytical questions ─────────────────
    if not _ollama_available(model):
        scope_note = f" (filtré: {ministry_name or ministry_code})" if ministry_code else ""
        fallback = f"⚠️ Ollama hors ligne{scope_note}.\n\n{context}\n\n*Lancez Ollama pour l'analyse IA.*"
        yield f"data: {_json.dumps({'token': fallback, 'done': True, 'entities': entities})}\n\n"
        return

    system_prompt = _build_system_prompt(ministry_name)
    hist_lines = []
    for turn in history[-2:]:
        role = "User" if turn.get("role") == "user" else "Assistant"
        hist_lines.append(f"{role}: {str(turn.get('text',''))[:150]}")
    hist_str = ("\nConversation précédente:\n" + "\n".join(hist_lines)) if hist_lines else ""
    prompt = _build_prompt(system_prompt, hist_str, context, question)

    try:
        r = requests.post(
            f"{OLLAMA_BASE}/api/generate",
            json={"model": model, "prompt": prompt, "stream": True,
                  "options": {"temperature": 0.1, "num_predict": 140, "num_ctx": 1024}},
            stream=True, timeout=90,
        )
        r.raise_for_status()
        for line in r.iter_lines():
            if line:
                data = _json.loads(line)
                token = data.get("response", "")
                done  = data.get("done", False)
                if token:
                    yield f"data: {_json.dumps({'token': token, 'done': False})}\n\n"
                if done:
                    yield f"data: {_json.dumps({'token': '', 'done': True, 'entities': entities})}\n\n"
                    return
    except Exception as e:
        yield f"data: {_json.dumps({'token': f'Erreur LLM: {e}', 'done': True, 'entities': entities})}\n\n"


_ollama_cache: dict = {}  # {model: (ts, bool)}

def _ollama_available(model: str = OLLAMA_MODEL) -> bool:
    """Check Ollama availability, cached for 60s to avoid an HTTP call on every request."""
    entry = _ollama_cache.get(model)
    if entry and time.time() - entry[0] < 60:
        return entry[1]
    try:
        r = requests.get(f"{OLLAMA_BASE}/api/tags", timeout=2)
        result = r.status_code == 200 and model.split(":")[0] in [
            m["name"].split(":")[0] for m in r.json().get("models", [])
        ]
    except Exception:
        result = False
    _ollama_cache[model] = (time.time(), result)
    return result


# ── Public API ───────────────────────────────────────────────────────────────────

def chat(question: str, model: str = OLLAMA_MODEL,
         history: list[dict] | None = None,
         ministry_code: Optional[str] = None,
         ministry_name: Optional[str] = None) -> dict[str, Any]:
    """
    Main RAG chat function.

    Args:
        question:      User's question
        model:         Ollama model name (used as fallback)
        history:       Last N conversation turns [{role, text}]
        ministry_code: If set, all SQL queries are scoped to this ministry
        ministry_name: Human-readable ministry name for the system prompt
    """
    history = history or []

    # ── Instant reply for pure greetings — no LLM, no DB ────────────────────────
    if _CONVERSATIONAL.match(question.strip()) and len(question.strip()) < 40:
        scope = f" Je suis configuré pour les données de **{ministry_name}**." if ministry_name else ""
        return {
            "question":      question,
            "answer":        f"Bonjour ! Comment puis-je vous aider ?{scope}\n\nPosez-moi une question sur les salaires, les grades, les tendances ou les anomalies.",
            "context":       "",
            "model":         "instant",
            "llm_used":      False,
            "entities":      {},
            "ministry_code": ministry_code,
        }

    entities = _extract_entities(question)

    try:
        context, top_intent = _detect_and_retrieve(question, entities, ministry_code=ministry_code, model=model)
    except Exception as e:
        return {
            "question": question, "answer": "⚠️ Impossible de récupérer les données. Réessayez dans un moment.",
            "context": "", "model": "error", "llm_used": False,
            "entities": entities, "ministry_code": ministry_code,
        }

    if context.startswith("[") and "error" in context.lower():
        return {
            "question": question, "answer": "⚠️ Les données ne sont pas disponibles pour le moment. Réessayez ou reformulez votre question.",
            "context": context, "model": "error", "llm_used": False,
            "entities": entities, "ministry_code": ministry_code,
        }

    # Fast path: Python-generated answer, no Ollama wait
    fast = _instant_answer(top_intent, context, ministry_name=ministry_name)
    if fast:
        return {
            "question": question, "answer": fast, "context": context,
            "model": "instant", "llm_used": False,
            "entities": entities, "ministry_code": ministry_code,
        }

    system_prompt = _build_system_prompt(ministry_name)
    llm_used = False
    llm_name = None
    answer   = ""

    if _ollama_available(model):
        try:
            answer   = _ollama_chat(context, question, history, system_prompt, model=model)
            llm_used = True
            llm_name = f"ollama ({model})"
        except Exception as e:
            answer = f"LLM error: {e}\n\n{context}"

    if not answer:
        scope_note = f" (filtré: {ministry_name or ministry_code})" if ministry_code else ""
        answer = (
            f"⚠️ **Ollama hors ligne**{scope_note}\n\n{context}\n\n"
            f"*Lancez Ollama pour activer l'analyse IA.*"
        )

    return {
        "question":      question,
        "answer":        answer,
        "context":       context,
        "model":         llm_name or model,
        "llm_used":      llm_used,
        "entities":      entities,
        "ministry_code": ministry_code,
    }
