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

import json
import re
from difflib import get_close_matches
from pathlib import Path
from typing import Any, Optional

import psycopg2
import requests

# ── LLM config ─────────────────────────────────────────────────────────────────
OLLAMA_BASE  = "http://localhost:11434"
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
    scope = (
        f"You are analyzing data for **{ministry_name}** only — all figures are scoped to this ministry."
        if ministry_name else
        "You are an admin with full access to all ministries."
    )
    return (
        f"You are INSAF, a sharp and friendly payroll analyst for Tunisia's civil service platform. {scope}\n\n"
        "Rules:\n"
        "- Answer ONLY from the DATA CONTEXT provided. Never invent numbers.\n"
        "- Be concise and direct — lead with the key finding, skip filler.\n"
        "- Bold important numbers. Use bullet points for lists.\n"
        "- All money in TND. Format large numbers: 1,234,567.\n"
        "- Reply in the same language as the question (FR/AR/EN).\n"
        "- If data is missing, say so in one sentence and suggest what to ask instead.\n"
        "- Be friendly and professional — like a knowledgeable colleague, not a robot."
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

    m = re.search(r'\b(?:min(?:istry)?|ministry|ministère|ministr)[_\s\-]*([0-9A-Z]{2,6})\b', q, re.I)
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


# ── DB helpers ──────────────────────────────────────────────────────────────────

def _get_db_conn():
    from etl.core.config import DB_CONFIG
    return psycopg2.connect(**DB_CONFIG, connect_timeout=10)


def _query(sql: str, params: tuple = (), limit: int = 30) -> list[dict]:
    with _get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchmany(limit)
            return [dict(zip(cols, row)) for row in rows]


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
    year_f   = "AND dt.year_num = ANY(%s)" if e.get("years") else ""
    min_f    = _mc_sql() if mc else ""
    params: list = []
    if mc:
        params.append(mc)
    if e.get("years"):
        params.append(e["years"])
    sql = f"""
        SELECT dt.year_num, dt.month_num,
               SUM(fp.m_netpay)               AS total_netpay,
               COUNT(DISTINCT fp.employee_sk) AS employees,
               AVG(fp.m_netpay)               AS avg_netpay
        FROM dw.fact_paie fp
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND dt.year_num > 0 {min_f} {year_f}
        GROUP BY dt.year_num, dt.month_num
        ORDER BY dt.year_num DESC, dt.month_num DESC
        LIMIT 24
    """
    return _fmt_rows(_query(sql, tuple(params)), "Monthly Payroll Totals")


def _intent_yearly_summary(e: dict, mc: Optional[str] = None) -> str:
    min_f  = _mc_sql() if mc else ""
    params = (mc,) if mc else ()
    sql = f"""
        SELECT dt.year_num,
               SUM(fp.m_netpay)               AS total_netpay,
               COUNT(DISTINCT fp.employee_sk) AS employees,
               AVG(fp.m_netpay)               AS avg_per_employee_month,
               COUNT(*)                       AS total_records
        FROM dw.fact_paie fp
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND dt.year_num > 0 {min_f}
        GROUP BY dt.year_num
        ORDER BY dt.year_num DESC
        LIMIT 10
    """
    return _fmt_rows(_query(sql, params), "Annual Payroll Summary")


def _intent_ministry_breakdown(e: dict, mc: Optional[str] = None) -> str:
    year_f = "AND dt.year_num = ANY(%s)" if e.get("years") else ""
    top_n  = e.get("top_n", 15)

    if mc:
        # User view: show sub-establishment breakdown within their ministry
        params: tuple = (mc,) + ((e["years"],) if e.get("years") else ())
        sql = f"""
            SELECT md.codetab,
                   COALESCE(de.libletabl, de.libcetabl, md.codetab) AS name,
                   SUM(md.total_netpay)  AS total_netpay,
                   SUM(md.employee_count) AS employees,
                   AVG(md.avg_netpay)   AS avg_netpay
            FROM dw.mv_ministry_details md
            LEFT JOIN dw.dim_etablissement de ON de.codetab = md.codetab
            WHERE md.codetab IN {_MINISTRY_SUBQ} {year_f}
            GROUP BY md.codetab, de.libletabl, de.libcetabl
            ORDER BY total_netpay DESC
            LIMIT {top_n}
        """
        return _fmt_rows(_query(sql, params), "Establishment Breakdown (your ministry)")

    # Admin view: cross-ministry
    params_a: tuple = (e["years"],) if e.get("years") else ()
    sql_a = f"""
        SELECT do2.codetab AS ministry_code, do2.liborgl AS ministry_name,
               dt.year_num,
               SUM(fp.m_netpay)               AS total_netpay,
               COUNT(DISTINCT fp.employee_sk) AS employees,
               AVG(fp.m_netpay)               AS avg_netpay
        FROM dw.fact_paie fp
        JOIN dw.dim_organisme do2 ON do2.organisme_sk = fp.organisme_sk
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND fp.organisme_sk <> 0 AND dt.year_num > 0 {year_f}
        GROUP BY do2.codetab, do2.liborgl, dt.year_num
        ORDER BY dt.year_num DESC, total_netpay DESC
        LIMIT {top_n * 3}
    """
    return _fmt_rows(_query(sql_a, params_a), "Ministry Payroll Breakdown")


def _intent_grade_breakdown(e: dict, mc: Optional[str] = None) -> str:
    year_f  = "AND dt.year_num = ANY(%s)" if e.get("years") else ""
    grade_f = "AND dg.grade_code = %s"    if e.get("grade_code") else ""
    min_f   = _mc_sql() if mc else ""

    params: list = []
    if mc:           params.append(mc)
    if e.get("years"):      params.append(e["years"])
    if e.get("grade_code"): params.append(e["grade_code"])

    sql = f"""
        SELECT dg.grade_code, dg.grade_label_fr,
               COUNT(DISTINCT fp.employee_sk)  AS employees,
               AVG(fp.m_netpay)                AS avg_netpay,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fp.m_netpay) AS median_netpay,
               MIN(fp.m_netpay) AS min_netpay,
               MAX(fp.m_netpay) AS max_netpay
        FROM dw.fact_paie fp
        JOIN dw.dim_grade dg ON dg.grade_sk = fp.grade_sk
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND dg.grade_sk <> 0
          AND dt.year_num > 0 {min_f} {year_f} {grade_f}
        GROUP BY dg.grade_code, dg.grade_label_fr
        ORDER BY avg_netpay DESC
        LIMIT 20
    """
    return _fmt_rows(_query(sql, tuple(params)), "Salary by Grade")


def _intent_employee_count(e: dict, mc: Optional[str] = None) -> str:
    min_f  = _mc_sql() if mc else ""
    params = (mc,) if mc else ()
    sql = f"""
        SELECT dt.year_num,
               COUNT(DISTINCT fp.employee_sk) AS active_employees,
               COUNT(*) AS total_payroll_records,
               ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT fp.employee_sk),0), 1) AS avg_months_per_employee
        FROM dw.fact_paie fp
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND dt.year_num > 0 {min_f}
        GROUP BY dt.year_num
        ORDER BY dt.year_num DESC
        LIMIT 8
    """
    return _fmt_rows(_query(sql, params), "Employee Count by Year")


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
    rows = _query(sql, (e["employee_sk"],))
    if not rows:
        return f"[Employee {e['employee_sk']}: no payroll records found]"
    return _fmt_rows(rows, f"Payroll history for employee {e['employee_sk']}")


def _intent_avg_salary(e: dict, mc: Optional[str] = None) -> str:
    year_f = "AND dt.year_num = ANY(%s)" if e.get("years") else ""
    min_f  = _mc_sql() if mc else ""
    params: list = []
    if mc:           params.append(mc)
    if e.get("years"): params.append(e["years"])
    sql = f"""
        SELECT dg.grade_code, dg.grade_label_fr, dg.category,
               COUNT(DISTINCT fp.employee_sk)              AS employees,
               ROUND(AVG(fp.m_netpay)::numeric, 0)         AS avg_netpay,
               ROUND(MIN(fp.m_netpay)::numeric, 0)         AS min_netpay,
               ROUND(MAX(fp.m_netpay)::numeric, 0)         AS max_netpay
        FROM dw.fact_paie fp
        JOIN dw.dim_grade dg ON dg.grade_sk = fp.grade_sk
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND dg.grade_sk <> 0
          AND dt.year_num > 0 {min_f} {year_f}
        GROUP BY dg.grade_code, dg.grade_label_fr, dg.category
        ORDER BY avg_netpay DESC
        LIMIT 20
    """
    return _fmt_rows(_query(sql, tuple(params)), "Average Salary by Grade")


def _intent_salary_distribution(e: dict, mc: Optional[str] = None) -> str:
    year_f = ("AND dt.year_num = ANY(%s)" if e.get("years")
              else "AND dt.year_num = (SELECT MAX(year_num) FROM dw.dim_temps WHERE year_num > 0)")
    min_f  = _mc_sql() if mc else ""
    params: list = []
    if mc:           params.append(mc)
    if e.get("years"): params.append(e["years"])
    sql = f"""
        SELECT
            CASE
                WHEN fp.m_netpay < 500   THEN '0–500 TND'
                WHEN fp.m_netpay < 1000  THEN '500–1,000 TND'
                WHEN fp.m_netpay < 1500  THEN '1,000–1,500 TND'
                WHEN fp.m_netpay < 2000  THEN '1,500–2,000 TND'
                WHEN fp.m_netpay < 3000  THEN '2,000–3,000 TND'
                WHEN fp.m_netpay < 5000  THEN '3,000–5,000 TND'
                ELSE '5,000+ TND'
            END AS salary_range,
            COUNT(*)                        AS records,
            COUNT(DISTINCT fp.employee_sk)  AS employees,
            ROUND(AVG(fp.m_netpay)::numeric, 0) AS avg_in_range
        FROM dw.fact_paie fp
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND dt.year_num > 0 {min_f} {year_f}
        GROUP BY 1
        ORDER BY MIN(fp.m_netpay)
    """
    return _fmt_rows(_query(sql, tuple(params)), "Salary Distribution")


def _intent_trends(e: dict, mc: Optional[str] = None) -> str:
    min_f  = _mc_sql() if mc else ""
    params = (mc,) if mc else ()
    sql = f"""
        SELECT dt.year_num,
               SUM(fp.m_netpay)               AS total_netpay,
               COUNT(DISTINCT fp.employee_sk) AS employees,
               ROUND(AVG(fp.m_netpay)::numeric, 0) AS avg_netpay,
               LAG(SUM(fp.m_netpay)) OVER (ORDER BY dt.year_num) AS prev_year_netpay
        FROM dw.fact_paie fp
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND dt.year_num > 0 {min_f}
        GROUP BY dt.year_num
        ORDER BY dt.year_num
    """
    rows = _query(sql, params)
    for r in rows:
        if r.get("prev_year_netpay") and r["prev_year_netpay"] > 0:
            r["yoy_growth_pct"] = round(
                (r["total_netpay"] - r["prev_year_netpay"]) / r["prev_year_netpay"] * 100, 2)
    return _fmt_rows(rows, "Year-over-Year Payroll Trend")


def _intent_indemnities(e: dict, mc: Optional[str] = None) -> str:
    year_f = "AND dt.year_num = ANY(%s)" if e.get("years") else ""
    min_f  = ""
    params: list = []
    if mc:
        # fact_indem uses organisme_sk; filter via dim_organisme.codetab
        min_f = "AND dorg.codetab IN (SELECT sub_codetab FROM dw.v_ministry_codetabs WHERE ministry_codetab = %s)"
        params.append(mc)
    if e.get("years"):
        params.append(e["years"])
    join_org = "JOIN dw.dim_organisme dorg ON dorg.organisme_sk = fi.organisme_sk" if mc else ""
    sql = f"""
        SELECT di.indemnite_code,
               di.indemnite_label_fr AS indemnite_label,
               di.nature_flag,
               COUNT(DISTINCT fi.employee_sk) AS employees,
               SUM(fi.m_netpay)         AS total_amount,
               ROUND(AVG(fi.m_netpay)::numeric, 0) AS avg_amount
        FROM dw.fact_indem fi
        JOIN dw.dim_indemnite di ON di.indemnite_sk = fi.indemnite_sk
        JOIN dw.dim_temps dt ON dt.time_sk = fi.time_sk
        {join_org}
        WHERE fi.employee_sk <> 0 AND di.indemnite_sk <> 0
          AND dt.year_num > 0 {min_f} {year_f}
        GROUP BY di.indemnite_code, di.indemnite_label_fr, di.nature_flag
        ORDER BY total_amount DESC
        LIMIT 15
    """
    return _fmt_rows(_query(sql, tuple(params)), "Indemnity/Allowance Breakdown")


def _intent_regional(e: dict, mc: Optional[str] = None) -> str:
    year_f = "AND dt.year_num = ANY(%s)" if e.get("years") else ""
    min_f  = _mc_sql() if mc else ""
    params: list = []
    if mc:           params.append(mc)
    if e.get("years"): params.append(e["years"])
    sql = f"""
        SELECT dr.lib_reg AS region, dr.codreg,
               COUNT(DISTINCT fp.employee_sk) AS employees,
               SUM(fp.m_netpay)               AS total_netpay,
               ROUND(AVG(fp.m_netpay)::numeric, 0) AS avg_netpay
        FROM dw.fact_paie fp
        JOIN dw.dim_region dr ON dr.region_sk = fp.region_sk
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND fp.region_sk <> 0
          AND dt.year_num > 0 {min_f} {year_f}
        GROUP BY dr.lib_reg, dr.codreg
        ORDER BY total_netpay DESC
        LIMIT 20
    """
    return _fmt_rows(_query(sql, tuple(params)), "Regional Payroll Breakdown")


def _intent_recent_month(e: dict, mc: Optional[str] = None) -> str:
    min_f  = _mc_sql() if mc else ""
    params = (mc,) if mc else ()
    sql = f"""
        SELECT dt.year_num, dt.month_num,
               COUNT(DISTINCT fp.employee_sk)       AS employees,
               SUM(fp.m_netpay)                     AS total_netpay,
               ROUND(AVG(fp.m_netpay)::numeric, 0)  AS avg_netpay,
               ROUND(AVG(fp.m_salbrut)::numeric, 0) AS avg_gross,
               ROUND(AVG(fp.m_retrait)::numeric, 0) AS avg_deductions
        FROM dw.fact_paie fp
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND dt.year_num > 0 {min_f}
        GROUP BY dt.year_num, dt.month_num
        ORDER BY dt.year_num DESC, dt.month_num DESC
        LIMIT 3
    """
    return _fmt_rows(_query(sql, params), "Most Recent Months")


def _intent_deductions(e: dict, mc: Optional[str] = None) -> str:
    year_f = "AND dt.year_num = ANY(%s)" if e.get("years") else ""
    min_f  = _mc_sql() if mc else ""
    params: list = []
    if mc:           params.append(mc)
    if e.get("years"): params.append(e["years"])
    sql = f"""
        SELECT dt.year_num,
               ROUND(AVG(fp.m_retrait)::numeric, 0) AS avg_deductions,
               ROUND(AVG(fp.m_cps)::numeric, 0)     AS avg_cps,
               ROUND(AVG(fp.m_cpe)::numeric, 0)     AS avg_cpe,
               ROUND(SUM(fp.m_retrait)::numeric, 0)  AS total_deductions,
               ROUND(AVG(fp.m_retrait / NULLIF(fp.m_salbrut,0) * 100)::numeric, 2) AS avg_deduction_pct
        FROM dw.fact_paie fp
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND dt.year_num > 0 {min_f} {year_f}
        GROUP BY dt.year_num
        ORDER BY dt.year_num DESC
        LIMIT 5
    """
    return _fmt_rows(_query(sql, tuple(params)), "Deductions Analysis")


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
    min_f  = _mc_sql() if mc else ""
    params = (mc,) if mc else ()
    sql = f"""
        SELECT COUNT(*)                        AS total_records,
               COUNT(DISTINCT fp.employee_sk)  AS total_employees,
               ROUND(SUM(fp.m_netpay)::numeric, 0)  AS total_netpay_all_time,
               ROUND(AVG(fp.m_netpay)::numeric, 0)  AS avg_netpay,
               ROUND(MIN(fp.m_netpay)::numeric, 0)  AS min_netpay,
               ROUND(MAX(fp.m_netpay)::numeric, 0)  AS max_netpay,
               MIN(dt.year_num) AS first_year,
               MAX(dt.year_num) AS last_year
        FROM dw.fact_paie fp
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND dt.year_num > 0 {min_f}
    """
    return _fmt_rows(_query(sql, params), "Payroll Statistics")


# ── Intent registry ─────────────────────────────────────────────────────────────

_INTENT_REGISTRY = [
    ([r"anomal|fraud|irregular|suspicious|unusual|flag|detect"],          _intent_anomalies,         "anomalies"),
    ([r"forecast|predict|next month|future|projection|prévision"],        _intent_forecast,          "forecast"),
    ([r"how many employee|number of employee|employee count|workforce|headcount|effectif"], _intent_employee_count, "employee_count"),
    ([r"employee[_\s]+\d{4,9}|agent[_\s]+\d{4,9}|profile.*employee|employee.*profile"],   _intent_employee_profile, "employee_profile"),
    ([r"ministr|organisme|department|établissement|establishment"],        _intent_ministry_breakdown, "ministry"),
    ([r"grade|échelon|echelon|categor|cadre"],                            _intent_grade_breakdown,   "grade"),
    ([r"indemn|allowance|bonus|supplement|prime|allocation"],             _intent_indemnities,       "indemnities"),
    ([r"region|governorate|gouvernorat|location|geographic|géograph"],    _intent_regional,          "regional"),
    ([r"deduct|retrait|cotis|cps|cpe|withhold|prélèv"],                  _intent_deductions,        "deductions"),
    ([r"average|avg|mean|median|typical|distribution|range|répartition"], _intent_avg_salary,        "avg_salary"),
    ([r"distribut|range|bracket|tranche|histogram|bucket"],              _intent_salary_distribution,"distribution"),
    ([r"trend|growth|increas|decreas|evolut|over.*year|year.*over|progression|évolution"], _intent_trends, "trends"),
    ([r"total.*pay|budget|payroll.*total|masse salariale|total.*salary"],  _intent_yearly_summary,   "yearly"),
    ([r"last month|recent|latest|current|this month|dernier|récent"],     _intent_recent_month,     "recent"),
    ([r"monthly|by month|per month|mensuel|mois"],                        _intent_total_payroll,    "monthly"),
]


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
                         model: str = OLLAMA_MODEL) -> str:

    # ── Conversational shortcut: no DB needed ────────────────────────────────────
    if _CONVERSATIONAL.match(question) and len(question.strip()) < 40:
        return ""  # LLM will handle it from context alone

    # ── Step 1: regex scoring (instant, no Ollama needed) ───────────────────────
    q = _normalize_question(question).lower()
    scored = [
        (sum(1 for p in patterns if re.search(p, q)), fn, name)
        for patterns, fn, name in _INTENT_REGISTRY
    ]
    scored.sort(key=lambda x: -x[0])

    if scored[0][0] > 0:
        # Regex found a match — use it directly (fast path)
        results = []
        for score, fn, name in scored[:2]:
            if score == 0:
                break
            try:
                result = fn(entities, mc=ministry_code)
                if result and not result.startswith("["):
                    results.append(result)
            except Exception as exc:
                results.append(f"[{name} error: {exc}]")
        if results:
            return "\n\n".join(results)

    # ── Step 2: LLM extraction (only when regex finds nothing) ──────────────────
    if _ollama_available(model):
        llm_parsed = _llm_extract(question, model=model)
        if llm_parsed:
            merged = dict(entities)
            for key in ("years", "months", "grade_code", "ministry_code", "employee_sk", "top_n"):
                if llm_parsed.get(key):
                    merged[key] = llm_parsed[key]

            results = []
            for intent_name in llm_parsed.get("intents", [])[:2]:
                fn = _INTENT_MAP.get(intent_name)
                if fn:
                    try:
                        result = fn(merged, mc=ministry_code)
                        if result and not result.startswith("["):
                            results.append(result)
                    except Exception as exc:
                        results.append(f"[{intent_name} error: {exc}]")
            if results:
                return "\n\n".join(results)

    # ── Final fallback: general stats ────────────────────────────────────────────
    try:
        return _intent_general_stats(entities, mc=ministry_code)
    except Exception as exc:
        return f"[General stats error: {exc}]"


# ── LLM backends ────────────────────────────────────────────────────────────────



def _ollama_chat(context: str, question: str, history: list[dict],
                 system_prompt: str, model: str = OLLAMA_MODEL) -> str:
    hist_lines = []
    for turn in history[-4:]:
        role = "User" if turn.get("role") == "user" else "Assistant"
        hist_lines.append(f"{role}: {str(turn.get('text',''))[:300]}")
    hist_str = ("\nPrevious conversation:\n" + "\n".join(hist_lines)) if hist_lines else ""

    prompt = _build_prompt(system_prompt, hist_str, context, question)
    r = requests.post(
        f"{OLLAMA_BASE}/api/generate",
        json={"model": model, "prompt": prompt, "stream": False,
              "options": {"temperature": 0.1, "num_predict": 350, "num_ctx": 2048}},
        timeout=120,
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

    # Instant reply for greetings
    if _CONVERSATIONAL.match(question.strip()) and len(question.strip()) < 40:
        scope = f" Données de **{ministry_name}**." if ministry_name else ""
        msg = f"Bonjour ! Comment puis-je vous aider ?{scope}"
        yield f"data: {_json.dumps({'token': msg, 'done': True, 'entities': {}})}\n\n"
        return

    entities = _extract_entities(question)
    try:
        context = _detect_and_retrieve(question, entities, ministry_code=ministry_code, model=model)
    except Exception as e:
        yield f"data: {_json.dumps({'token': '⚠️ Données indisponibles. Réessayez dans un moment.', 'done': True, 'entities': entities})}\n\n"
        return

    if context.startswith("[") and "error" in context.lower():
        yield f"data: {_json.dumps({'token': '⚠️ Les données ne sont pas disponibles. Réessayez ou reformulez votre question.', 'done': True, 'entities': entities})}\n\n"
        return

    system_prompt = _build_system_prompt(ministry_name)
    hist_lines = []
    for turn in history[-4:]:
        role = "User" if turn.get("role") == "user" else "Assistant"
        hist_lines.append(f"{role}: {str(turn.get('text',''))[:300]}")
    hist_str = ("\nPrevious conversation:\n" + "\n".join(hist_lines)) if hist_lines else ""
    prompt = _build_prompt(system_prompt, hist_str, context, question)

    if not _ollama_available(model):
        yield f"data: {_json.dumps({'token': '⚠️ Ollama not running.', 'done': True, 'entities': entities})}\n\n"
        return

    try:
        r = requests.post(
            f"{OLLAMA_BASE}/api/generate",
            json={"model": model, "prompt": prompt, "stream": True,
                  "options": {"temperature": 0.1, "num_predict": 350, "num_ctx": 2048}},
            stream=True, timeout=120,
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
        yield f"data: {_json.dumps({'token': f'Error: {e}', 'done': True, 'entities': entities})}\n\n"


def _ollama_available(model: str = OLLAMA_MODEL) -> bool:
    try:
        r = requests.get(f"{OLLAMA_BASE}/api/tags", timeout=3)
        if r.status_code != 200:
            return False
        names = [m["name"].split(":")[0] for m in r.json().get("models", [])]
        return model.split(":")[0] in names
    except Exception:
        return False


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

    # Retrieve data context (ministry-scoped if applicable)
    try:
        context = _detect_and_retrieve(question, entities, ministry_code=ministry_code, model=model)
    except Exception as e:
        return {
            "question": question, "answer": "⚠️ Impossible de récupérer les données. Réessayez dans un moment.",
            "context": "", "model": "error", "llm_used": False,
            "entities": entities, "ministry_code": ministry_code,
        }

    # If context is an error, return clean message without calling LLM
    if context.startswith("[") and "error" in context.lower():
        return {
            "question": question, "answer": "⚠️ Les données ne sont pas disponibles pour le moment. Réessayez ou reformulez votre question.",
            "context": context, "model": "error", "llm_used": False,
            "entities": entities, "ministry_code": ministry_code,
        }

    system_prompt = _build_system_prompt(ministry_name)
    llm_used  = False
    llm_name  = None
    answer    = ""

    # Try Ollama
    if _ollama_available(model):
        try:
            answer   = _ollama_chat(context, question, history, system_prompt, model=model)
            llm_used = True
            llm_name = f"ollama ({model})"
        except Exception as e:
            answer = f"LLM error: {e}\n\n{context}"

    # Raw context fallback
    if not answer:
        scope_note = f" (filtered to: {ministry_name or ministry_code})" if ministry_code else ""
        answer = (
            f"⚠️ **Ollama not running** — showing raw data{scope_note}\n\n"
            f"{context}\n\n"
            f"*Start Ollama and run `ollama pull {model}` to enable AI answers.*"
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
