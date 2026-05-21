"""
api/chatbot.py  — v2
====================
Advanced RAG Chatbot for INSAF Payroll Intelligence.

Improvements over v1:
  - Entity extraction: year, month, ministry, grade, employee_sk, top_n
  - Dynamic SQL with WHERE filters derived from the question
  - 16 intent handlers covering all DW tables + ML results
  - Multi-intent: top-2 scoring intents are combined
  - Conversation history passed to the LLM
  - Forecast + anomaly JSON files integrated
  - Structured, readable context formatting
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import psycopg2
import requests

OLLAMA_BASE   = "http://localhost:11434"
DEFAULT_MODEL = "llama3.2"

_MODELS_DIR = Path(__file__).resolve().parent.parent / "ml" / "models"

# ── System prompt ──────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are INSAF Assistant, an expert AI analyst for the Tunisian Government Payroll Intelligence Platform (INSAF).
You have access to real payroll data retrieved live from a PostgreSQL data warehouse covering all Tunisian civil servants.

Rules:
1. Answer ONLY from the provided DATA CONTEXT. Never invent numbers.
2. Use TND (Tunisian Dinar) for all monetary values.
3. Format answers clearly: use bullet points, bold key figures, and line breaks for readability.
4. If the context contains data, extract and present the key insights — don't just repeat raw rows.
5. If no relevant data is in the context, say so and suggest what the user could ask instead.
6. Be concise but complete. Lead with the most important finding.
7. For trends, highlight direction (increase/decrease) and magnitude.
8. For comparisons, always state what is being compared."""


# ── Entity extractor ───────────────────────────────────────────────────────────

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
    q_lower = q.lower()
    entities: dict[str, Any] = {}

    # Years (4-digit starting with 20)
    years = [int(y) for y in re.findall(r'\b(20\d{2})\b', q)]
    if years:
        entities["years"] = sorted(set(years))

    # Named months
    for name, num in _MONTH_MAP.items():
        if re.search(rf'\b{re.escape(name)}\b', q_lower):
            entities.setdefault("months", [])
            if num not in entities["months"]:
                entities["months"].append(num)

    # Ministry code pattern e.g. "MIN001", "ministry 10", "codetab 123"
    m = re.search(r'\b(?:min(?:istry)?|ministry|ministère|ministr)[_\s\-]*([0-9A-Z]{2,6})\b', q, re.I)
    if m:
        entities["ministry_code"] = m.group(1).upper()

    # Grade code e.g. "grade A1", "grade 102"
    m = re.search(r'\bgrade[_\s]+([A-Z0-9\-]{1,8})\b', q, re.I)
    if m:
        entities["grade_code"] = m.group(1).upper()

    # Employee SK (numeric after "employee", "emp", "agent")
    m = re.search(r'\b(?:employee|emp|agent|employe)[_\s]+(\d{4,9})\b', q, re.I)
    if not m:
        m = re.search(r'\bsk[_\s]*(\d{4,9})\b', q, re.I)
    if m:
        entities["employee_sk"] = int(m.group(1))

    # Top N
    m = re.search(r'\btop[\s\-]+(\d{1,3})\b|\b(\d{1,3})[\s\-]+(?:top|best|highest|biggest|largest)\b', q, re.I)
    if m:
        entities["top_n"] = int(m.group(1) or m.group(2))

    return entities


# ── DB helpers ─────────────────────────────────────────────────────────────────

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


# ── Intent handlers ────────────────────────────────────────────────────────────

def _intent_total_payroll(e: dict) -> str:
    year_filter = "AND dt.year_num = ANY(%s)" if e.get("years") else ""
    params = (e["years"],) if e.get("years") else ()
    sql = f"""
        SELECT dt.year_num, dt.month_num,
               SUM(fp.m_netpay)                AS total_netpay,
               SUM(fp.m_salbrut)               AS total_gross,
               COUNT(DISTINCT fp.employee_sk)  AS employees,
               AVG(fp.m_netpay)                AS avg_netpay
        FROM dw.fact_paie fp
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND dt.year_num > 0 {year_filter}
        GROUP BY dt.year_num, dt.month_num
        ORDER BY dt.year_num DESC, dt.month_num DESC
        LIMIT 24
    """
    return _fmt_rows(_query(sql, params), "Monthly Payroll Totals")


def _intent_yearly_summary(e: dict) -> str:
    sql = """
        SELECT dt.year_num,
               SUM(fp.m_netpay)                AS total_netpay,
               SUM(fp.m_salbrut)               AS total_gross,
               COUNT(DISTINCT fp.employee_sk)  AS employees,
               AVG(fp.m_netpay)                AS avg_per_employee_month,
               COUNT(*)                        AS total_records
        FROM dw.fact_paie fp
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND dt.year_num > 0
        GROUP BY dt.year_num
        ORDER BY dt.year_num DESC
        LIMIT 10
    """
    return _fmt_rows(_query(sql), "Annual Payroll Summary")


def _intent_ministry_breakdown(e: dict) -> str:
    year_filter = "AND dt.year_num = ANY(%s)" if e.get("years") else ""
    params = (e["years"],) if e.get("years") else ()
    top_n = e.get("top_n", 15)
    sql = f"""
        SELECT do2.codetab                      AS ministry_code,
               do2.liborgl                     AS ministry_name,
               dt.year_num,
               SUM(fp.m_netpay)                AS total_netpay,
               COUNT(DISTINCT fp.employee_sk)  AS employees,
               AVG(fp.m_netpay)                AS avg_netpay
        FROM dw.fact_paie fp
        JOIN dw.dim_organisme do2 ON do2.organisme_sk = fp.organisme_sk
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND fp.organisme_sk <> 0
          AND dt.year_num > 0 {year_filter}
        GROUP BY do2.codetab, do2.liborgl, dt.year_num
        ORDER BY dt.year_num DESC, total_netpay DESC
        LIMIT {top_n * 3}
    """
    return _fmt_rows(_query(sql), "Ministry Payroll Breakdown")


def _intent_grade_breakdown(e: dict) -> str:
    year_filter = "AND dt.year_num = ANY(%s)" if e.get("years") else ""
    grade_filter = "AND dg.grade_code = %s" if e.get("grade_code") else ""
    params: tuple = ()
    if e.get("years") and e.get("grade_code"):
        params = (e["years"], e["grade_code"])
    elif e.get("years"):
        params = (e["years"],)
    elif e.get("grade_code"):
        params = (e["grade_code"],)

    sql = f"""
        SELECT dg.grade_code, dg.grade_label_fr,
               COUNT(DISTINCT fp.employee_sk)  AS employees,
               AVG(fp.m_netpay)                AS avg_netpay,
               PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fp.m_netpay) AS median_netpay,
               MIN(fp.m_netpay)                AS min_netpay,
               MAX(fp.m_netpay)                AS max_netpay,
               SUM(fp.m_netpay)                AS total_netpay
        FROM dw.fact_paie fp
        JOIN dw.dim_grade dg ON dg.grade_sk = fp.grade_sk
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND dg.grade_sk <> 0
          AND dt.year_num > 0 {year_filter} {grade_filter}
        GROUP BY dg.grade_code, dg.grade_label_fr
        ORDER BY avg_netpay DESC
        LIMIT 20
    """
    return _fmt_rows(_query(sql, params), "Salary by Grade")


def _intent_employee_count(e: dict) -> str:
    sql = """
        SELECT dt.year_num,
               COUNT(DISTINCT fp.employee_sk) AS active_employees,
               COUNT(*) AS total_payroll_records,
               ROUND(COUNT(*)::numeric / COUNT(DISTINCT fp.employee_sk), 1) AS avg_months_per_employee
        FROM dw.fact_paie fp
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND dt.year_num > 0
        GROUP BY dt.year_num
        ORDER BY dt.year_num DESC
        LIMIT 8
    """
    return _fmt_rows(_query(sql), "Employee Count by Year")


def _intent_employee_profile(e: dict) -> str:
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


def _intent_avg_salary(e: dict) -> str:
    year_filter = "AND dt.year_num = ANY(%s)" if e.get("years") else ""
    params = (e["years"],) if e.get("years") else ()
    sql = f"""
        SELECT dg.grade_code, dg.grade_label_fr, dg.category,
               COUNT(DISTINCT fp.employee_sk)  AS employees,
               ROUND(AVG(fp.m_netpay)::numeric, 0)   AS avg_netpay,
               ROUND(AVG(fp.m_salbrut)::numeric, 0)  AS avg_gross,
               ROUND(AVG(fp.m_retrait)::numeric, 0)  AS avg_deductions,
               ROUND(MIN(fp.m_netpay)::numeric, 0)   AS min_netpay,
               ROUND(MAX(fp.m_netpay)::numeric, 0)   AS max_netpay
        FROM dw.fact_paie fp
        JOIN dw.dim_grade dg ON dg.grade_sk = fp.grade_sk
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND dg.grade_sk <> 0
          AND dt.year_num > 0 {year_filter}
        GROUP BY dg.grade_code, dg.grade_label_fr, dg.category
        ORDER BY avg_netpay DESC
        LIMIT 20
    """
    return _fmt_rows(_query(sql, params), "Average Salary by Grade")


def _intent_salary_distribution(e: dict) -> str:
    year_filter = "AND dt.year_num = ANY(%s)" if e.get("years") else "AND dt.year_num = (SELECT MAX(year_num) FROM dw.dim_temps WHERE year_num > 0)"
    params = (e["years"],) if e.get("years") else ()
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
        WHERE fp.employee_sk <> 0 AND dt.year_num > 0 {year_filter}
        GROUP BY 1
        ORDER BY MIN(fp.m_netpay)
    """
    return _fmt_rows(_query(sql, params), "Salary Distribution")


def _intent_trends(e: dict) -> str:
    sql = """
        SELECT dt.year_num,
               SUM(fp.m_netpay)               AS total_netpay,
               COUNT(DISTINCT fp.employee_sk) AS employees,
               ROUND(AVG(fp.m_netpay)::numeric, 0) AS avg_netpay,
               LAG(SUM(fp.m_netpay)) OVER (ORDER BY dt.year_num) AS prev_year_netpay
        FROM dw.fact_paie fp
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND dt.year_num > 0
        GROUP BY dt.year_num
        ORDER BY dt.year_num
    """
    rows = _query(sql)
    if rows:
        for r in rows:
            if r.get("prev_year_netpay") and r["prev_year_netpay"] > 0:
                r["yoy_growth_pct"] = round(
                    (r["total_netpay"] - r["prev_year_netpay"]) / r["prev_year_netpay"] * 100, 2)
    return _fmt_rows(rows, "Year-over-Year Payroll Trend")


def _intent_indemnities(e: dict) -> str:
    year_filter = "AND dt.year_num = ANY(%s)" if e.get("years") else ""
    params = (e["years"],) if e.get("years") else ()
    sql = f"""
        SELECT di.indemnite_code,
               di.indemnite_label_fr    AS indemnite_label,
               di.nature_flag,
               COUNT(DISTINCT fi.employee_sk) AS employees,
               SUM(fi.m_netpay)         AS total_amount,
               ROUND(AVG(fi.m_netpay)::numeric, 0) AS avg_amount
        FROM dw.fact_indem fi
        JOIN dw.dim_indemnite di ON di.indemnite_sk = fi.indemnite_sk
        JOIN dw.dim_temps dt ON dt.time_sk = fi.time_sk
        WHERE fi.employee_sk <> 0 AND di.indemnite_sk <> 0
          AND dt.year_num > 0 {year_filter}
        GROUP BY di.indemnite_code, di.indemnite_label_fr, di.nature_flag
        ORDER BY total_amount DESC
        LIMIT 15
    """
    return _fmt_rows(_query(sql, params), "Indemnity/Allowance Breakdown")


def _intent_regional(e: dict) -> str:
    year_filter = "AND dt.year_num = ANY(%s)" if e.get("years") else ""
    params = (e["years"],) if e.get("years") else ()
    sql = f"""
        SELECT dr.lib_reg AS region, dr.codreg,
               COUNT(DISTINCT fp.employee_sk) AS employees,
               SUM(fp.m_netpay)               AS total_netpay,
               ROUND(AVG(fp.m_netpay)::numeric, 0) AS avg_netpay
        FROM dw.fact_paie fp
        JOIN dw.dim_region dr ON dr.region_sk = fp.region_sk
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND fp.region_sk <> 0
          AND dt.year_num > 0 {year_filter}
        GROUP BY dr.lib_reg, dr.codreg
        ORDER BY total_netpay DESC
        LIMIT 20
    """
    return _fmt_rows(_query(sql, params), "Regional Payroll Breakdown")


def _intent_recent_month(e: dict) -> str:
    sql = """
        SELECT dt.year_num, dt.month_num,
               COUNT(DISTINCT fp.employee_sk)  AS employees,
               SUM(fp.m_netpay)                AS total_netpay,
               ROUND(AVG(fp.m_netpay)::numeric, 0) AS avg_netpay,
               ROUND(AVG(fp.m_salbrut)::numeric, 0) AS avg_gross,
               ROUND(AVG(fp.m_retrait)::numeric, 0) AS avg_deductions
        FROM dw.fact_paie fp
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND dt.year_num > 0
        GROUP BY dt.year_num, dt.month_num
        ORDER BY dt.year_num DESC, dt.month_num DESC
        LIMIT 3
    """
    return _fmt_rows(_query(sql), "Most Recent Months")


def _intent_deductions(e: dict) -> str:
    year_filter = "AND dt.year_num = ANY(%s)" if e.get("years") else ""
    params = (e["years"],) if e.get("years") else ()
    sql = f"""
        SELECT dt.year_num,
               ROUND(AVG(fp.m_retrait)::numeric, 0) AS avg_deductions,
               ROUND(AVG(fp.m_cps)::numeric, 0)     AS avg_cps,
               ROUND(AVG(fp.m_cpe)::numeric, 0)     AS avg_cpe,
               ROUND(AVG(fp.m_capdeces)::numeric, 0) AS avg_capdeces,
               ROUND(SUM(fp.m_retrait)::numeric, 0)  AS total_deductions,
               ROUND(AVG(fp.m_retrait / NULLIF(fp.m_salbrut,0) * 100)::numeric, 2) AS avg_deduction_pct
        FROM dw.fact_paie fp
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND dt.year_num > 0 {year_filter}
        GROUP BY dt.year_num
        ORDER BY dt.year_num DESC
        LIMIT 5
    """
    return _fmt_rows(_query(sql, params), "Deductions Analysis")


def _intent_anomalies(_e: dict) -> str:
    path = _MODELS_DIR / "anomaly_results.json"
    if not path.exists():
        return "[Anomaly results not found — model not yet trained]"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        winner = data.get("winner", "if")
        mc = data.get("method_comparison", {})
        wm = mc.get(winner, {})
        meta = data.get("_meta", {}).get("final_flag", {})

        lines = [
            f"**Anomaly Detection Summary** (model: {data.get('model','?')}, winner: {winner.upper()})",
            f"- Total records analyzed: {data.get('total_records', '?'):,}",
            f"- Total employees: {data.get('total_employees', '?'):,}",
            f"- Anomalies flagged: {wm.get('n_flagged', meta.get('n_flagged', '?')):,}",
            f"- Anomaly rate: {wm.get('rate_pct', meta.get('anomaly_rate', '?'))}%",
            f"- Z-score threshold: {data.get('zscore_threshold', 3.0)}",
            "",
            "**Method comparison:**",
        ]
        for method, stats in mc.items():
            lines.append(
                f"  - {method.upper()}: {stats.get('n_flagged',0):,} flagged "
                f"({stats.get('rate_pct',0):.2f}%), avg z-score: {stats.get('avg_zscore',0):.3f}"
            )

        # severity breakdown if available
        sev = data.get("severity_summary") or (meta.get("severity_summary") if meta else None)
        if sev:
            lines += ["", "**Severity breakdown:**",
                      f"  - High (|z|≥3.5): {sev.get('high',0):,}",
                      f"  - Medium (|z|≥2.5): {sev.get('medium',0):,}",
                      f"  - Low: {sev.get('low',0):,}"]
        return "\n".join(lines)
    except Exception as exc:
        return f"[Anomaly data parse error: {exc}]"


def _intent_forecast(_e: dict) -> str:
    path = _MODELS_DIR / "payroll_forecast_results.json"
    if not path.exists():
        return "[Forecast results not found — model not yet trained]"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        winner = data.get("winner", "sarima")
        mc = data.get("model_comparison", {})
        wm = mc.get(winner, {})
        forecasts = data.get("forecast_6m", [])

        lines = [
            f"**Payroll Forecast Summary** (best model: {winner.upper()})",
            f"- Training months: {data.get('train_months','?')}",
            f"- Best model MAPE: {wm.get('mape','?')}%  |  RMSE: {wm.get('rmse',0):,.0f} TND",
            "",
            "**6-month forecast (total net pay):**",
        ]
        for f in forecasts:
            lines.append(f"  - {f['date']}: {f['predicted_netpay']:,.0f} TND")

        lines += ["", "**Model comparison (MAPE %):**"]
        for model, s in mc.items():
            if s.get("mape", 0) > 0:
                lines.append(f"  - {model.upper()}: MAPE={s['mape']:.2f}%, DA={s.get('da',0):.1f}%")
        return "\n".join(lines)
    except Exception as exc:
        return f"[Forecast data parse error: {exc}]"


def _intent_general_stats(_e: dict) -> str:
    sql = """
        SELECT COUNT(*)                        AS total_records,
               COUNT(DISTINCT fp.employee_sk)  AS total_employees,
               ROUND(SUM(fp.m_netpay)::numeric, 0)   AS total_netpay_all_time,
               ROUND(AVG(fp.m_netpay)::numeric, 0)   AS avg_netpay,
               ROUND(MIN(fp.m_netpay)::numeric, 0)   AS min_netpay,
               ROUND(MAX(fp.m_netpay)::numeric, 0)   AS max_netpay,
               MIN(dt.year_num)               AS first_year,
               MAX(dt.year_num)               AS last_year
        FROM dw.fact_paie fp
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0 AND dt.year_num > 0
    """
    rows = _query(sql)
    return _fmt_rows(rows, "General Data Warehouse Statistics")


# ── Intent registry ────────────────────────────────────────────────────────────
# Each entry: (score_patterns, handler_fn)

_INTENT_REGISTRY = [
    # (regex_patterns_list, handler_fn, name)
    ([r"anomal|fraud|irregular|suspicious|unusual|flag|detect"], _intent_anomalies, "anomalies"),
    ([r"forecast|predict|next month|future|projection|prévision"], _intent_forecast, "forecast"),
    ([r"how many employee|number of employee|employee count|workforce|headcount|effectif"], _intent_employee_count, "employee_count"),
    ([r"employee[_\s]+\d{4,9}|agent[_\s]+\d{4,9}|profile.*employee|employee.*profile"], _intent_employee_profile, "employee_profile"),
    ([r"ministr|organisme|department|ministry|ministère"], _intent_ministry_breakdown, "ministry"),
    ([r"grade|échelon|echelon|categor|cadre"], _intent_grade_breakdown, "grade"),
    ([r"indemn|allowance|bonus|supplement|prime|allocation"], _intent_indemnities, "indemnities"),
    ([r"region|governorate|gouvernorat|location|geographic|géograph"], _intent_regional, "regional"),
    ([r"deduct|retrait|cotis|cps|cpe|withhold|prélèv"], _intent_deductions, "deductions"),
    ([r"average|avg|mean|median|typical|distribution|range|dispersion|répartition"], _intent_avg_salary, "avg_salary"),
    ([r"distribut|range|bracket|tranche|histogram|bucket"], _intent_salary_distribution, "distribution"),
    ([r"trend|growth|increas|decreas|evolut|over.*year|year.*over|progression|évolution"], _intent_trends, "trends"),
    ([r"total.*pay|budget|payroll.*total|masse salariale|total.*salary|total.*net"], _intent_yearly_summary, "yearly"),
    ([r"last month|recent|latest|current|this month|last year|dernier|récent"], _intent_recent_month, "recent"),
    ([r"monthly|by month|per month|mensuel|mois"], _intent_total_payroll, "monthly"),
]


def _score_intent(question: str, patterns: list[str]) -> int:
    q = question.lower()
    return sum(1 for p in patterns if re.search(p, q))


def _detect_and_retrieve(question: str, entities: dict) -> str:
    q = question.lower()

    # Score all intents
    scored = [
        (sum(1 for p in patterns if re.search(p, q)), fn, name)
        for patterns, fn, name in _INTENT_REGISTRY
    ]
    scored.sort(key=lambda x: -x[0])

    # Run top 1 or 2 intents if both score > 0
    results = []
    for score, fn, name in scored[:2]:
        if score == 0:
            break
        try:
            result = fn(entities)
            if result and "[" not in result[:5]:  # not an error
                results.append(result)
        except Exception as exc:
            results.append(f"[{name} error: {exc}]")

    if results:
        return "\n\n".join(results)

    # Fallback: general stats
    try:
        return _intent_general_stats(entities)
    except Exception as exc:
        return f"[General stats error: {exc}]"


# ── Ollama ─────────────────────────────────────────────────────────────────────

def _format_history(history: list[dict]) -> str:
    if not history:
        return ""
    lines = ["\nPrevious conversation (for context):"]
    for turn in history[-4:]:  # last 4 turns
        role = "User" if turn.get("role") == "user" else "Assistant"
        text = str(turn.get("text", ""))[:300]  # truncate long turns
        lines.append(f"{role}: {text}")
    return "\n".join(lines)


def _ollama_chat(context: str, question: str, history: list[dict],
                 model: str = DEFAULT_MODEL) -> str:
    history_str = _format_history(history)
    prompt = (
        f"{SYSTEM_PROMPT}\n\n"
        f"{history_str}\n\n"
        f"DATA CONTEXT (live from INSAF data warehouse):\n{context}\n\n"
        f"USER QUESTION: {question}\n\n"
        f"ANSWER (use markdown formatting — bold key numbers, bullet points for lists):"
    )
    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.15, "num_predict": 700},
    }
    r = requests.post(f"{OLLAMA_BASE}/api/generate", json=payload, timeout=120)
    r.raise_for_status()
    return r.json().get("response", "").strip()


def _ollama_available(model: str = DEFAULT_MODEL) -> bool:
    try:
        r = requests.get(f"{OLLAMA_BASE}/api/tags", timeout=3)
        if r.status_code != 200:
            return False
        names = [m["name"].split(":")[0] for m in r.json().get("models", [])]
        return model.split(":")[0] in names
    except Exception:
        return False


# ── Public API ─────────────────────────────────────────────────────────────────

def chat(question: str, model: str = DEFAULT_MODEL,
         history: list[dict] | None = None) -> dict[str, Any]:
    """
    Main RAG chat function.
    history: list of {role: 'user'|'bot', text: str} dicts (last N turns).
    """
    history = history or []
    entities = _extract_entities(question)

    try:
        context = _detect_and_retrieve(question, entities)
    except Exception as e:
        context = f"[Retrieval error: {e}]"

    if _ollama_available(model):
        try:
            answer = _ollama_chat(context, question, history, model=model)
            llm_used = True
        except Exception as e:
            answer = f"LLM error: {e}\n\n{context}"
            llm_used = False
    else:
        answer = (
            f"⚠️ **Ollama not running** — showing raw data\n\n"
            f"{context}\n\n"
            f"*To enable AI answers: start Ollama and run `ollama pull {model}`*"
        )
        llm_used = False

    return {
        "question": question,
        "answer":   answer,
        "context":  context,
        "model":    model,
        "llm_used": llm_used,
        "entities": entities,
    }
