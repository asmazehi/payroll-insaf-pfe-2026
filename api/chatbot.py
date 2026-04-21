"""
api/chatbot.py
==============
RAG Chatbot — Retrieval-Augmented Generation over the INSAF PostgreSQL DW.

Architecture:
    User question
         |
    SQL retrieval layer (query DW for relevant payroll facts)
         |
    Context builder (formats retrieved data into readable context)
         |
    Ollama llama3.2 (generates answer grounded in the retrieved context)
         |
    Answer

This avoids hallucination: the LLM only answers from real DW data.

Requires:
    - Ollama running on http://localhost:11434
    - llama3.2 model pulled: ollama pull llama3.2
    - PostgreSQL DW accessible via etl.core.config.DB_CONFIG
"""
from __future__ import annotations

import re
from typing import Any

import psycopg2
import requests

OLLAMA_BASE   = "http://localhost:11434"
DEFAULT_MODEL = "llama3.2"

# ── System prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are INSAF Assistant, an AI analyst for the Tunisian Government Payroll Intelligence System (INSAF).
You have access to real payroll data retrieved from the data warehouse.
Answer questions clearly and concisely based ONLY on the provided data context.
If the data doesn't contain the answer, say so honestly.
Use TND (Tunisian Dinar) for monetary values. Be specific with numbers.
Do not invent data that is not in the context."""


# ── SQL retrieval layer ───────────────────────────────────────────────────────

def _get_db_conn():
    from etl.core.config import DB_CONFIG
    return psycopg2.connect(**DB_CONFIG)


def _query(sql: str, params: tuple = ()) -> list[dict]:
    """Execute a SQL query and return rows as list of dicts."""
    with _get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]


# Intent patterns → retrieval queries
_INTENTS = [
    # Total payroll
    (r"total.*(payroll|salary|net.?pay|budget)|budget.*payroll",
     """
     SELECT dt.year_num, SUM(fp.m_netpay) AS total_netpay,
            COUNT(DISTINCT fp.employee_sk) AS employees,
            COUNT(*) AS records
     FROM dw.fact_paie fp
     JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
     WHERE fp.employee_sk <> 0 AND dt.year_num > 0
     GROUP BY dt.year_num
     ORDER BY dt.year_num DESC
     LIMIT 5
     """),

    # Ministry breakdown
    (r"ministr|organisme|department",
     """
     SELECT do2.liborgl AS ministry, dt.year_num,
            SUM(fp.m_netpay) AS total_netpay,
            COUNT(DISTINCT fp.employee_sk) AS employees
     FROM dw.fact_paie fp
     JOIN dw.dim_organisme do2 ON do2.organisme_sk = fp.organisme_sk
     JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
     WHERE fp.employee_sk <> 0 AND fp.organisme_sk <> 0 AND dt.year_num > 0
     GROUP BY do2.liborgl, dt.year_num
     ORDER BY dt.year_num DESC, total_netpay DESC
     LIMIT 20
     """),

    # Grade breakdown
    (r"grade|echelon|categor",
     """
     SELECT dg.grade_code, dg.lib_grade,
            COUNT(DISTINCT fp.employee_sk) AS employees,
            AVG(fp.m_netpay) AS avg_netpay,
            SUM(fp.m_netpay) AS total_netpay
     FROM dw.fact_paie fp
     JOIN dw.dim_grade dg ON dg.grade_sk = fp.grade_sk
     WHERE fp.employee_sk <> 0
     GROUP BY dg.grade_code, dg.lib_grade
     ORDER BY total_netpay DESC
     LIMIT 15
     """),

    # Anomalies
    (r"anomal|fraud|irregular|suspicious|unusual|flag",
     """
     SELECT employee_sk, grade_code, nature_code, ministry_code,
            month_num, year_num, m_netpay, z_score, pct_deviation,
            zscore_flag, if_flag
     FROM dw.fact_paie fp
     WHERE fp.employee_sk <> 0
     LIMIT 1
     """),  # placeholder — we'll redirect to CSV report

    # Average salary
    (r"average|avg|mean.*salary|salary.*mean|typical.*pay",
     """
     SELECT dg.grade_code, dg.lib_grade,
            AVG(fp.m_netpay)    AS avg_netpay,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fp.m_netpay) AS median_netpay,
            MIN(fp.m_netpay)    AS min_netpay,
            MAX(fp.m_netpay)    AS max_netpay,
            COUNT(*)            AS records
     FROM dw.fact_paie fp
     JOIN dw.dim_grade dg ON dg.grade_sk = fp.grade_sk
     WHERE fp.employee_sk <> 0
     GROUP BY dg.grade_code, dg.lib_grade
     ORDER BY avg_netpay DESC
     LIMIT 10
     """),

    # Employee count
    (r"how many employee|number of employee|employee count|workforce",
     """
     SELECT dt.year_num,
            COUNT(DISTINCT fp.employee_sk) AS total_employees
     FROM dw.fact_paie fp
     JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
     WHERE fp.employee_sk <> 0 AND dt.year_num > 0
     GROUP BY dt.year_num
     ORDER BY dt.year_num DESC
     LIMIT 5
     """),

    # Indemnities
    (r"indemn|allowance|bonus|supplement",
     """
     SELECT di.label AS indemnity_type,
            SUM(fi.m_montant) AS total_amount,
            COUNT(DISTINCT fi.employee_sk) AS employees,
            AVG(fi.m_montant) AS avg_amount
     FROM dw.fact_indem fi
     JOIN dw.dim_indemnite di ON di.indemnite_sk = fi.indemnite_sk
     WHERE fi.employee_sk <> 0
     GROUP BY di.label
     ORDER BY total_amount DESC
     LIMIT 10
     """),

    # Trends / growth
    (r"trend|growth|increas|decreas|evolut|over.*year|year.*over",
     """
     SELECT dt.year_num,
            SUM(fp.m_netpay)               AS total_netpay,
            COUNT(DISTINCT fp.employee_sk)  AS employees,
            AVG(fp.m_netpay)               AS avg_netpay
     FROM dw.fact_paie fp
     JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
     WHERE fp.employee_sk <> 0 AND dt.year_num > 0
     GROUP BY dt.year_num
     ORDER BY dt.year_num
     """),
]


def _detect_intent_and_retrieve(question: str) -> str:
    """
    Match question to an intent, run the SQL query, and return
    formatted context string for the LLM.
    """
    q_lower = question.lower()

    # Special case: anomaly questions → read CSV report if available
    if re.search(r"anomal|fraud|irregular|suspicious|unusual|flag", q_lower):
        try:
            import pandas as pd
            from pathlib import Path
            report = Path(__file__).resolve().parent.parent / "ml" / "models" / "anomaly_report.csv"
            if report.exists():
                df = pd.read_csv(report)
                total  = len(df)
                rate   = (df["anomaly_flag"].sum() / total * 100) if total > 0 else 0
                top5   = df[df["anomaly_flag"]].nlargest(5, "z_score")[
                    ["employee_sk", "grade_code", "ministry_code",
                     "month_num", "year_num", "m_netpay", "z_score"]
                ].to_string(index=False)
                return (
                    f"Anomaly report summary:\n"
                    f"- Total records analyzed: {total:,}\n"
                    f"- Anomalies detected: {int(df['anomaly_flag'].sum()):,} ({rate:.2f}%)\n"
                    f"- Z-score flagged: {int(df['zscore_flag'].sum()):,}\n"
                    f"- Isolation Forest flagged: {int(df['if_flag'].sum()):,}\n\n"
                    f"Top 5 highest z-score anomalies:\n{top5}"
                )
        except Exception:
            pass

    # General intent matching → SQL
    for pattern, sql in _INTENTS:
        if re.search(pattern, q_lower):
            try:
                rows = _query(sql)
                if not rows:
                    return "No data found for this query."
                # Format as readable text
                lines = []
                for i, row in enumerate(rows[:20]):
                    parts = []
                    for k, v in row.items():
                        if isinstance(v, float):
                            parts.append(f"{k}: {v:,.2f}")
                        else:
                            parts.append(f"{k}: {v}")
                    lines.append("  " + " | ".join(parts))
                return f"Retrieved {len(rows)} records:\n" + "\n".join(lines)
            except Exception as e:
                return f"[DB error: {e}]"

    # Fallback: general stats
    try:
        rows = _query("""
            SELECT COUNT(*) AS total_records,
                   COUNT(DISTINCT employee_sk) AS employees,
                   SUM(m_netpay) AS total_netpay,
                   MIN(m_netpay) AS min_netpay,
                   MAX(m_netpay) AS max_netpay,
                   AVG(m_netpay) AS avg_netpay
            FROM dw.fact_paie
            WHERE employee_sk <> 0
        """)
        if rows:
            r = rows[0]
            return (
                f"General DW stats: {r['total_records']:,} payroll records, "
                f"{r['employees']:,} employees, "
                f"total net pay {r['total_netpay']:,.0f} TND, "
                f"avg {r['avg_netpay']:,.0f} TND/month/employee."
            )
    except Exception as e:
        return f"[DB error: {e}]"

    return "No specific data retrieved."


# ── Ollama call ───────────────────────────────────────────────────────────────

def _ollama_chat(system: str, context: str, question: str,
                 model: str = DEFAULT_MODEL,
                 temperature: float = 0.2,
                 max_tokens: int = 500) -> str:
    """Send a RAG-style prompt to Ollama and return the response."""
    prompt = (
        f"{system}\n\n"
        f"DATA CONTEXT (retrieved from INSAF data warehouse):\n"
        f"{context}\n\n"
        f"USER QUESTION: {question}\n\n"
        f"ANSWER:"
    )
    payload = {
        "model":   model,
        "prompt":  prompt,
        "stream":  False,
        "options": {
            "temperature": temperature,
            "num_predict": max_tokens,
        },
    }
    r = requests.post(f"{OLLAMA_BASE}/api/generate",
                      json=payload, timeout=90)
    r.raise_for_status()
    return r.json().get("response", "").strip()


def _ollama_available(model: str = DEFAULT_MODEL) -> bool:
    try:
        r = requests.get(f"{OLLAMA_BASE}/api/tags", timeout=3)
        if r.status_code != 200:
            return False
        models = [m["name"].split(":")[0] for m in r.json().get("models", [])]
        return model.split(":")[0] in models
    except Exception:
        return False


# ── Public API ────────────────────────────────────────────────────────────────

def chat(question: str, model: str = DEFAULT_MODEL) -> dict[str, Any]:
    """
    Main RAG chat function. Returns a dict with:
        answer    : the LLM-generated answer
        context   : the data context retrieved from DW
        model     : the model used
        llm_used  : True if LLM answered, False if fallback
    """
    # Step 1: retrieve relevant data from DW
    try:
        context = _detect_intent_and_retrieve(question)
    except Exception as e:
        context = f"[Retrieval error: {e}]"

    # Step 2: generate answer with LLM
    if _ollama_available(model):
        try:
            answer  = _ollama_chat(SYSTEM_PROMPT, context, question, model=model)
            llm_used = True
        except Exception as e:
            answer   = f"LLM error: {e}. Raw data: {context}"
            llm_used = False
    else:
        # Fallback: return raw retrieved context without LLM
        answer   = (
            f"[Ollama not available — showing raw data]\n\n{context}\n\n"
            f"To enable LLM answers: start Ollama and run 'ollama pull {model}'"
        )
        llm_used = False

    return {
        "question": question,
        "answer":   answer,
        "context":  context,
        "model":    model,
        "llm_used": llm_used,
    }
