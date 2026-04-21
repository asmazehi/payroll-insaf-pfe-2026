"""
ml/llm_explainer.py
===================
LLM Anomaly Explainer — uses Ollama (local, free) to generate
plain-language explanations for flagged payroll anomalies.

Model: llama3.2 (pulled via: ollama pull llama3.2)
Requires: Ollama running locally on http://localhost:11434

Usage:
    from ml.llm_explainer import explain_anomaly, explain_batch

    # Single record
    row = df[df["anomaly_flag"]].iloc[0]
    explanation = explain_anomaly(row)

    # Batch (returns list of dicts with explanation added)
    results = explain_batch(df[df["anomaly_flag"]].head(10))
"""
from __future__ import annotations

import json
import re
import time
from typing import Any

import pandas as pd
import requests

OLLAMA_BASE   = "http://localhost:11434"
DEFAULT_MODEL = "llama3.2"

# ── Connection check ──────────────────────────────────────────────────────────

def _ollama_available(model: str = DEFAULT_MODEL) -> bool:
    """Return True if Ollama is running and the model is loaded."""
    try:
        r = requests.get(f"{OLLAMA_BASE}/api/tags", timeout=3)
        if r.status_code != 200:
            return False
        models = [m["name"].split(":")[0] for m in r.json().get("models", [])]
        return model.split(":")[0] in models
    except Exception:
        return False


def _generate(prompt: str, model: str = DEFAULT_MODEL,
              temperature: float = 0.3, max_tokens: int = 300) -> str:
    """Call Ollama /api/generate and return the response text."""
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
                      json=payload, timeout=60)
    r.raise_for_status()
    return r.json().get("response", "").strip()


# ── Prompt builder ────────────────────────────────────────────────────────────

def _build_prompt(row: pd.Series) -> str:
    """Build a concise anomaly explanation prompt from a payroll record."""
    flags = []
    if row.get("zscore_flag"):
        flags.append(f"Z-score={row.get('z_score', 'N/A'):.2f} (threshold: |z|>3)")
    if row.get("if_flag"):
        flags.append(f"Isolation Forest score={row.get('if_score', 'N/A'):.4f} (negative = anomalous)")

    deviation = row.get("pct_deviation", 0)
    emp_mean  = row.get("emp_mean", 0)
    emp_std   = row.get("emp_std", 0)
    netpay    = row.get("m_netpay", 0)

    prompt = f"""You are a payroll auditor analyzing anomalous salary records for the Tunisian government (INSAF system).

Employee record:
- Employee ID: {row.get('employee_sk', 'N/A')}
- Grade: {row.get('grade_code', 'N/A')}
- Nature code: {row.get('nature_code', 'N/A')}
- Ministry: {row.get('ministry_code', 'N/A')}
- Month/Year: {int(row.get('month_num', 0))}/{int(row.get('year_num', 0))}
- Net pay this month: {netpay:,.2f} TND
- This employee's average net pay: {emp_mean:,.2f} TND
- Standard deviation: {emp_std:,.2f} TND
- Deviation from personal median: {deviation:.1f}%

Anomaly detection flags triggered: {', '.join(flags) if flags else 'anomaly_flag combined'}

Write a short (2-3 sentences) plain-language explanation of why this record is flagged as anomalous.
Be specific about the numbers. Do not use jargon. Do not say "I" or start with "This record".
Focus on what is unusual and what it might indicate (data entry error, pay adjustment, fraud risk, etc.)."""

    return prompt


# ── Public API ────────────────────────────────────────────────────────────────

def explain_anomaly(row: pd.Series,
                    model: str = DEFAULT_MODEL) -> str:
    """
    Generate a plain-language explanation for a single anomalous payroll record.
    Returns a string explanation, or a fallback message if Ollama is unavailable.
    """
    if not _ollama_available(model):
        return (
            f"[LLM unavailable] Net pay {row.get('m_netpay', 0):,.0f} TND deviates "
            f"{row.get('pct_deviation', 0):.1f}% from employee median "
            f"(z={row.get('z_score', 0):.2f})."
        )

    prompt = _build_prompt(row)
    try:
        return _generate(prompt, model=model)
    except Exception as e:
        return f"[LLM error: {e}] Z-score={row.get('z_score', 0):.2f}, deviation={row.get('pct_deviation', 0):.1f}%"


def explain_batch(df_anomalies: pd.DataFrame,
                  model: str = DEFAULT_MODEL,
                  max_records: int = 50,
                  delay_secs: float = 0.1) -> list[dict]:
    """
    Generate explanations for a batch of anomalous records.

    Args:
        df_anomalies : DataFrame filtered to anomaly_flag == True
        max_records  : cap to avoid very long runs (default 50)
        delay_secs   : small pause between calls to avoid overloading Ollama

    Returns:
        List of dicts with original record fields + 'explanation' key.
    """
    available = _ollama_available(model)
    if not available:
        print(f"[llm_explainer] Ollama not running or model '{model}' not found.")
        print("  Start Ollama and run: ollama pull llama3.2")

    results = []
    subset  = df_anomalies.head(max_records)

    for i, (_, row) in enumerate(subset.iterrows()):
        expl = explain_anomaly(row, model=model)
        record = row.to_dict()
        record["explanation"] = expl
        results.append(record)

        if (i + 1) % 10 == 0:
            print(f"  [llm_explainer] {i + 1}/{len(subset)} explained...")
        if delay_secs > 0:
            time.sleep(delay_secs)

    return results


def save_explanations(df_anomalies: pd.DataFrame,
                      output_path: str | None = None,
                      max_records: int = 50) -> pd.DataFrame:
    """
    Explain anomalies and save to CSV. Returns a DataFrame with explanations.
    Default output: ml/models/anomaly_explanations.csv
    """
    from pathlib import Path

    if output_path is None:
        output_path = str(
            Path(__file__).resolve().parent / "models" / "anomaly_explanations.csv"
        )

    results  = explain_batch(df_anomalies, max_records=max_records)
    df_out   = pd.DataFrame(results)

    keep_cols = [
        "employee_sk", "grade_code", "nature_code", "ministry_code",
        "month_num", "year_num", "m_netpay", "emp_mean", "emp_std",
        "z_score", "pct_deviation", "zscore_flag", "if_flag",
        "anomaly_flag", "explanation",
    ]
    export_cols = [c for c in keep_cols if c in df_out.columns]
    df_out[export_cols].to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"  Saved {len(df_out)} explanations -> {output_path}")
    return df_out[export_cols]
