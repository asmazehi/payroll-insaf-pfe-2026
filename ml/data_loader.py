"""
ml/data_loader.py
=================
Loads data from PostgreSQL DW into pandas DataFrames for ML.
All queries filter out unknown members (sk=0) and year=0.
"""
from __future__ import annotations
import pandas as pd
import psycopg2
from etl.core.config import DB_CONFIG


def _conn():
    return psycopg2.connect(**DB_CONFIG)


def load_monthly_payroll() -> pd.DataFrame:
    """
    Aggregate fact_paie by year+month.
    Used for: time series forecasting of total payroll.
    Returns one row per month.
    """
    sql = """
        SELECT
            dt.year_num,
            dt.month_num,
            dt.month_start_date,
            COUNT(*)                  AS employee_count,
            SUM(fp.m_netpay)          AS total_netpay,
            SUM(fp.m_salbrut)         AS total_salbrut,
            AVG(fp.m_netpay)          AS avg_netpay,
            SUM(fp.m_retrait)         AS total_deductions,
            SUM(fp.m_cps)             AS total_cps,
            SUM(fp.m_cpe)             AS total_cpe
        FROM dw.fact_paie fp
        JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
        WHERE fp.employee_sk <> 0
          AND dt.year_num > 0
          AND fp.m_netpay IS NOT NULL
        GROUP BY dt.year_num, dt.month_num, dt.month_start_date
        ORDER BY dt.year_num, dt.month_num
    """
    with _conn() as conn:
        df = pd.read_sql(sql, conn, parse_dates=["month_start_date"])
    df = df.sort_values("month_start_date").reset_index(drop=True)
    return df


def load_monthly_payroll_by_ministry() -> pd.DataFrame:
    """
    Aggregate fact_paie by year+month+ministry.
    Used for: per-ministry payroll forecasting.
    """
    sql = """
        SELECT
            dt.year_num,
            dt.month_num,
            dt.month_start_date,
            do2.codetab            AS ministry_code,
            do2.liborgl            AS ministry_name_fr,
            COUNT(*)               AS employee_count,
            SUM(fp.m_netpay)       AS total_netpay,
            SUM(fp.m_salbrut)      AS total_salbrut,
            AVG(fp.m_netpay)       AS avg_netpay
        FROM dw.fact_paie fp
        JOIN dw.dim_temps     dt  ON dt.time_sk      = fp.time_sk
        JOIN dw.dim_organisme do2 ON do2.organisme_sk = fp.organisme_sk
        WHERE fp.employee_sk  <> 0
          AND dt.year_num     >  0
          AND do2.organisme_sk <> 0
          AND fp.m_netpay IS NOT NULL
        GROUP BY dt.year_num, dt.month_num, dt.month_start_date,
                 do2.codetab, do2.liborgl
        ORDER BY dt.year_num, dt.month_num
    """
    with _conn() as conn:
        df = pd.read_sql(sql, conn, parse_dates=["month_start_date"])
    df = df.sort_values(["ministry_code", "month_start_date"]).reset_index(drop=True)
    return df


def load_individual_payroll() -> pd.DataFrame:
    """
    Individual-level payroll records with dimension attributes.
    Used for: salary prediction and anomaly detection.
    """
    sql = """
        SELECT
            fp.employee_sk,
            dt.year_num,
            dt.month_num,
            dt.month_start_date,
            dg.grade_code,
            dg.grade_label_fr,
            dg.category,
            dg.retire_age,
            dn.nature_code,
            dn.nature_label_fr,
            do2.codetab           AS ministry_code,
            do2.liborgl           AS ministry_name_fr,
            fp.pa_eche,
            fp.pa_sitfam,
            fp.m_netpay,
            fp.m_salbrut,
            fp.m_salimp,
            fp.m_retrait,
            fp.m_cps,
            fp.m_cpe,
            fp.m_capdeces,
            fp.m_sub,
            fp.m_avkm,
            fp.m_avlog
        FROM dw.fact_paie fp
        JOIN dw.dim_temps     dt  ON dt.time_sk       = fp.time_sk
        JOIN dw.dim_grade     dg  ON dg.grade_sk       = fp.grade_sk
        JOIN dw.dim_nature    dn  ON dn.nature_sk      = fp.nature_sk
        JOIN dw.dim_organisme do2 ON do2.organisme_sk  = fp.organisme_sk
        WHERE fp.employee_sk   <> 0
          AND fp.grade_sk      <> 0
          AND fp.nature_sk     <> 0
          AND fp.organisme_sk  <> 0
          AND dt.year_num      >  0
          AND fp.m_netpay IS NOT NULL
          AND fp.m_salbrut IS NOT NULL
    """
    with _conn() as conn:
        df = pd.read_sql(sql, conn, parse_dates=["month_start_date"])
    return df


def load_monthly_indemnity() -> pd.DataFrame:
    """
    Aggregate fact_indem by year+month.
    Used for: indemnity forecasting.
    """
    sql = """
        SELECT
            dt.year_num,
            dt.month_num,
            dt.month_start_date,
            COUNT(*)              AS employee_count,
            SUM(fi.m_netpay)      AS total_indemnity,
            AVG(fi.m_netpay)      AS avg_indemnity
        FROM dw.fact_indem fi
        JOIN dw.dim_temps dt ON dt.time_sk = fi.time_sk
        WHERE fi.employee_sk <> 0
          AND dt.year_num    >  0
          AND fi.m_netpay IS NOT NULL
        GROUP BY dt.year_num, dt.month_num, dt.month_start_date
        ORDER BY dt.year_num, dt.month_num
    """
    with _conn() as conn:
        df = pd.read_sql(sql, conn, parse_dates=["month_start_date"])
    df = df.sort_values("month_start_date").reset_index(drop=True)
    return df
