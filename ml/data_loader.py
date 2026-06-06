"""
ml/data_loader.py
=================
Loads data from PostgreSQL DW into pandas DataFrames for ML.
All queries filter out unknown members (sk=0) and year=0.
"""
from __future__ import annotations
import pandas as pd
import psycopg2
import psycopg2.extensions as _pext
from etl.core.config import DB_CONFIG

# Convert PostgreSQL NUMERIC/DECIMAL → Python float instead of decimal.Decimal.
# This prevents pandas from creating large object-dtype blocks during DataFrame
# construction, which causes fragmentation-OOM on named cursors with big chunks.
_DEC2FLOAT = _pext.new_type(
    _pext.DECIMAL.values,
    "DEC2FLOAT",
    lambda val, cur: float(val) if val is not None else None,
)
_pext.register_type(_DEC2FLOAT)


def _conn(timeout: bool = False):
    opts = "-c statement_timeout=0 -c work_mem=256MB"
    return psycopg2.connect(**DB_CONFIG, options=opts)


def load_monthly_payroll() -> pd.DataFrame:
    """
    Monthly payroll aggregates — reads from materialized view (sub-ms vs 70s on raw table).
    Used for: time series forecasting of total payroll.
    Returns one row per month.
    """
    sql = """
        SELECT
            year_num,
            month_num,
            month_start_date,
            employee_count,
            total_netpay,
            total_grosspay      AS total_salbrut,
            avg_netpay,
            total_deductions,
            total_cps,
            total_cpe
        FROM dw.mv_payroll_by_month
        ORDER BY year_num, month_num
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


def load_individual_payroll(sample_pct: float = 100.0) -> pd.DataFrame:
    """
    Individual-level payroll records for anomaly detection.

    Uses fp.codetab (backfilled for all 42M rows) instead of dim_organisme,
    so all ministries and etablissements are covered — not just the 3 that
    matched the old organisme.json lookup.

    sample_pct: 1–100. When 100 (default), no TABLESAMPLE — full dataset.
    """
    tablesample = f"TABLESAMPLE SYSTEM({sample_pct})" if sample_pct < 100.0 else ""
    sql = f"""
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
            COALESCE(fp.codetab, do2.codetab) AS ministry_code,
            COALESCE(de.libletabl, do2.liborgl) AS ministry_name_fr,
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
        FROM dw.fact_paie fp {tablesample}
        JOIN dw.dim_temps          dt  ON dt.time_sk      = fp.time_sk
        JOIN dw.dim_grade          dg  ON dg.grade_sk     = fp.grade_sk
        JOIN dw.dim_nature         dn  ON dn.nature_sk    = fp.nature_sk
        LEFT JOIN dw.dim_organisme do2 ON do2.organisme_sk = fp.organisme_sk
        LEFT JOIN dw.dim_etablissement de ON de.codetab   = fp.codetab
        WHERE fp.employee_sk <> 0
          AND fp.grade_sk    <> 0
          AND fp.nature_sk   <> 0
          AND dt.year_num    >  0
          AND fp.m_netpay   IS NOT NULL
          AND fp.m_salbrut  IS NOT NULL
    """
    _money = ["m_netpay","m_salbrut","m_salimp","m_retrait","m_cps","m_cpe",
              "m_capdeces","m_sub","m_avkm","m_avlog"]
    _cat   = ["grade_code","grade_label_fr","category","nature_code",
              "nature_label_fr","ministry_code","ministry_name_fr"]

    _money_set = set(_money)
    _cat_set   = set(_cat)

    def _rows_to_df(rows: list, col_names: list) -> pd.DataFrame:
        # Build column-by-column (avoids pandas block-consolidation OOM).
        col_data = list(zip(*rows))
        data = {}
        for i, col in enumerate(col_names):
            if col in _money_set:
                import numpy as np
                data[col] = np.array(col_data[i], dtype="float32")
            elif col in _cat_set:
                data[col] = pd.Categorical(col_data[i])
            else:
                data[col] = list(col_data[i])
        return pd.DataFrame(data)

    conn = _conn()
    try:
        with conn.cursor(name="individual_payroll_cur") as cur:
            cur.itersize = 500_000
            cur.execute(sql)
            first = cur.fetchmany(500_000)
            if not first:
                return pd.DataFrame()
            cols = [d[0] for d in cur.description]
            chunks = [_rows_to_df(first, cols)]
            del first
            while True:
                rows = cur.fetchmany(500_000)
                if not rows:
                    break
                chunks.append(_rows_to_df(rows, cols))
                del rows
        df = pd.concat(chunks, ignore_index=True)
        del chunks
    finally:
        conn.close()
    df["month_start_date"] = pd.to_datetime(df["month_start_date"], errors="coerce")
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
