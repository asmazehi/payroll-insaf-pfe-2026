"""
etl/generate_powerbi.py
=======================
Generates a Power BI Template (.pbit) from the live PostgreSQL DW.
Works with ALL versions of Power BI Desktop.

Run:
    python -m etl.generate_powerbi

Output:
    powerbi/insaf_dw.pbit  <- double-click this in Power BI Desktop
"""
from __future__ import annotations

import io
import json
import uuid
import zipfile
from pathlib import Path

import psycopg2
from etl.core.config import DB_CONFIG

# ── Config ─────────────────────────────────────────────────────────────────────
POWERBI_DIR  = Path(__file__).resolve().parent.parent / "powerbi"
PBIT_PATH    = POWERBI_DIR / "insaf_dw.pbit"
PG_HOST      = f"{DB_CONFIG['host']}:{DB_CONFIG['port']}"
PG_DB        = DB_CONFIG["dbname"]
DW_SCHEMA    = "dw"

# ── PostgreSQL type -> Power BI dataType ───────────────────────────────────────
PG_TO_PBI = {
    "bigint":                      "int64",
    "integer":                     "int64",
    "smallint":                    "int64",
    "numeric":                     "decimal",
    "real":                        "double",
    "double precision":             "double",
    "text":                        "string",
    "character varying":           "string",
    "character":                   "string",
    "boolean":                     "boolean",
    "date":                        "dateTime",
    "timestamp without time zone": "dateTime",
    "timestamp with time zone":    "dateTime",
}

TABLES = [
    "dim_employee", "dim_grade", "dim_nature", "dim_organisme",
    "dim_region", "dim_temps", "dim_indemnite",
    "fact_paie", "fact_indem",
]

HIDDEN_COLS = {
    "employee_sk", "grade_sk", "nature_sk", "organisme_sk",
    "region_sk", "time_sk", "indemnite_sk",
    "is_unknown", "dw_load_ts", "load_ts", "run_id", "source_file",
    "dq_grade_matched", "dq_nature_matched", "dq_org_matched",
    "dq_region_matched", "dq_has_issues", "dq_issue_count",
}

NO_SUMMARIZE = {
    "employee_sk", "grade_sk", "nature_sk", "organisme_sk",
    "region_sk", "time_sk", "indemnite_sk",
    "employee_id", "grade_code", "nature_code", "indemnite_code",
    "codetab", "cab", "sg", "dg", "dire", "sdir", "serv", "unite",
    "coddep", "codreg", "code_dept", "code_region", "codgouv", "deleg",
    "year_num", "month_num", "quarter_num", "semester_num",
    "year_month", "month_start_date", "birth_date", "hire_date",
    "appointment_date", "date_entry",
    "pa_type", "pa_eche", "pa_sitfam", "pa_loca_raw", "pa_sec",
    "pa_nbrfam", "pa_enfits", "pa_totinf", "pa_article", "pa_parag",
    "pa_mp", "pa_regcnr", "pa_indice", "gender", "is_unknown",
    "nature_flag", "is_taxable", "is_cnr", "zone", "insurance_code",
    "nature_type", "typstruct", "arg1", "arg2", "retire_age",
    "dq_issue_count",
}

RELATIONSHIPS = [
    # fact_paie -> dims  (6 relationships)
    ("fact_paie",  "employee_sk",  "dim_employee",  "employee_sk"),
    ("fact_paie",  "time_sk",      "dim_temps",     "time_sk"),
    ("fact_paie",  "grade_sk",     "dim_grade",     "grade_sk"),
    ("fact_paie",  "nature_sk",    "dim_nature",    "nature_sk"),
    ("fact_paie",  "organisme_sk", "dim_organisme", "organisme_sk"),
    ("fact_paie",  "region_sk",    "dim_region",    "region_sk"),
    # fact_indem -> same shared dims  (6 relationships, inactive where conflict)
    ("fact_indem", "employee_sk",  "dim_employee",  "employee_sk"),
    ("fact_indem", "time_sk",      "dim_temps",     "time_sk"),
    ("fact_indem", "grade_sk",     "dim_grade",     "grade_sk"),
    ("fact_indem", "nature_sk",    "dim_nature",    "nature_sk"),
    ("fact_indem", "organisme_sk", "dim_organisme", "organisme_sk"),
    ("fact_indem", "region_sk",    "dim_region",    "region_sk"),
    # fact_indem -> dim_indemnite  (exclusive to DW2)
    ("fact_indem", "indemnite_sk", "dim_indemnite", "indemnite_sk"),
]

# fact_indem relationships to shared dims must be inactive
# (Power BI allows only one active path between two tables)
INACTIVE_RELS = {
    ("fact_indem", "employee_sk",  "dim_employee",  "employee_sk"),
    ("fact_indem", "time_sk",      "dim_temps",     "time_sk"),
    ("fact_indem", "grade_sk",     "dim_grade",     "grade_sk"),
    ("fact_indem", "nature_sk",    "dim_nature",    "nature_sk"),
    ("fact_indem", "organisme_sk", "dim_organisme", "organisme_sk"),
    ("fact_indem", "region_sk",    "dim_region",    "region_sk"),
}

MEASURES = [
    # Payroll (DW1)
    ("Total Net Pay",        "SUM(fact_paie[m_netpay])",    "#,##0.000 TND", "Payroll"),
    ("Total Gross Pay",      "SUM(fact_paie[m_salbrut])",   "#,##0.000 TND", "Payroll"),
    ("Avg Net Pay",          "AVERAGEX(FILTER(fact_paie, fact_paie[m_netpay] <> BLANK()), fact_paie[m_netpay])", "#,##0.000 TND", "Payroll"),
    ("Min Net Pay",          "MINX(FILTER(fact_paie, fact_paie[m_netpay] <> BLANK()), fact_paie[m_netpay])",    "#,##0.000 TND", "Payroll"),
    ("Max Net Pay",          "MAXX(FILTER(fact_paie, fact_paie[m_netpay] <> BLANK()), fact_paie[m_netpay])",    "#,##0.000 TND", "Payroll"),
    ("Employee Count",       "DISTINCTCOUNT(fact_paie[employee_sk])",                                           "#,##0",         "Payroll"),
    ("Total Deductions",     "SUM(fact_paie[m_retrait]) + SUM(fact_paie[m_cps]) + SUM(fact_paie[m_cpe])",      "#,##0.000 TND", "Payroll"),
    ("Total Taxable Salary", "SUM(fact_paie[m_salimp])",                                                        "#,##0.000 TND", "Payroll"),
    ("Net to Gross Ratio",   "DIVIDE(SUM(fact_paie[m_netpay]), SUM(fact_paie[m_salbrut]))",                     "0.00%",         "Payroll"),
    # Indemnities (DW2)
    ("Total Indemnity",       "SUM(fact_indem[m_netpay])",                                                                                  "#,##0.000 TND", "Indemnities"),
    ("Indemnity Avg",         "AVERAGEX(FILTER(fact_indem, fact_indem[m_netpay] <> BLANK()), fact_indem[m_netpay])",                         "#,##0.000 TND", "Indemnities"),
    ("Indemnity Employee Count", "DISTINCTCOUNT(fact_indem[employee_sk])",                                                                   "#,##0",         "Indemnities"),
    # Combined
    ("Total Compensation",   "[Total Net Pay] + [Total Indemnity]",                "#,##0.000 TND", "Combined"),
    ("Payroll Share",        "DIVIDE([Total Net Pay], [Total Compensation])",       "0.00%",         "Combined"),
    ("Indemnity Share",      "DIVIDE([Total Indemnity], [Total Compensation])",     "0.00%",         "Combined"),
]


# ── Schema ─────────────────────────────────────────────────────────────────────

def fetch_schema() -> dict[str, list[tuple[str, str]]]:
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()
    cur.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = ANY(%s)
        ORDER BY table_name, ordinal_position
    """, (DW_SCHEMA, TABLES))
    schema: dict[str, list] = {t: [] for t in TABLES}
    for tbl, col, dtype in cur.fetchall():
        schema[tbl].append((col, dtype))
    conn.close()
    return schema


# ── DataModelSchema builders ───────────────────────────────────────────────────

def _col(name: str, pg_type: str) -> dict:
    pbi_type  = PG_TO_PBI.get(pg_type, "string")
    summarize = "none" if (name in NO_SUMMARIZE or pbi_type not in ("decimal","double","int64")) else "sum"
    c = {
        "name":          name,
        "dataType":      pbi_type,
        "lineageTag":    str(uuid.uuid4()),
        "summarizeBy":   summarize,
        "sourceColumn":  name,
        "annotations":   [{"name": "SummarizationSetBy", "value": "Automatic"}],
    }
    if name in HIDDEN_COLS:
        c["isHidden"] = True
    return c


def _m_query(table: str) -> list[str]:
    return [
        f'let',
        f'    Source = PostgreSQL.Database("{PG_HOST}", "{PG_DB}"),',
        f'    Result = Source{{[Schema="{DW_SCHEMA}", Item="{table}"]}}[Data]',
        f'in',
        f'    Result',
    ]


def _table(name: str, cols: list[tuple[str, str]]) -> dict:
    return {
        "name":       name,
        "lineageTag": str(uuid.uuid4()),
        "columns":    [_col(c, t) for c, t in cols],
        "partitions": [{
            "name":     name,
            "dataView": "full",
            "source":   {"type": "m", "expression": _m_query(name)},
        }],
        "annotations": [{"name": "PBI_ResultType", "value": "Table"}],
    }


def _measures_table() -> dict:
    measures = []
    for mname, expr, fmt, folder in MEASURES:
        measures.append({
            "name":          mname,
            "expression":    expr,
            "formatString":  fmt,
            "lineageTag":    str(uuid.uuid4()),
            "displayFolder": folder,
        })
    return {
        "name":       "_Measures",
        "lineageTag": str(uuid.uuid4()),
        "columns": [{
            "name":         "_dummy",
            "dataType":     "string",
            "isHidden":     True,
            "lineageTag":   str(uuid.uuid4()),
            "summarizeBy":  "none",
            "sourceColumn": "_dummy",
        }],
        "measures": measures,
        "partitions": [{
            "name":     "_Measures",
            "dataView": "full",
            "source":   {"type": "calculated", "expression": 'Row("_dummy", BLANK())'},
        }],
    }


def _relationship(from_tbl, from_col, to_tbl, to_col) -> dict:
    is_active = (from_tbl, from_col, to_tbl, to_col) not in INACTIVE_RELS
    return {
        "name":       str(uuid.uuid4()),
        "fromTable":  from_tbl,
        "fromColumn": from_col,
        "toTable":    to_tbl,
        "toColumn":   to_col,
        "isActive":   is_active,
    }


def build_model(schema: dict) -> dict:
    tables = [_table(t, schema[t]) for t in TABLES if schema.get(t)]
    tables.append(_measures_table())

    relationships = [_relationship(*r) for r in RELATIONSHIPS]

    return {
        "name": "Model",
        "compatibilityLevel": 1550,
        "model": {
            "culture": "en-US",
            "dataAccessOptions": {
                "legacyRedirects":        True,
                "returnErrorValuesAsNull": True,
            },
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "sourceQueryCulture": "en-US",
            "tables":        tables,
            "relationships": relationships,
            "annotations": [
                {"name": "PBI_QueryOrder", "value": json.dumps(TABLES + ["_Measures"])},
                {"name": "__PBI_TimeIntelligenceEnabled", "value": "1"},
            ],
        },
    }


# ── .pbit ZIP builder ──────────────────────────────────────────────────────────

CONTENT_TYPES = """<?xml version="1.0" encoding="utf-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="json" ContentType="application/json"/>
  <Default Extension="xml"  ContentType="application/xml"/>
</Types>"""

REPORT_LAYOUT = json.dumps({
    "id": 0,
    "resourcePackages": [],
    "sections": [{
        "id":           0,
        "name":         "ReportSection",
        "displayName":  "Page 1",
        "filters":      "[]",
        "ordinal":      0,
        "visualContainers": [],
        "config":       json.dumps({"relationships": []}),
    }],
    "config":              "{}",
    "layoutOptimization":  0,
})


def write_pbit(model: dict, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml",   CONTENT_TYPES)
        zf.writestr("Version",               "2.0")
        zf.writestr("Settings",              "{}")
        zf.writestr("Metadata",              "{}")
        zf.writestr("SecurityBindings",      "")
        zf.writestr("DataModelSchema",       json.dumps(model, ensure_ascii=False))
        zf.writestr("Report/Layout",         REPORT_LAYOUT)
    out_path.write_bytes(buf.getvalue())


# ── Entry point ────────────────────────────────────────────────────────────────

def generate() -> None:
    print("Fetching schema from PostgreSQL...")
    schema = fetch_schema()

    print("Building data model...")
    model = build_model(schema)

    print(f"Writing {PBIT_PATH.name}...")
    write_pbit(model, PBIT_PATH)

    print(f"\nDone. Open in Power BI Desktop:")
    print(f"  {PBIT_PATH}")
    print(f"\n{len(TABLES)} tables, {len(RELATIONSHIPS)} relationships, {len(MEASURES)} DAX measures")


if __name__ == "__main__":
    generate()
