"""
etl/generate_powerbi.py
=======================
Generates a Power BI Template (.pbit) from the live PostgreSQL DW.

Run:
    python -m etl.generate_powerbi

Output:
    powerbi/insaf_dw.pbit  <- double-click in Power BI Desktop
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
PG_HOST      = DB_CONFIG["host"]
PG_PORT      = str(DB_CONFIG["port"])
PG_DB        = DB_CONFIG["dbname"]
DW_SCHEMA    = "dw"

PG_TO_PBI = {
    "bigint":                      "int64",
    "integer":                     "int64",
    "smallint":                    "int64",
    "numeric":                     "decimal",
    "real":                        "double",
    "double precision":            "double",
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
    "appointment_date", "date_entry", "pa_type", "pa_eche", "pa_sitfam",
    "pa_loca_raw", "pa_sec", "pa_nbrfam", "pa_enfits", "pa_totinf",
    "pa_article", "pa_parag", "pa_mp", "pa_regcnr", "pa_indice",
    "gender", "is_unknown", "nature_flag", "is_taxable", "is_cnr",
    "zone", "insurance_code", "nature_type", "typstruct", "arg1", "arg2",
    "retire_age", "dq_issue_count",
}

# (fromTable, fromColumn, toTable, toColumn, isActive)
RELATIONSHIPS = [
    ("fact_paie",  "employee_sk",  "dim_employee",  "employee_sk",  True),
    ("fact_paie",  "time_sk",      "dim_temps",     "time_sk",      True),
    ("fact_paie",  "grade_sk",     "dim_grade",     "grade_sk",     True),
    ("fact_paie",  "nature_sk",    "dim_nature",    "nature_sk",    True),
    ("fact_paie",  "organisme_sk", "dim_organisme", "organisme_sk", True),
    ("fact_paie",  "region_sk",    "dim_region",    "region_sk",    True),
    ("fact_indem", "employee_sk",  "dim_employee",  "employee_sk",  False),
    ("fact_indem", "time_sk",      "dim_temps",     "time_sk",      False),
    ("fact_indem", "grade_sk",     "dim_grade",     "grade_sk",     False),
    ("fact_indem", "nature_sk",    "dim_nature",    "nature_sk",    False),
    ("fact_indem", "organisme_sk", "dim_organisme", "organisme_sk", False),
    ("fact_indem", "region_sk",    "dim_region",    "region_sk",    False),
    ("fact_indem", "indemnite_sk", "dim_indemnite", "indemnite_sk", True),
]

MEASURES = [
    ("Total Net Pay",            "SUM(fact_paie[m_netpay])",                                                                "#,##0.000",  "Payroll"),
    ("Total Gross Pay",          "SUM(fact_paie[m_salbrut])",                                                               "#,##0.000",  "Payroll"),
    ("Avg Net Pay",              "AVERAGEX(FILTER(fact_paie,fact_paie[m_netpay]<>BLANK()),fact_paie[m_netpay])",             "#,##0.000",  "Payroll"),
    ("Min Net Pay",              "MINX(FILTER(fact_paie,fact_paie[m_netpay]<>BLANK()),fact_paie[m_netpay])",                "#,##0.000",  "Payroll"),
    ("Max Net Pay",              "MAXX(FILTER(fact_paie,fact_paie[m_netpay]<>BLANK()),fact_paie[m_netpay])",                "#,##0.000",  "Payroll"),
    ("Employee Count",           "DISTINCTCOUNT(fact_paie[employee_sk])",                                                   "#,##0",      "Payroll"),
    ("Total Deductions",         "SUM(fact_paie[m_retrait])+SUM(fact_paie[m_cps])+SUM(fact_paie[m_cpe])",                  "#,##0.000",  "Payroll"),
    ("Net to Gross Ratio",       "DIVIDE(SUM(fact_paie[m_netpay]),SUM(fact_paie[m_salbrut]))",                              "0.00%",      "Payroll"),
    ("Total Indemnity",          "SUM(fact_indem[m_netpay])",                                                               "#,##0.000",  "Indemnities"),
    ("Indemnity Avg",            "AVERAGEX(FILTER(fact_indem,fact_indem[m_netpay]<>BLANK()),fact_indem[m_netpay])",         "#,##0.000",  "Indemnities"),
    ("Indemnity Employee Count", "DISTINCTCOUNT(fact_indem[employee_sk])",                                                  "#,##0",      "Indemnities"),
    ("Total Compensation",       "[Total Net Pay]+[Total Indemnity]",                                                       "#,##0.000",  "Combined"),
    ("Payroll Share",            "DIVIDE([Total Net Pay],[Total Compensation])",                                             "0.00%",      "Combined"),
    ("Indemnity Share",          "DIVIDE([Total Indemnity],[Total Compensation])",                                          "0.00%",      "Combined"),
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


# ── Model builders ─────────────────────────────────────────────────────────────

def _col(name: str, pg_type: str) -> dict:
    pbi_type  = PG_TO_PBI.get(pg_type, "string")
    summarize = "none" if (name in NO_SUMMARIZE or pbi_type not in ("decimal", "double", "int64")) else "sum"
    obj = {
        "name":         name,
        "dataType":     pbi_type,
        "lineageTag":   str(uuid.uuid4()),
        "summarizeBy":  summarize,
        "sourceColumn": name,
        "annotations":  [{"name": "SummarizationSetBy", "value": "Automatic"}],
    }
    if name in HIDDEN_COLS:
        obj["isHidden"] = True
    return obj


def _table(name: str, cols: list[tuple[str, str]]) -> dict:
    expression = (
        f'let\n'
        f'    Source = PostgreSQL.Database("{PG_HOST}:{PG_PORT}", "{PG_DB}"),\n'
        f'    dw_schema = Source{{[Schema="{DW_SCHEMA}",Item="{name}"]}}[Data]\n'
        f'in\n'
        f'    dw_schema'
    )
    return {
        "name":       name,
        "lineageTag": str(uuid.uuid4()),
        "columns":    [_col(c, t) for c, t in cols],
        "partitions": [{
            "name":   name,
            "source": {"type": "m", "expression": expression},
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
        "columns":    [],
        "measures":   measures,
        "partitions": [{
            "name":   "_Measures",
            "source": {"type": "m", "expression": "let\n    Source = #table({},{})\nin\n    Source"},
        }],
        "annotations": [{"name": "PBI_ResultType", "value": "Table"}],
    }


def _build_model(schema: dict) -> dict:
    tables = [_table(t, schema[t]) for t in TABLES if schema.get(t)]
    tables.append(_measures_table())

    relationships = []
    for ft, fc, tt, tc, active in RELATIONSHIPS:
        rel = {
            "name":       str(uuid.uuid4()),
            "fromTable":  ft,
            "fromColumn": fc,
            "toTable":    tt,
            "toColumn":   tc,
            "isActive":   active,
        }
        relationships.append(rel)

    return {
        "name":               "Model",
        "compatibilityLevel": 1500,
        "model": {
            "culture": "en-US",
            "dataAccessOptions": {
                "legacyRedirects":         True,
                "returnErrorValuesAsNull": True,
            },
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "sourceQueryCulture": "en-US",
            "tables":        tables,
            "relationships": relationships,
            "annotations": [
                {"name": "__PBI_TimeIntelligenceEnabled", "value": "1"},
                {"name": "PBIDesktopVersion", "value": "2.128.0.0"},
            ],
        },
    }


# ── .pbit writer ───────────────────────────────────────────────────────────────

CONTENT_TYPES_XML = (
    '<?xml version="1.0" encoding="utf-8"?>'
    '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
    '<Default Extension="json" ContentType="application/json"/>'
    '<Override PartName="/DataModelSchema" ContentType="application/json"/>'
    '<Override PartName="/Report/Layout" ContentType="application/json"/>'
    '<Override PartName="/Settings" ContentType="application/json"/>'
    '<Override PartName="/Metadata" ContentType="application/json"/>'
    '<Override PartName="/Version" ContentType="application/octet-stream"/>'
    '<Override PartName="/SecurityBindings" ContentType="application/octet-stream"/>'
    '</Types>'
)

REPORT_LAYOUT = {
    "id": 0,
    "resourcePackages": [],
    "sections": [{
        "id":               0,
        "name":             "ReportSection",
        "displayName":      "Page 1",
        "filters":          "[]",
        "ordinal":          0,
        "visualContainers": [],
        "config":           json.dumps({"relationships": []}),
    }],
    "config":             "{}",
    "layoutOptimization": 0,
}

SETTINGS = {
    "useNewFilterExperience":                   True,
    "allowChangeFilterTypes":                   True,
    "useStylableVisualContainerHeader":         True,
    "exportDataMode":                           1,
    "hideVisualContainerHeader":                False,
    "useEnhancedTooltips":                      False,
    "optOutNewOnOffVisualHeader":               False,
    "enableDeveloperMode":                      False,
    "isPaginatedReportWebViewEnabled":          False,
    "isCrossHighlightingDisabled":              False,
    "isEntirePageContextMenuEnabled":           False,
}

METADATA = {
    "version":   "4.0",
    "culture":   "en-US",
    "created":   "2026-01-01T00:00:00",
    "modified":  "2026-01-01T00:00:00",
}


def _utf16(text: str) -> bytes:
    """Encode string as UTF-16 LE with BOM — required for ALL text files in .pbit."""
    return b"\xff\xfe" + text.encode("utf-16-le")


def write_pbit(model: dict) -> None:
    PBIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # [Content_Types].xml stays UTF-8 (it's XML, not JSON)
        zf.writestr("[Content_Types].xml", CONTENT_TYPES_XML.encode("utf-8"))
        zf.writestr("Version",          "3.0".encode("utf-16-le"))  # UTF-16 LE, no BOM
        zf.writestr("Settings",         _utf16(json.dumps(SETTINGS,      ensure_ascii=False)))
        zf.writestr("Metadata",         _utf16(json.dumps(METADATA,      ensure_ascii=False)))
        zf.writestr("SecurityBindings", b"")
        zf.writestr("DataModelSchema",  _utf16(json.dumps(model,         ensure_ascii=False)))
        zf.writestr("Report/Layout",    _utf16(json.dumps(REPORT_LAYOUT, ensure_ascii=False)))
    PBIT_PATH.write_bytes(buf.getvalue())


# ── Entry point ────────────────────────────────────────────────────────────────

def generate() -> None:
    print("Fetching schema from PostgreSQL...")
    schema = fetch_schema()

    print("Building tabular model...")
    model = _build_model(schema)

    print(f"Writing {PBIT_PATH.name}...")
    write_pbit(model)

    size_kb = PBIT_PATH.stat().st_size // 1024
    print(f"\nDone! ({size_kb} KB)")
    print(f"File: {PBIT_PATH}")
    print(f"\n  {len(TABLES)} tables")
    print(f"  {len(RELATIONSHIPS)} relationships (6 active for fact_paie, 1 active for fact_indem->dim_indemnite,")
    print(f"   6 inactive for fact_indem->shared dims — use USERELATIONSHIP() in DAX to activate)")
    print(f"  {len(MEASURES)} DAX measures")


if __name__ == "__main__":
    generate()
