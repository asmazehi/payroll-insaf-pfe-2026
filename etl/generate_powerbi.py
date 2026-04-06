"""
etl/generate_powerbi.py
=======================
Generates a Power BI Project (.pbip) directly from the live PostgreSQL DW schema.
Requires Power BI Desktop 2.117+ (mid-2023 or later).

Run:
    python -m etl.generate_powerbi

Output:
    powerbi/insaf_dw.pbip           ← double-click this in Power BI Desktop
    powerbi/insaf_dw.Dataset/       ← data model: all tables, relationships, measures
    powerbi/insaf_dw.Report/        ← blank report ready to build
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path

import psycopg2

from etl.core.config import DB_CONFIG

# ── Config ─────────────────────────────────────────────────────────────────────
POWERBI_DIR  = Path(__file__).resolve().parent.parent / "powerbi"
PROJECT_NAME = "insaf_dw"
PG_HOST      = f"{DB_CONFIG['host']}:{DB_CONFIG['port']}"
PG_DB        = DB_CONFIG["dbname"]
DW_SCHEMA    = "dw"

# ── PostgreSQL -> TMDL data type map ────────────────────────────────────────────
PG_TO_TMDL = {
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

# ── Tables in load order ────────────────────────────────────────────────────────
TABLES = [
    "dim_employee", "dim_grade", "dim_nature", "dim_organisme",
    "dim_region", "dim_temps", "dim_indemnite",
    "fact_paie", "fact_indem",
]

# Columns hidden from the report canvas (still available via formulas)
HIDDEN = {
    # Surrogate keys — used for joins only
    "employee_sk", "grade_sk", "nature_sk", "organisme_sk",
    "region_sk", "time_sk", "indemnite_sk",
    # Internal flags
    "is_unknown", "dw_load_ts", "load_ts", "run_id", "source_file",
    # DQ flags — hide by default, analysts can unhide if needed
    "dq_grade_matched", "dq_nature_matched", "dq_org_matched",
    "dq_region_matched", "dq_has_issues", "dq_issue_count",
    "dq_grade_method", "dq_nature_method", "dq_org_method", "dq_region_method",
}

# Columns where Power BI should NOT auto-sum (codes, keys, dates, flags)
NO_SUMMARIZE = {
    "employee_sk", "grade_sk", "nature_sk", "organisme_sk",
    "region_sk", "time_sk", "indemnite_sk",
    "employee_id", "grade_code", "nature_code", "indemnite_code",
    "codetab", "cab", "sg", "dg", "dire", "sdir", "serv", "unite",
    "coddep", "codreg", "code_dept", "code_region", "codgouv", "deleg",
    "year_num", "month_num", "quarter_num", "semester_num",
    "year_month", "month_start_date",
    "birth_date", "hire_date", "appointment_date", "date_entry",
    "pa_type", "pa_eche", "pa_sitfam", "pa_loca_raw", "pa_sec",
    "pa_nbrfam", "pa_enfits", "pa_totinf", "pa_article", "pa_parag",
    "pa_mp", "pa_regcnr", "pa_indice",
    "gender", "is_unknown", "nature_flag", "is_taxable", "is_cnr",
    "zone", "insurance_code", "nature_type", "typstruct", "deleg",
    "dq_issue_count", "arg1", "arg2", "retire_age",
}

# ── Relationships (fromTable, fromCol, toTable, toCol) ──────────────────────────
RELATIONSHIPS = [
    ("fact_paie",  "employee_sk",  "dim_employee",  "employee_sk"),
    ("fact_paie",  "time_sk",      "dim_temps",     "time_sk"),
    ("fact_paie",  "grade_sk",     "dim_grade",     "grade_sk"),
    ("fact_paie",  "nature_sk",    "dim_nature",    "nature_sk"),
    ("fact_paie",  "organisme_sk", "dim_organisme", "organisme_sk"),
    ("fact_paie",  "region_sk",    "dim_region",    "region_sk"),
    ("fact_indem", "employee_sk",  "dim_employee",  "employee_sk"),
    ("fact_indem", "time_sk",      "dim_temps",     "time_sk"),
    ("fact_indem", "grade_sk",     "dim_grade",     "grade_sk"),
    ("fact_indem", "nature_sk",    "dim_nature",    "nature_sk"),
    ("fact_indem", "organisme_sk", "dim_organisme", "organisme_sk"),
    ("fact_indem", "region_sk",    "dim_region",    "region_sk"),
    ("fact_indem", "indemnite_sk", "dim_indemnite", "indemnite_sk"),
]

# ── DAX Measures (name, expression, format_string, display_folder) ──────────────
MEASURES = [
    # ── Payroll ──
    ("Total Net Pay",
     "SUM(fact_paie[m_netpay])",
     "#,##0.000",
     "Payroll"),

    ("Total Gross Pay",
     "SUM(fact_paie[m_salbrut])",
     "#,##0.000",
     "Payroll"),

    ("Avg Net Pay",
     "AVERAGEX(FILTER(fact_paie, fact_paie[m_netpay] <> BLANK()), fact_paie[m_netpay])",
     "#,##0.000",
     "Payroll"),

    ("Min Net Pay",
     "MINX(FILTER(fact_paie, fact_paie[m_netpay] <> BLANK()), fact_paie[m_netpay])",
     "#,##0.000",
     "Payroll"),

    ("Max Net Pay",
     "MAXX(FILTER(fact_paie, fact_paie[m_netpay] <> BLANK()), fact_paie[m_netpay])",
     "#,##0.000",
     "Payroll"),

    ("Employee Count",
     "DISTINCTCOUNT(fact_paie[employee_sk])",
     "#,##0",
     "Payroll"),

    ("Total Deductions",
     "SUM(fact_paie[m_retrait]) + SUM(fact_paie[m_cps]) + SUM(fact_paie[m_cpe])",
     "#,##0.000",
     "Payroll"),

    ("Total Taxable Salary",
     "SUM(fact_paie[m_salimp])",
     "#,##0.000",
     "Payroll"),

    ("Net to Gross Ratio",
     "DIVIDE(SUM(fact_paie[m_netpay]), SUM(fact_paie[m_salbrut]))",
     "0.00%",
     "Payroll"),

    # ── Indemnities ──
    ("Total Indemnity",
     "SUM(fact_indem[m_netpay])",
     "#,##0.000",
     "Indemnities"),

    ("Indemnity Employee Count",
     "DISTINCTCOUNT(fact_indem[employee_sk])",
     "#,##0",
     "Indemnities"),

    ("Avg Indemnity",
     "AVERAGEX(FILTER(fact_indem, fact_indem[m_netpay] <> BLANK()), fact_indem[m_netpay])",
     "#,##0.000",
     "Indemnities"),

    # ── Combined ──
    ("Total Compensation",
     "[Total Net Pay] + [Total Indemnity]",
     "#,##0.000",
     "Combined"),

    ("Payroll Share",
     "DIVIDE([Total Net Pay], [Total Compensation])",
     "0.00%",
     "Combined"),

    ("Indemnity Share",
     "DIVIDE([Total Indemnity], [Total Compensation])",
     "0.00%",
     "Combined"),
]


# ── Schema fetcher ─────────────────────────────────────────────────────────────

def fetch_schema() -> dict[str, list[tuple[str, str]]]:
    """Returns {table_name: [(column_name, pg_type), ...]} for all DW tables."""
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


# ── TMDL generators ────────────────────────────────────────────────────────────

def _ltag() -> str:
    return str(uuid.uuid4())


def _column_tmdl(col: str, pg_type: str) -> str:
    tmdl_type = PG_TO_TMDL.get(pg_type, "string")
    hidden     = col in HIDDEN
    summarize  = "none" if col in NO_SUMMARIZE or tmdl_type not in ("decimal","double","int64") else "sum"
    is_key     = col.endswith("_sk")

    lines = [f"\tcolumn {col}"]
    lines.append(f"\t\tdataType: {tmdl_type}")
    if hidden:
        lines.append(f"\t\tisHidden")
    if is_key:
        lines.append(f"\t\tisKey")
    lines.append(f"\t\tlineageTag: {_ltag()}")
    lines.append(f"\t\tsummarizeBy: {summarize}")
    lines.append(f"\t\tsourceColumn: {col}")
    lines.append("")
    lines.append(f"\t\tannotation SummarizationSetBy = Automatic")
    lines.append("")
    return "\n".join(lines)


def _table_tmdl(table: str, columns: list[tuple[str, str]]) -> str:
    m_query = (
        f"let\n"
        f"\t\t\t    Source = PostgreSQL.Database(\"{PG_HOST}\", \"{PG_DB}\"),\n"
        f"\t\t\t    Result = Source{{[Schema=\"{DW_SCHEMA}\", Item=\"{table}\"]}}[Data]\n"
        f"\t\t\tin\n"
        f"\t\t\t    Result"
    )

    lines = [
        f"table {table}",
        f"\tlineageTag: {_ltag()}",
        f"",
    ]

    for col, pg_type in columns:
        lines.append(_column_tmdl(col, pg_type))

    lines += [
        f"\tpartition {table} = m",
        f"\t\tmode: import",
        f"\t\tsource",
        f"\t\t\t```",
        f"\t\t\t{m_query}",
        f"\t\t\t```",
        f"",
        f"\tannotation PBI_ResultType = Table",
        f"",
    ]

    return "\n".join(lines)


def _measures_tmdl() -> str:
    lines = [
        "table _Measures",
        f"\tlineageTag: {_ltag()}",
        "",
    ]

    for name, expr, fmt, folder in MEASURES:
        lines += [
            f"\tmeasure '{name}' = {expr}",
            f"\t\tformatString: {fmt}",
            f"\t\tlineageTag: {_ltag()}",
            f"\t\tdisplayFolder: {folder}",
            "",
        ]

    lines += [
        "\tannotation PBI_ResultType = Table",
        "",
    ]
    return "\n".join(lines)


def _relationships_tmdl() -> str:
    lines = []
    for from_tbl, from_col, to_tbl, to_col in RELATIONSHIPS:
        lines += [
            f"relationship {_ltag()}",
            f"\tfromTable: {from_tbl}",
            f"\tfromColumn: {from_col}",
            f"\ttoTable: {to_tbl}",
            f"\ttoColumn: {to_col}",
            "",
        ]
    return "\n".join(lines)


def _model_tmdl() -> str:
    return (
        "model Model\n"
        f"\tlineageTag: {_ltag()}\n"
        "\tculture: en-US\n"
        "\n"
        "\tannotation PBIDesktopVersion = 2.128.751.0\n"
    )


def _database_tmdl() -> str:
    return (
        f"database {PROJECT_NAME}\n"
        "\tcompatibilityLevel: 1567\n"
    )


# ── File writers ───────────────────────────────────────────────────────────────

def _write(path: Path, content: str | dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(content, dict):
        path.write_text(json.dumps(content, indent=2, ensure_ascii=False), encoding="utf-8")
    else:
        path.write_text(content, encoding="utf-8")
    print(f"  wrote {path.relative_to(POWERBI_DIR)}")


def generate() -> None:
    print(f"Fetching schema from PostgreSQL ({PG_DB})...")
    schema = fetch_schema()

    ds_dir = POWERBI_DIR / f"{PROJECT_NAME}.Dataset"
    rp_dir = POWERBI_DIR / f"{PROJECT_NAME}.Report"
    def_dir = ds_dir / "definition"
    tbl_dir = def_dir / "tables"

    print(f"\nGenerating Power BI Project -> {POWERBI_DIR.name}/")

    # ── Root .pbip file ──────────────────────────────────────────────────────
    _write(POWERBI_DIR / f"{PROJECT_NAME}.pbip", {
        "version": "1.0",
        "artifacts": [{"report": {"path": f"{PROJECT_NAME}.Report"}}],
        "settings": {"enableTmdlSchemaChange": True},
    })

    # ── Dataset .platform ────────────────────────────────────────────────────
    _write(ds_dir / ".platform", {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "Dataset", "displayName": PROJECT_NAME},
        "config": {"version": "2.0", "logicalId": str(uuid.uuid4())},
    })

    # ── definition.pbidataset ────────────────────────────────────────────────
    _write(ds_dir / "definition.pbidataset", {"version": "1.0", "settings": {}})

    # ── database.tmdl + model.tmdl ───────────────────────────────────────────
    _write(def_dir / "database.tmdl", _database_tmdl())
    _write(def_dir / "model.tmdl",    _model_tmdl())

    # ── Table TMDL files ─────────────────────────────────────────────────────
    for table in TABLES:
        cols = schema.get(table, [])
        if not cols:
            print(f"  WARNING: no columns found for {table} — skipping")
            continue
        _write(tbl_dir / f"{table}.tmdl", _table_tmdl(table, cols))

    # ── Measures table ───────────────────────────────────────────────────────
    _write(tbl_dir / "_Measures.tmdl", _measures_tmdl())

    # ── Relationships ────────────────────────────────────────────────────────
    _write(def_dir / "relationships.tmdl", _relationships_tmdl())

    # ── Report .platform ─────────────────────────────────────────────────────
    _write(rp_dir / ".platform", {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
        "metadata": {"type": "Report", "displayName": PROJECT_NAME},
        "config": {"version": "2.0", "logicalId": str(uuid.uuid4())},
    })

    # ── definition.pbir ──────────────────────────────────────────────────────
    _write(rp_dir / "definition.pbir", {
        "version": "1.0",
        "datasetReference": {"byPath": {"path": f"../{PROJECT_NAME}.Dataset"}},
    })

    print(f"\nDone. Open this file in Power BI Desktop:")
    print(f"  {POWERBI_DIR / PROJECT_NAME}.pbip")
    print()
    print("Relationships set up (13 total):")
    for ft, fc, tt, tc in RELATIONSHIPS:
        print(f"  {ft}.{fc} -> {tt}.{tc}")
    print()
    print(f"DAX measures ready ({len(MEASURES)} total):")
    for name, _, _, folder in MEASURES:
        print(f"  [{folder}] {name}")


if __name__ == "__main__":
    generate()
