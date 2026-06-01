import sys, psycopg2
sys.path.insert(0, ".")
from etl.core.config import DB_CONFIG

conn = psycopg2.connect(**DB_CONFIG)
cur  = conn.cursor()

# tables in dw schema
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='dw' ORDER BY table_name")
tables = [r[0] for r in cur.fetchall()]
print("dw schema tables:", tables)

# employee count over time
cur.execute("SELECT COUNT(*) FROM dw.fact_paie")
total_records = cur.fetchone()[0]
print(f"\nTotal records in fact_paie: {total_records:,}")

cur.execute("SELECT COUNT(DISTINCT employee_sk) FROM dw.fact_paie WHERE employee_sk <> 0")
total_emp = cur.fetchone()[0]
print(f"Total distinct employees (all time): {total_emp:,}")

cur.execute("""
    SELECT dt.year_num,
           COUNT(DISTINCT fp.employee_sk) AS employees,
           COUNT(*) AS records
    FROM dw.fact_paie fp
    JOIN dw.dim_temps dt ON dt.time_sk = fp.time_sk
    WHERE fp.employee_sk <> 0
    GROUP BY dt.year_num
    ORDER BY dt.year_num DESC
    LIMIT 8
""")
print("\nLast 8 years (employee count / records):")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]:>8,} employees  |  {r[2]:>10,} records")

conn.close()
