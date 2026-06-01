import sys
sys.stdout.reconfigure(line_buffering=True)
print("step 1: importing psycopg2", flush=True)
import psycopg2
print("step 2: importing config", flush=True)
from etl.core.config import DB_CONFIG
print("step 3: connecting...", flush=True)
conn = psycopg2.connect(**DB_CONFIG)
print("step 4: connected!", flush=True)
with conn.cursor() as cur:
    cur.execute("SELECT reltuples::bigint FROM pg_class WHERE relname='fact_paie'")
    print(f"fact_paie approx rows: {cur.fetchone()[0]:,}", flush=True)
conn.close()
print("done", flush=True)
