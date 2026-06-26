import os
import sys
import psycopg2
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# .env faylini yuklaymiz
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path)

DB_HOST = os.getenv("EXPORT_DB_HOST", "localhost")
DB_PORT = os.getenv("EXPORT_DB_PORT", "5435")
DB_NAME = os.getenv("EXPORT_DB_NAME", "smartup_clean")
DB_USER = os.getenv("EXPORT_DB_USER", "postgres")
DB_PASSWORD = os.getenv("EXPORT_DB_PASSWORD")

EXCEL_LIMIT = 1_048_576
OUTPUT_DIR = Path(__file__).parent / "excel_exports"

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=int(DB_PORT),
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )

def get_all_tables(conn):
    query = """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

def get_row_count(conn, schema, table):
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}";')
        return cur.fetchone()[0]

def export_table(conn, schema, table, output_dir):
    row_count = get_row_count(conn, schema, table)

    if row_count > EXCEL_LIMIT:
        print(f"  [OGOHLANTIRISH] {schema}.{table}: {row_count:,} qator — "
              f"Excel limiti ({EXCEL_LIMIT:,}) dan oshadi! Faqat birinchi {EXCEL_LIMIT:,} qator yoziladi.")
        limit_clause = f"LIMIT {EXCEL_LIMIT}"
    else:
        limit_clause = ""

    query = f'SELECT * FROM "{schema}"."{table}" {limit_clause};'
    df = pd.read_sql_query(query, conn)

    prefix = f"{schema}_" if schema != "public" else ""
    filename = output_dir / f"{prefix}{table}.xlsx"

    df.to_excel(filename, index=False, engine="openpyxl")
    written = min(row_count, EXCEL_LIMIT)
    print(f"  [OK] {schema}.{table}: {row_count:,} qator -> {filename.name}")
    return written

def main():
    print(f"Ulanish: {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}\n")

    try:
        conn = get_connection()
    except Exception as e:
        print(f"[XATO] DB ga ulanib bo'lmadi: {e}")
        sys.exit(1)

    OUTPUT_DIR.mkdir(exist_ok=True)

    tables = get_all_tables(conn)
    if not tables:
        print("Hech qanday jadval topilmadi.")
        conn.close()
        return

    print(f"Topilgan jadvallar soni: {len(tables)}\n")
    print("=" * 60)

    total_rows = 0
    for schema, table in tables:
        try:
            written = export_table(conn, schema, table, OUTPUT_DIR)
            total_rows += written
        except Exception as e:
            print(f"  [XATO] {schema}.{table}: {e}")

    conn.close()
    print("=" * 60)
    print(f"\nYakunlandi! Jami: {len(tables)} fayl, {total_rows:,} qator")
    print(f"Papka: {OUTPUT_DIR.resolve()}")

if __name__ == "__main__":
    main()
