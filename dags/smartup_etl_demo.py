"""
QADAM 3: GENERALIZATSIYA — barcha 18 endpoint, bitta skriptda

Bu fayl 1- va 2-qadamlardagi mantiqning umumlashtirilgan (generalize
qilingan) versiyasi. Asosiy g'oya: extract/transform/load funksiyalari
endi "order" yoki "product_group" deb QATTIQ YOZILMAGAN — ular har
qanday endpoint nomi va URL bilan ishlay oladigan qilib yozilgan.

Buning uchun pastda ENDPOINTS lug'ati bor - har bir qator:
    "jadval_nomi": ("url", need_date)

va dastur shu lug'at bo'yicha BITTA halqada (for loop) hammasini
ketma-ket ishlov beradi.
"""

import base64
import json
import os
import traceback
from datetime import datetime, timedelta

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# ═══════════════════════════════════════════════════════════════════
# DEMO REJIMI: bu fayl ustozga ko'rsatish uchun TEZKOR versiya.
# Asosiy farq: DATE_BEGIN endi "2023-01-01" emas, balki "bugundan
# 30 kun oldin". Bu sanaga bog'liq endpointlar uchun chunk sonini
# kamaytiradi - demak ancha kamroq HTTP so'rov.
#
# ESLATMA: bu - faqat DEMO uchun. Haqiqiy production versiyasida
# (smartup_etl.py, bu faylning asl nusxasi) to'liq tarix saqlanadi.
# ═══════════════════════════════════════════════════════════════════
USERNAME = os.environ["SMARTUP_USERNAME"]
PASSWORD = os.environ["SMARTUP_PASSWORD"]
PROJECT_CODE = os.environ.get("SMARTUP_PROJECT_CODE", "trade")
FILIAL_ID = os.environ.get("SMARTUP_FILIAL_ID", "86401")

DATE_BEGIN = (datetime.now() - timedelta(days=30)).strftime("%d.%m.%Y")
DATE_END = datetime.now().strftime("%d.%m.%Y")
CHUNK_DAYS = 30

DB_USER = os.environ.get("DB_USER", "postgres")
DB_PASSWORD = os.environ["DB_PASSWORD"]
# MUHIM: bu skript Docker konteyner ICHIDA ishlaydi (Airflow orqali).
# "localhost" konteyner ichidan konteynerning O'ZINI bildiradi, sizning
# Windows kompyuteringizni emas! Shuning uchun maxsus Docker manzili
# "host.docker.internal" ishlatiladi - bu "konteynerdan tashqaridagi,
# meni ishga tushirgan kompyuter" degan ma'noni bildiradi.
DB_HOST = os.environ.get("DB_HOST", "host.docker.internal")
DB_PORT = os.environ.get("DB_PORT", "5432")
DB_NAME = os.environ.get("DB_NAME", "smartup_v2")

# PostgreSQL'da zaxira so'zlar (bular jadval nomi sifatida ishlatilganda
# muammo chiqaradi, shuning uchun "safe" nomga almashtiramiz)
RESERVED_TABLE_NAMES = {
    "order": "orders",
    "group": "groups",
    "user": "users",
}

# ── 2. ENDPOINTLAR — eski kodingizdan olingan, hammasi ────────────────
# format: "jadval_nomi": ("url", need_date)
ENDPOINTS = {
    # SANA KERAK BO'LGAN ENDPOINTLAR (hujjatlar/tranzaksiyalar)
    "order":              ("https://smartup.online/b/trade/txs/tdeal/order$export",             True),
    "return":             ("https://smartup.online/b/anor/mxsx/mdeal/return$export",            True),
    "visit":              ("https://smartup.online/b/trade/txs/tvt/visit$export",               True),
    "movement":           ("https://smartup.online/b/anor/mxsx/mfm/movement$export",            True),
    "cashin":             ("https://smartup.online/b/trade/txs/tcs/cashin$export",              True),
    "equipment_movement": ("https://smartup.online/b/anor/mxsx/mqpf/equipment_movement$export", True),
    "inventory":          ("https://smartup.online/b/anor/mxsx/mr/inventory$export",            True),
    "service":            ("https://smartup.online/b/anor/mxsx/mr/service$export",              True),
    "room":               ("https://smartup.online/b/anor/mxsx/mrf/room$export",                True),
    "contract":           ("https://smartup.online/b/anor/mxsx/mkf/contract$export",            True),
    "return_reason":      ("https://smartup.online/b/anor/mxsx/mdeal/return_reason$export",     True),
    "bank_operation":     ("https://smartup.online/b/anor/mxsx/mkcs/bank_operation$export",     True),
    "stock_balance":      ("https://smartup.online/b/anor/mxsx/mkw/balance$export",             True),
    "stock_input":        ("https://smartup.online/b/anor/mxsx/mkw/input$export",               True),
    "stock_movement":     ("https://smartup.online/b/anor/mxsx/mkw/movement$export",            True),
    "stock_purchase":     ("https://smartup.online/b/anor/mxsx/mkw/purchase$export",            True),
    "stock_return":       ("https://smartup.online/b/anor/mxsx/mkw/return$export",              True),
    "stocktaking":        ("https://smartup.online/b/anor/mxsx/mkw/stocktaking$export",         True),
    "stock_writeoff":     ("https://smartup.online/b/anor/mxsx/mkw/writeoff$export",            True),
    "logistics":          ("https://smartup.online/b/trade/txs/tdeal/logistics$export",          True),

    # SANA KERAK EMAS (ma'lumotnomalar / spravochniklar)
    "product_group":      ("https://smartup.online/b/anor/mxsx/mr/product_group$export",        False),
    "price_type":         ("https://smartup.online/b/anor/api/v2/mkr/price_type$export",        False),
    "product_price":      ("https://smartup.online/b/anor/api/v2/mkf/product_price$export",     False),
    "producer":           ("https://smartup.online/b/anor/mxsx/mr/producer$export",             False),
    "legal_person":       ("https://smartup.online/b/anor/mxsx/mr/legal_person$export",         False),
    "natural_person":     ("https://smartup.online/b/anor/mxsx/mr/natural_person$export",       False),
    "person_group":       ("https://smartup.online/b/anor/mxsx/mr/person_group$export",         False),
    "equipment_balance":  ("https://smartup.online/b/trade/txs/tvt/equipment_balance$export_data", False),
}


# ── 3. YORDAMCHI: sanani 30 kunlik bo'laklarga bo'lish ───────────────
def date_chunks(start_str: str, end_str: str, days: int = 30):
    start = datetime.strptime(start_str, "%d.%m.%Y")
    end = datetime.strptime(end_str, "%d.%m.%Y")
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=days - 1), end)
        yield current.strftime("%d.%m.%Y"), chunk_end.strftime("%d.%m.%Y")
        current = chunk_end + timedelta(days=1)


def build_headers() -> dict:
    credentials = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
    return {
        "Authorization": f"Basic {credentials}",
        "Content-Type": "application/json",
        "project_code": PROJECT_CODE,
        "filial_id": FILIAL_ID,
    }


def extract_records(data, table_name: str) -> list:
    """
    Smartup javobi turlicha shaklda kelishi mumkin:
      - to'g'ridan-to'g'ri list
      - {"<table_name>": [...]}
      - {"data": [...]}
    Bu funksiya - 2-qadamdagi mantiqning umumlashtirilgani: endpoint
    nomidan qat'i nazar to'g'ri ro'yxatni topib beradi.
    """
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in [table_name, "data", "rows", "result", "items", "list"]:
            if key in data and isinstance(data[key], list):
                return data[key]
        # Hech biri mos kelmasa, lug'at ichidagi BIRINCHI ro'yxatni qaytaramiz
        for v in data.values():
            if isinstance(v, list):
                return v
    return []


# ── 4. EXTRACT — endi har qanday endpoint uchun ishlaydi ─────────────
def extract(table_name: str, url: str, need_date: bool) -> list:
    headers = build_headers()

    if not need_date:
        # Sana kerak emas - bitta so'rov yetarli
        response = requests.get(url, headers=headers, timeout=180)
        response.raise_for_status()
        records = extract_records(response.json(), table_name)
        print(f"   [{table_name}] {len(records)} ta yozuv olindi (sanasiz).")
        return records

    # Sana kerak - chunking + dedup
    chunks = list(date_chunks(DATE_BEGIN, DATE_END, CHUNK_DAYS))
    all_records = []
    seen_ids = set()
    id_key = None

    for d_begin, d_end in chunks:
        params = {"date_begin": d_begin, "date_end": d_end}
        try:
            response = requests.get(url, headers=headers, params=params, timeout=120)
            response.raise_for_status()
            rows = extract_records(response.json(), table_name)
        except Exception as e:
            print(f"   [{table_name}] {d_begin}-{d_end}: XATO -> {e}")
            continue

        if not rows:
            continue

        if id_key is None:
            first_row = rows[0]
            # Nomzodlar ro'yxati kengaytirilgan - 18 xil endpointning
            # turli ID-nomlarini qoplash uchun.
            for candidate in [f"{table_name}_id", "order_id", "deal_id", "id", "code"]:
                if candidate in first_row:
                    id_key = candidate
                    break

        new_count = 0
        for row in rows:
            uid = row.get(id_key) if id_key else str(row)
            if uid not in seen_ids:
                seen_ids.add(uid)
                all_records.append(row)
                new_count += 1

        if new_count:
            pass  # progressni quyida umumiy hisoblaymiz, har bir chunk uchun chiqarmaymiz (18 endpoint uchun log juda uzun bo'lib ketadi)

    print(f"   [{table_name}] {len(all_records)} ta unikal yozuv olindi (ID ustuni: {id_key}).")
    return all_records


# ── 5. REKURSIV flatten — 2-qadamdagi bilan bir xil mantiq ───────────
def flatten_nested(df: pd.DataFrame, table_name: str, child_tables: dict) -> pd.DataFrame:
    df = df.reset_index(drop=True)
    row_uid_col = f"__{table_name}_row_uid__"
    df[row_uid_col] = df.index

    # Haqiqiy ID ustunini topish — topilmasa row index fallback sifatida ishlatiladi
    real_id_col = None
    for candidate in [f"{table_name}_id", "id", "code"]:
        if candidate in df.columns:
            real_id_col = candidate
            break

    for col in [c for c in df.columns if c != row_uid_col]:
        sample = df[col].dropna()
        if len(sample) == 0:
            continue

        is_list_column = sample.apply(lambda x: isinstance(x, list)).any()
        is_dict_column = sample.apply(lambda x: isinstance(x, dict)).any()

        if is_list_column:
            child_rows = []
            for _, row in df.iterrows():
                items = row.get(col)
                link_value = row[real_id_col] if real_id_col else row[row_uid_col]
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict):
                            item = dict(item)
                            item[f"{table_name}_row_id"] = link_value
                            child_rows.append(item)

            if child_rows:
                child_df = pd.json_normalize(child_rows)
                child_table_name = f"{table_name}_{col}"
                child_df = flatten_nested(child_df, child_table_name, child_tables)
                child_tables[child_table_name] = child_df
                print(f"     -> '{child_table_name}': {len(child_df)} qator")

            df = df.drop(columns=[col])

        elif is_dict_column:
            df[col] = df[col].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else x
            )

    return df


# ── 6. TOZALASH YORDAMCHI FUNKSIYALARI ───────────────────────────────
def apply_state_filter(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Universal state filtri: 'A' qiymat mavjud bo'lsa, faqat faol yozuvlarni saqlaydi."""
    if "state" not in df.columns:
        return df
    has_active = df["state"].dropna().astype(str).str.upper().eq("A").any()
    if not has_active:
        return df
    oldin = len(df)
    df = df[df["state"].astype(str).str.upper() == "A"].reset_index(drop=True)
    removed = oldin - len(df)
    if removed > 0:
        print(f"  [{table_name}] state='A' filtri: {oldin} -> {len(df)} qator ({removed} ta olib tashlandi)")
    return df


def drop_null_columns(df: pd.DataFrame, table_name: str, threshold: float = 0.99) -> pd.DataFrame:
    """99%+ bo'sh ustunlarni avtomatik olib tashlaydi."""
    cols_to_drop = []
    for col in df.columns:
        null_ratio = df[col].isna().mean()
        if null_ratio >= threshold:
            cols_to_drop.append((col, null_ratio))
    for col, ratio in cols_to_drop:
        print(f"  [{table_name}] Ustun '{col}' olib tashlandi, chunki {ratio*100:.1f}% bo'sh")
    if cols_to_drop:
        df = df.drop(columns=[c for c, _ in cols_to_drop])
    return df


def remove_duplicates(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Asosiy ID ustuni bo'yicha duplicatlarni olib tashlaydi (eng so'nggi yozuvni saqlaydi)."""
    id_col = None
    for candidate in [f"{table_name}_id", "id"]:
        if candidate in df.columns:
            id_col = candidate
            break
    if id_col is None:
        return df
    oldin = len(df)
    df = df.drop_duplicates(subset=[id_col], keep="last").reset_index(drop=True)
    removed = oldin - len(df)
    if removed > 0:
        print(f"  [{table_name}] Duplicate: {removed} ta olib tashlandi ('{id_col}' ustuni bo'yicha)")
    return df


def fix_date_columns(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Nomida 'date' bo'lgan string ustunlarni datetime tipiga o'tkazadi."""
    for col in df.columns:
        if "date" not in col.lower() or df[col].dtype != object:
            continue
        sample = df[col].dropna()
        if len(sample) == 0 or not sample.apply(lambda x: isinstance(x, str)).all():
            continue
        try:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        except Exception:
            pass
    return df


# ── 7. TRANSFORM ───────────────────────────────────────────────────────
def transform(records: list, table_name: str) -> tuple[pd.DataFrame, dict]:
    if not records:
        return pd.DataFrame(), {}

    df = pd.json_normalize(records)

    # Universal state filtri (inventory, contract, legal_person va boshqa
    # state-li jadvallar uchun — 'A' qiymati bo'lsa, faqat faollarni saqlaydi)
    df = apply_state_filter(df, table_name)

    # 99%+ bo'sh ustunlarni olib tashlash
    df = drop_null_columns(df, table_name)

    # Asosiy ID bo'yicha duplicatlarni olib tashlash
    df = remove_duplicates(df, table_name)

    # Sana ustunlarini string -> datetime tipiga o'tkazish
    df = fix_date_columns(df, table_name)

    child_tables = {}
    df = flatten_nested(df, table_name, child_tables)

    row_uid_col = f"__{table_name}_row_uid__"
    if row_uid_col in df.columns:
        df = df.drop(columns=[row_uid_col])

    return df, child_tables


# ── 8. LOAD ────────────────────────────────────────────────────────────
def safe_name(name: str) -> str:
    """PostgreSQL zaxira so'zlarini xavfsiz nomga almashtiradi."""
    return RESERVED_TABLE_NAMES.get(name, name)


def _write_table(df: pd.DataFrame, table_name: str, engine) -> None:
    """Ustun nomlarini normalize qilib, DROP CASCADE + schema + chunksize bilan yozadi."""
    df = df.copy()
    df.columns = [
        c.lower().replace(" ", "_").replace("-", "_").replace(".", "_")
        for c in df.columns
    ]
    with engine.connect() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS public."{table_name}" CASCADE'))
        conn.commit()
    df.to_sql(table_name, engine, schema="public", if_exists="replace", index=False, chunksize=1000)


def load(df: pd.DataFrame, table_name: str, child_tables: dict, engine) -> dict:
    summary = {}

    if df.empty:
        print(f"   [{table_name}] Bo'sh - yozilmadi.")
        return summary

    out_name = safe_name(table_name)
    _write_table(df, out_name, engine)
    summary[out_name] = len(df)
    print(f"   [{out_name}] {len(df)} qator yozildi.")

    for child_name, child_df in child_tables.items():
        if child_df.empty:
            continue
        out_child_name = safe_name(child_name)
        _write_table(child_df, out_child_name, engine)
        summary[out_child_name] = len(child_df)
        print(f"   [{out_child_name}] {len(child_df)} qator yozildi.")

    return summary


# ── 9. BITTA ENDPOINT ETL — Airflow parallel task'lar uchun ──────────
def build_db_url() -> str:
    return f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def etl_one_endpoint(table_name: str, url: str, need_date: bool) -> None:
    """Bitta endpoint uchun to'liq ETL (extract -> transform -> load).
    Har bir Airflow Task o'z engine'ini yaratadi — parallel xavfsiz.
    """
    engine = create_engine(build_db_url())
    success = False
    try:
        print(f"── {table_name} ──────────────────────────────────")
        raw_records = extract(table_name, url, need_date)
        clean_df, child_tables = transform(raw_records, table_name)
        load(clean_df, table_name, child_tables, engine)
        success = True
    except Exception as e:
        print(f"   [{table_name}] XATO: {e}")
        traceback.print_exc()
        raise
    finally:
        engine.dispose()
    print(f"   [{table_name}] any_success={success}")


# ── 10. MAIN — barcha 28 endpointni ketma-ket ishlov beradi ───────────
def main():
    db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_url)

    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print("PostgreSQL'ga ulanish muvaffaqiyatli.\n")

    overall_summary = {}
    start_time = datetime.now()
    ok_count = 0
    fail_count = 0

    for table_name, (url, need_date) in ENDPOINTS.items():
        print(f"── {table_name} ──────────────────────────────────")
        try:
            raw_records = extract(table_name, url, need_date)
            clean_df, child_tables = transform(raw_records, table_name)
            result = load(clean_df, table_name, child_tables, engine)
            overall_summary[table_name] = result if result else "BO'SH"
            ok_count += 1
        except Exception as e:
            print(f"   [{table_name}] XATO: {e}")
            traceback.print_exc()
            overall_summary[table_name] = f"XATO: {e}"
            fail_count += 1
        print()

    duration = (datetime.now() - start_time).seconds

    print("=" * 60)
    print("YAKUNIY HISOBOT:")
    for name, result in overall_summary.items():
        print(f"  {name:<22}: {result}")
    print(f"\nMuvaffaqiyatli: {ok_count} | Xato: {fail_count}")
    print(f"Jami vaqt: {duration} sekund")
    print("=" * 60)


if __name__ == "__main__":
    main()
