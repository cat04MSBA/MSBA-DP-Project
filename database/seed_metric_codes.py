"""
seed_metric_codes.py
====================
Populates metadata.metric_codes — the crosswalk between
every source's original indicator code and our metric_id.

FIFTH and final seed script to run.
Requires metadata.sources and metadata.metrics to exist first.

Safe to re-run — uses upsert logic throughout.

HOW EACH SOURCE IS HANDLED:
    world_bank  Original codes from API (e.g. NY.GDP.PCAP.CD)
                metric_id = 'wb.' + code.lower().replace('.','_')
    imf         Original codes from DataMapper API (e.g. NGDP_RPCH)
                metric_id = 'imf.' + code.lower()
    pwt         Original codes = Stata variable names (e.g. rtfpna)
                metric_id = 'pwt.' + var_name.lower()
    oxford      Single metric, code = 'Score' (XLSX column header)
    openalex    Single metric, code = 'ai_publication_count'
    wipo_ip     Single metric, code = '4c' (WIPO AI patent indicator code)
    oecd_msti   3 metrics, codes = MSTI variable codes:
                B, GV, T_RS
"""

import requests
import pandas as pd
from sqlalchemy import text
from database.connection import get_engine

engine = get_engine()


# ═══════════════════════════════════════════════════════════════
# HELPER: UPSERT ONE METRIC CODE
# ═══════════════════════════════════════════════════════════════
def upsert_metric_code(conn, metric_id: str, source_id: str, code: str):
    """Insert or update one row in metadata.metric_codes."""
    conn.execute(text("""
        INSERT INTO metadata.metric_codes (metric_id, source_id, code)
        VALUES (:metric_id, :source_id, :code)
        ON CONFLICT (metric_id, source_id) DO UPDATE SET
            code = EXCLUDED.code
    """), {'metric_id': metric_id, 'source_id': source_id, 'code': code})


# ═══════════════════════════════════════════════════════════════
# HELPER: LOAD ALL METRIC IDS FROM DATABASE
# Used to verify we only insert codes for metrics that exist.
# ═══════════════════════════════════════════════════════════════
def load_metrics(source_id: str = None):
    if source_id:
        return pd.read_sql("""
            SELECT metric_id FROM metadata.metrics
            WHERE source_id = %(source_id)s
        """, engine, params={'source_id': source_id})
    return pd.read_sql("SELECT metric_id FROM metadata.metrics", engine)


# ═══════════════════════════════════════════════════════════════
# 1. WORLD BANK METRIC CODES
# Re-fetch from API to get exact original codes.
# metric_id was derived from these at seeding time.
# We re-derive the mapping rather than reversing it.
# ═══════════════════════════════════════════════════════════════
# Must match EDUCATION_ALLOWLIST in seed_metrics.py exactly
EDUCATION_ALLOWLIST = {
    'SE.PRM.UNER.FE', 'SE.PRM.UNER.MA',
    'SE.XPD.TOTL.GD.ZS', 'SE.XPD.TOTL.GB.ZS',
    'SE.XPD.PRIM.PC.ZS', 'SE.XPD.SECO.PC.ZS', 'SE.XPD.TERT.PC.ZS',
    'SE.PRM.GINT.FE.ZS', 'SE.PRM.GINT.MA.ZS',
    'SL.TLF.TOTL.FE.ZS', 'SL.TLF.TOTL.IN',
    'SE.ADT.LITR.FE.ZS', 'SE.ADT.LITR.MA.ZS', 'SE.ADT.LITR.ZS',
    'SE.ADT.1524.LT.FE.ZS', 'SE.ADT.1524.LT.MA.ZS', 'SE.ADT.1524.LT.ZS',
    'SE.PRM.PRSL.FE.ZS', 'SE.PRM.PRSL.MA.ZS',
    'SP.POP.0014.TO.ZS', 'SP.POP.1564.TO.ZS',
    'SE.PRM.CMPT.FE.ZS', 'SE.PRM.CMPT.MA.ZS', 'SE.PRM.CMPT.ZS',
    'SE.SEC.PROG.FE.ZS', 'SE.SEC.PROG.MA.ZS',
    'SE.PRM.ENRL.TC.ZS',
    'SE.PRM.REPT.FE.ZS', 'SE.PRM.REPT.MA.ZS',
    'SE.PRE.ENRR',
    'SE.PRM.ENRR', 'SE.PRM.NENR',
    'SE.ENR.PRIM.FM.ZS', 'SE.ENR.PRSC.FM.ZS',
    'SE.SEC.ENRR', 'SE.SEC.NENR',
    'SE.TER.ENRR',
    'SE.PRM.TCAQ.ZS',
    'SL.UEM.TOTL.FE.ZS', 'SL.UEM.TOTL.MA.ZS', 'SL.UEM.TOTL.ZS',
}


def fetch_wb_topic(topic_id):
    """Fetch all indicators for a WB topic using path-based URL with pagination."""
    page = 1
    all_indicators = []
    while True:
        url = (
            f"https://api.worldbank.org/v2/topic/{topic_id}/indicator"
            f"?format=json&per_page=500&page={page}"
        )
        data = None
        for attempt in range(3):
            try:
                response = requests.get(url, timeout=20)
                response.raise_for_status()
                data = response.json()
                break
            except Exception as e:
                print(f"  ⚠ Topic {topic_id} page {page} attempt {attempt+1}/3: {e}")

        if not data or len(data) < 2 or not data[1]:
            break

        total_pages = data[0].get('pages', 1)
        all_indicators.extend(data[1])
        if page >= total_pages:
            break
        page += 1
    return all_indicators


def seed_wb_metric_codes(conn):
    print("\n── World Bank metric codes ──")

    topic_ids = [1, 2, 4, 5, 9, 14, 21]
    count = 0

    for topic_id in topic_ids:
        all_indicators = fetch_wb_topic(topic_id)

        # Education: apply allowlist — matches seed_metrics.py exactly
        if topic_id == 4:
            all_indicators = [i for i in all_indicators if i.get('id') in EDUCATION_ALLOWLIST]

        for ind in all_indicators:
            original_code = ind.get('id')
            if not original_code:
                continue
            metric_id = f"wb.{original_code.lower().replace('.', '_')}"
            upsert_metric_code(conn, metric_id, 'world_bank', original_code)
            count += 1

        print(f"    ✓ topic {topic_id}: {len(all_indicators)} codes")

    print(f"  ✓ Upserted {count} World Bank metric codes")


# ═══════════════════════════════════════════════════════════════
# 2. IMF METRIC CODES
# Original codes from DataMapper API.
# ═══════════════════════════════════════════════════════════════
def seed_imf_metric_codes(conn):
    print("\n── IMF metric codes ──")

    url = "https://www.imf.org/external/datamapper/api/v1/indicators"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"  ✗ Could not fetch IMF indicators: {e}")
        raise

    indicators = data.get('indicators', {})
    count = 0

    for code in indicators.keys():
        if not code:
            continue
        metric_id = f"imf.{code.lower()}"
        upsert_metric_code(conn, metric_id, 'imf', code)
        count += 1

    print(f"  ✓ Upserted {count} IMF metric codes")


# ═══════════════════════════════════════════════════════════════
# 3. PWT METRIC CODES
# Original codes = Stata variable names from the .dta file.
# ═══════════════════════════════════════════════════════════════
# PWT_VARIABLE_LABELS reused from seed_metrics — same source, same fix.
# Variable names are stable for PWT 11.0.
PWT_VARIABLE_LABELS = {
    'rgdpe', 'rgdpo', 'pop', 'emp', 'avh', 'hc', 'ccon', 'cda',
    'cgdpe', 'cgdpo', 'cn', 'ck', 'ctfp', 'cwtfp', 'rgdpna',
    'rconna', 'rdana', 'rnna', 'rkna', 'rtfpna', 'rwtfpna',
    'labsh', 'irr', 'delta', 'xr', 'pl_con', 'pl_da', 'pl_gdpo',
    'i_cig', 'i_xm', 'i_xr', 'i_outlier', 'i_irr', 'cor_exp',
    'csh_c', 'csh_i', 'csh_g', 'csh_x', 'csh_m', 'csh_r',
    'pl_c', 'pl_i', 'pl_g', 'pl_x', 'pl_m', 'pl_n', 'pl_k',
}


def seed_pwt_metric_codes(conn):
    print("\n── PWT metric codes ──")

    count = 0
    for var_name in PWT_VARIABLE_LABELS:
        metric_id = f"pwt.{var_name.lower()}"
        upsert_metric_code(conn, metric_id, 'pwt', var_name)
        count += 1

    print(f"  ✓ Upserted {count} PWT metric codes")


# ═══════════════════════════════════════════════════════════════
# 4. OXFORD METRIC CODE
# Single metric. Code = XLSX column header ('Score').
# ═══════════════════════════════════════════════════════════════
def seed_oxford_metric_codes(conn):
    print("\n── Oxford metric codes ──")
    upsert_metric_code(conn, 'oxford.ai_readiness', 'oxford', 'Score')
    print("  ✓ Upserted 1 Oxford metric code")



# ═══════════════════════════════════════════════════════════════
# 6. OPENALEX METRIC CODE
# Single metric. Code = OpenAlex concept ID for AI.
# ═══════════════════════════════════════════════════════════════
def seed_openalex_metric_codes(conn):
    print("\n── OpenAlex metric codes ──")
    upsert_metric_code(
        conn,
        'openalex.ai_publication_count',
        'openalex',
        'C154945302'  # OpenAlex concept ID for Artificial Intelligence
    )
    print("  ✓ Upserted 1 OpenAlex metric code")


# ═══════════════════════════════════════════════════════════════
# 7. WIPO IP METRIC CODE
# Single metric. Code = IPC classification for AI patents.
# ═══════════════════════════════════════════════════════════════
def seed_wipo_metric_codes(conn):
    print("\n── WIPO IP metric codes ──")
    upsert_metric_code(
        conn,
        'wipo.ai_patent_count',
        'wipo_ip',
        '4c'  # IPC class: Computer Science / AI
    )
    print("  ✓ Upserted 1 WIPO IP metric code")


# ═══════════════════════════════════════════════════════════════
# 8. OECD MSTI METRIC CODES
# 3 metrics. Codes = OECD MSTI variable codes.
# Confirmed from OECD MSTI documentation.
# ═══════════════════════════════════════════════════════════════
def seed_oecd_metric_codes(conn):
    print("\n── OECD MSTI metric codes ──")

    oecd_codes = [
        ('oecd.berd_gdp',                'B'),
        ('oecd.goverd_gdp',              'GV'),
        ('oecd.researchers_per_thousand','T_RS'),
    ]

    for metric_id, code in oecd_codes:
        upsert_metric_code(conn, metric_id, 'oecd_msti', code)

    print(f"  ✓ Upserted {len(oecd_codes)} OECD MSTI metric codes")


# ═══════════════════════════════════════════════════════════════
# MAIN — RUN ALL IN ORDER
# ═══════════════════════════════════════════════════════════════
print("Seeding metadata.metric_codes...")
print("=" * 50)

with engine.connect() as conn:
    seed_wb_metric_codes(conn)
    seed_imf_metric_codes(conn)
    seed_pwt_metric_codes(conn)
    seed_oxford_metric_codes(conn)
    seed_openalex_metric_codes(conn)
    seed_wipo_metric_codes(conn)
    seed_oecd_metric_codes(conn)
    conn.commit()

print("\n" + "=" * 50)

# ─────────────────────────────────────────────────────────────
# VERIFY
# ─────────────────────────────────────────────────────────────
result = pd.read_sql("""
    SELECT source_id, COUNT(*) AS metric_code_count
    FROM metadata.metric_codes
    GROUP BY source_id
    ORDER BY source_id
""", engine)

print("\nMetric codes per source:")
print(result.to_string(index=False))

total = pd.read_sql(
    "SELECT COUNT(*) AS total FROM metadata.metric_codes", engine
)
print(f"\nTotal metric codes: {total['total'].iloc[0]}")
