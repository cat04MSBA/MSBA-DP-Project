"""
seed_metric_codes.py
====================
Populates metadata.metric_codes — the crosswalk between
every source's original indicator code and our metric_id.

FIFTH and final seed script to run.
Requires metadata.sources and metadata.metrics to exist first.

Safe to re-run — uses drift-aware upsert logic throughout.

DRIFT-AWARE UPSERT DESIGN:
    The upsert_metric_code() function checks if the incoming code
    differs from the stored code before updating. If it differs,
    it does NOT apply the update — it prints a warning and skips.

    WHY METRIC CODE CHANGES ARE BLOCKED IN SEED SCRIPTS:
        A metric code change means the source changed its indicator
        identifier (e.g. World Bank renamed 'NY.GDP.PCAP.CD' to
        something else). Every historical row in the silver layer
        was inserted using the old code mapping. A silent update
        to the crosswalk would break the ability to trace those
        rows back to the source. The change must go through drift
        detection on the next ingestion run where it is logged to
        ops.metadata_changes and emailed to the team for review.

HOW EACH SOURCE IS HANDLED:
    world_bank  Original codes from API (e.g. NY.GDP.PCAP.CD)
    imf         Original codes from DataMapper API (e.g. NGDP_RPCH)
    pwt         Original codes = Stata variable names (e.g. rtfpna)
    oxford      Single metric, code = 'Score'
    openalex    Single metric, code = 'C154945302'
    wipo_ip     Single metric, code = '4c'
    oecd_msti   3 metrics, codes = B, GV, T_RS
"""

import requests
import pandas as pd
from sqlalchemy import text
from database.connection import get_engine

engine = get_engine()


# ═══════════════════════════════════════════════════════════════
# HELPER: DRIFT-AWARE UPSERT ONE METRIC CODE
# ═══════════════════════════════════════════════════════════════
def upsert_metric_code(conn, metric_id: str,
                       source_id: str, code: str):
    """
    Insert or drift-aware-update one row in metadata.metric_codes.

    WHY DRIFT-AWARE (not unconditional ON CONFLICT DO UPDATE):
        Metric code changes are CRITICAL — a code change means
        the source changed its indicator identifier. All historical
        rows in the silver layer were inserted using the old mapping.
        A silent update bypasses drift detection and breaks
        traceability. The seed script must not be used to
        accidentally apply what should be a reviewed change.

        - New mapping: INSERT normally.
        - Same code: no-op (idempotent).
        - Different code: skip and warn. Must go through drift
          detection on next ingestion run.
    """
    existing = conn.execute(text("""
        SELECT code FROM metadata.metric_codes
        WHERE metric_id = :metric_id AND source_id = :source_id
    """), {'metric_id': metric_id, 'source_id': source_id}).fetchone()

    if existing is None:
        # New mapping — insert normally.
        conn.execute(text("""
            INSERT INTO metadata.metric_codes (metric_id, source_id, code)
            VALUES (:metric_id, :source_id, :code)
        """), {'metric_id': metric_id, 'source_id': source_id, 'code': code})

    elif existing[0] == code:
        # Same code — no-op.
        pass

    else:
        # Different code — skip and warn.
        print(
            f"  ⚠ Skipping metric code change for {metric_id} "
            f"({source_id}): stored='{existing[0]}', "
            f"incoming='{code}'. "
            f"Use drift detection to review this change."
        )


# ═══════════════════════════════════════════════════════════════
# HELPER: LOAD ALL METRIC IDS FROM DATABASE
# ═══════════════════════════════════════════════════════════════
def load_metrics(source_id: str = None):
    """Load metric_ids from metadata.metrics, optionally filtered."""
    if source_id:
        return pd.read_sql("""
            SELECT metric_id FROM metadata.metrics
            WHERE source_id = %(source_id)s
        """, engine, params={'source_id': source_id})
    return pd.read_sql("SELECT metric_id FROM metadata.metrics", engine)


# ═══════════════════════════════════════════════════════════════
# 1. WORLD BANK METRIC CODES
# ═══════════════════════════════════════════════════════════════
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
    """Fetch all indicators for a WB topic with pagination."""
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

        if topic_id == 4:
            all_indicators = [
                i for i in all_indicators
                if i.get('id') in EDUCATION_ALLOWLIST
            ]

        for ind in all_indicators:
            original_code = ind.get('id')
            if not original_code:
                continue
            metric_id = f"wb.{original_code.lower().replace('.', '_')}"
            upsert_metric_code(conn, metric_id, 'world_bank', original_code)
            count += 1

        print(f"    ✓ topic {topic_id}: {len(all_indicators)} codes")

    print(f"  ✓ Processed {count} World Bank metric codes")


# ═══════════════════════════════════════════════════════════════
# 2. IMF METRIC CODES
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

    print(f"  ✓ Processed {count} IMF metric codes")


# ═══════════════════════════════════════════════════════════════
# 3. PWT METRIC CODES
# ═══════════════════════════════════════════════════════════════
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

    print(f"  ✓ Processed {count} PWT metric codes")


# ═══════════════════════════════════════════════════════════════
# 4. OXFORD METRIC CODE
# ═══════════════════════════════════════════════════════════════
def seed_oxford_metric_codes(conn):
    print("\n── Oxford metric codes ──")
    upsert_metric_code(conn, 'oxford.ai_readiness', 'oxford', 'Score')
    print("  ✓ Processed 1 Oxford metric code")


# ═══════════════════════════════════════════════════════════════
# 5. OPENALEX METRIC CODE
# ═══════════════════════════════════════════════════════════════
def seed_openalex_metric_codes(conn):
    print("\n── OpenAlex metric codes ──")
    upsert_metric_code(
        conn,
        'openalex.ai_publication_count',
        'openalex',
        'C154945302'
    )
    print("  ✓ Processed 1 OpenAlex metric code")


# ═══════════════════════════════════════════════════════════════
# 6. WIPO IP METRIC CODE
# ═══════════════════════════════════════════════════════════════
def seed_wipo_metric_codes(conn):
    print("\n── WIPO IP metric codes ──")
    upsert_metric_code(
        conn,
        'wipo.ai_patent_count',
        'wipo_ip',
        '4c'
    )
    print("  ✓ Processed 1 WIPO IP metric code")


# ═══════════════════════════════════════════════════════════════
# 7. OECD MSTI METRIC CODES
# ═══════════════════════════════════════════════════════════════
def seed_oecd_metric_codes(conn):
    print("\n── OECD MSTI metric codes ──")

    oecd_codes = [
        ('oecd.berd_gdp',                 'B'),
        ('oecd.goverd_gdp',               'GV'),
        ('oecd.researchers_per_thousand',  'T_RS'),
    ]

    for metric_id, code in oecd_codes:
        upsert_metric_code(conn, metric_id, 'oecd_msti', code)

    print(f"  ✓ Processed {len(oecd_codes)} OECD MSTI metric codes")


# ═══════════════════════════════════════════════════════════════
# MAIN
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
