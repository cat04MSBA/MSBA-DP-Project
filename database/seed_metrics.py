"""
seed_metrics.py
===============
Populates metadata.metrics — one row per metric.

THIRD seed script to run.
metadata.metric_codes depends on this existing first.

Safe to re-run — uses upsert logic throughout.

Schema: metric_id, metric_name, source_id, category,
        unit, description, frequency, available_from, available_to

No subcategory column — removed from schema.
available_from and available_to are set to NULL here.
They are auto-calculated after ingestion by a separate script:
    SELECT MIN(year), MAX(year) FROM standardized.observations
    WHERE metric_id = '...'

METADATA SOURCES:
    world_bank  → fully automated from REST API
                  name, unit, description, category all from API
    imf         → fully automated from DataMapper API
                  name, description from API
    pwt         → fully automated from Stata file embedded labels
                  variable name = code, label = name + description
    oxford      → 1 metric hardcoded
                  description from oxfordinsights.com
    openalex    → 1 metric hardcoded
                  name and description confirmed from API response
    wipo_ip     → 1 metric hardcoded
                  description from WIPO IP Statistics documentation
    oecd_msti   → 3 metrics hardcoded
                  variable codes confirmed from OECD MSTI documentation
                  NOTE: API URL to be added once confirmed by team
"""

import requests
import pandas as pd
from sqlalchemy import text
from database.connection import get_engine

engine = get_engine()


# ═══════════════════════════════════════════════════════════════
# HELPER: UPSERT ONE METRIC
# ═══════════════════════════════════════════════════════════════
def upsert_metric(conn, metric: dict):
    """
    Insert or update one row in metadata.metrics.
    available_from and available_to are intentionally excluded
    from the UPDATE — they are owned by the post-ingestion
    calculation script, not seed scripts.
    """
    conn.execute(text("""
        INSERT INTO metadata.metrics (
            metric_id, metric_name, source_id, category,
            unit, description, frequency,
            available_from, available_to
        )
        VALUES (
            :metric_id, :metric_name, :source_id, :category,
            :unit, :description, :frequency,
            :available_from, :available_to
        )
        ON CONFLICT (metric_id) DO UPDATE SET
            metric_name  = EXCLUDED.metric_name,
            source_id    = EXCLUDED.source_id,
            category     = EXCLUDED.category,
            unit         = EXCLUDED.unit,
            description  = EXCLUDED.description,
            frequency    = EXCLUDED.frequency
            -- available_from and available_to intentionally excluded:
            -- owned by post-ingestion calculation script
    """), metric)


def make_metric(metric_id, metric_name, source_id, category,
                unit, description, frequency='annual'):
    """Helper to build a metric dict with NULL available dates."""
    return {
        'metric_id':      metric_id,
        'metric_name':    metric_name,
        'source_id':      source_id,
        'category':       category,
        'unit':           unit,
        'description':    description,
        'frequency':      frequency,
        'available_from': None,
        'available_to':   None
    }


# ═══════════════════════════════════════════════════════════════
# 1. WORLD BANK METRICS
# Fully automated from REST API.
# name, unit, description, category all come from API response.
# Nothing invented.
#
# Topic IDs confirmed from:
#   https://api.worldbank.org/v2/topics?format=json
# ═══════════════════════════════════════════════════════════════
# Education indicator allowlist — exactly the 41 featured indicators
# from the World Bank Education topic page (worldbank.org/en/topic/education).
# Topic 4 returns thousands of indicators across all WB databases;
# this allowlist restricts to the curated featured set only.
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


def seed_world_bank_metrics(conn):
    print("\n── World Bank ──")

    # Topic IDs for our 7 agreed categories
    topic_ids = {
        1:  'Economy & Growth',
        2:  'Financial Sector',
        4:  'Education',
        5:  'Energy & Mining',
        9:  'Infrastructure',
        14: 'Science & Technology',
        21: 'Trade'
    }

    count = 0
    for topic_id, topic_name in topic_ids.items():
        print(f"  Fetching: {topic_name} (ID: {topic_id})...")

        all_indicators = fetch_wb_topic(topic_id)

        if not all_indicators:
            print(f"  ⚠ No indicators returned for topic {topic_id}")
            continue

        # Education: apply allowlist to restrict to featured indicators only
        if topic_id == 4:
            all_indicators = [i for i in all_indicators if i.get('id') in EDUCATION_ALLOWLIST]

        for ind in all_indicators:
            if not ind.get('id') or not ind.get('name'):
                continue

            original_code = ind['id']
            metric_id = f"wb.{original_code.lower().replace('.', '_')}"

            unit = ind.get('unit') or None
            if unit and str(unit).strip() == '':
                unit = None

            upsert_metric(conn, make_metric(
                metric_id   = metric_id,
                metric_name = ind['name'],
                source_id   = 'world_bank',
                category    = topic_name,
                unit        = unit,
                description = ind.get('sourceNote') or None,
                frequency   = 'annual'
            ))
            count += 1

        print(f"    ✓ {topic_name}: {len(all_indicators)} indicators")

    print(f"  ✓ Upserted {count} World Bank metrics")


# ═══════════════════════════════════════════════════════════════
# 2. IMF METRICS
# Fully automated from DataMapper API.
# name and description come directly from API.
# Nothing invented.
#
# Endpoint confirmed from:
#   https://www.imf.org/external/datamapper/api/help
# ═══════════════════════════════════════════════════════════════
def seed_imf_metrics(conn):
    print("\n── IMF ──")

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

    for code, info in indicators.items():
        if not code or not info.get('label'):
            continue

        # metric_id convention: imf.original_code_lowercase
        # e.g. NGDP_RPCH → imf.ngdp_rpch
        metric_id = f"imf.{code.lower()}"

        unit = info.get('unit') or None
        if unit and str(unit).strip() == '':
            unit = None

        upsert_metric(conn, make_metric(
            metric_id   = metric_id,
            metric_name = info.get('label'),            # from API
            source_id   = 'imf',
            category    = None,                         # IMF API has no topics
            unit        = unit,                         # from API if present
            description = info.get('description') or None,  # from API
            frequency   = 'annual'
        ))
        count += 1

    print(f"  ✓ Upserted {count} IMF metrics")


# ═══════════════════════════════════════════════════════════════
# 3. PWT METRICS
# Fully automated from Stata file embedded labels.
# Variable labels are embedded in .dta format and extracted
# via pandas StataReader — nothing invented.
#
# URL confirmed from:
#   https://www.rug.nl/ggdc/productivity/pwt/
# ═══════════════════════════════════════════════════════════════
# PWT 11.0 variable labels — extracted from pwt110.dta
# Hardcoded to avoid dependency on file download at seeding time.
# Source: Penn World Tables 11.0, doi:10.34894/FABVLR
PWT_VARIABLE_LABELS = {
    'rgdpe':    'Expenditure-side real GDP at chained PPPs (in mil. 2021US$)',
    'rgdpo':    'Output-side real GDP at chained PPPs (in mil. 2021US$)',
    'pop':      'Population (in millions)',
    'emp':      'Number of persons engaged (in millions)',
    'avh':      'Average annual hours worked by persons engaged (source: The Conference Board)',
    'hc':       'Human capital index, see note hc',
    'ccon':     'Real consumption of households and government, at current PPPs (in mil. 2021US$)',
    'cda':      'Real domestic absorption, see note cda',
    'cgdpe':    'Expenditure-side real GDP at current PPPs (in mil. 2021US$)',
    'cgdpo':    'Output-side real GDP at current PPPs (in mil. 2021US$)',
    'cn':       'Capital stock at current PPPs (in mil. 2021US$)',
    'ck':       'Capital services levels at current PPPs (USA=1)',
    'ctfp':     'TFP level at current PPPs (USA=1)',
    'cwtfp':    'Welfare-relevant TFP levels at current PPPs (USA=1)',
    'rgdpna':   'Real GDP at constant 2021 national prices (in mil. 2021US$)',
    'rconna':   'Real consumption at constant 2021 national prices (in mil. 2021US$)',
    'rdana':    'Real domestic absorption at constant 2021 national prices (in mil. 2021US$)',
    'rnna':     'Capital stock at constant 2021 national prices (in mil. 2021US$)',
    'rkna':     'Capital services at constant 2021 national prices (2021=1)',
    'rtfpna':   'TFP at constant national prices (2021=1)',
    'rwtfpna':  'Welfare-relevant TFP at constant national prices (2021=1)',
    'labsh':    'Share of labour compensation in GDP at current national prices',
    'irr':      'Real internal rate of return',
    'delta':    'Average depreciation rate of the capital stock',
    'xr':       'Exchange rate, national currency/USD (market+estimated)',
    'pl_con':   'Price level of CCON (PPP/XR), price level of USA GDPo in 2021=1',
    'pl_da':    'Price level of CDA (PPP/XR), price level of USA GDPo in 2021=1',
    'pl_gdpo':  'Price level of CGDPo (PPP/XR), price level of USA GDPo in 2021=1',
    'i_cig':    '0/1/2/3/4, see note i_cig',
    'i_xm':     '0/1/2, see note i_xm',
    'i_xr':     '0/1: the exchange rate is market-based (0) or estimated (1)',
    'i_outlier':'0/1, see note i_outlier',
    'i_irr':    '0/1/2/3, see note i_irr',
    'cor_exp':  'Correlation between expenditure shares, see note cor_exp',
    'csh_c':    'Share of household consumption at current PPPs',
    'csh_i':    'Share of gross capital formation at current PPPs',
    'csh_g':    'Share of government consumption at current PPPs',
    'csh_x':    'Share of merchandise exports at current PPPs',
    'csh_m':    'Share of merchandise imports at current PPPs',
    'csh_r':    'Share of residual trade and GDP statistical discrepancy at current PPPs',
    'pl_c':     'Price level of household consumption, price level of USA GDPo in 2021=1',
    'pl_i':     'Price level of capital formation, price level of USA GDPo in 2021=1',
    'pl_g':     'Price level of government consumption, price level of USA GDPo in 2021=1',
    'pl_x':     'Price level of exports, price level of USA GDPo in 2021=1',
    'pl_m':     'Price level of imports, price level of USA GDPo in 2021=1',
    'pl_n':     'Price level of the capital stock, price level of USA 2021=1',
    'pl_k':     'Price level of the capital services, price level of USA=1',
}


def seed_pwt_metrics(conn):
    print("\n── Penn World Tables ──")

    # Skip identifier columns — not metrics
    skip_columns = {'countrycode', 'country', 'currency_unit', 'year'}

    count = 0
    for var_name, label in PWT_VARIABLE_LABELS.items():
        if var_name in skip_columns:
            continue

        metric_id = f"pwt.{var_name.lower()}"

        upsert_metric(conn, make_metric(
            metric_id   = metric_id,
            metric_name = label,
            source_id   = 'pwt',
            category    = 'Productivity',
            unit        = None,         # units embedded in label text
            description = label,        # label IS the description in PWT
            frequency   = 'annual'
        ))
        count += 1

    print(f"  ✓ Upserted {count} PWT metrics")


# ═══════════════════════════════════════════════════════════════
# 4. OXFORD METRIC — 1 metric hardcoded
# Overall AI Readiness Index score only.
# No pillars, no sub-indicators.
# Description taken from Oxford Insights website:
#   https://oxfordinsights.com/ai-readiness/ai-readiness-index/
# ═══════════════════════════════════════════════════════════════
def seed_oxford_metrics(conn):
    print("\n── Oxford Insights ──")

    upsert_metric(conn, make_metric(
        metric_id   = 'oxford.ai_readiness',
        metric_name = 'Government AI Readiness Index',
        source_id   = 'oxford',
        category    = 'AI Readiness',
        unit        = '0-100 score',
        description = (
            'Measures the extent to which a government can harness AI '
            'to benefit the public. Assesses governments annually across '
            'pillars of Government, Technology Sector, and '
            'Data & Infrastructure. Published annually since 2019. '
            'Source: oxfordinsights.com/ai-readiness/ai-readiness-index/'
        )
    ))

    print("  ✓ Upserted 1 Oxford metric: oxford.ai_readiness")



# ═══════════════════════════════════════════════════════════════
# 5. OPENALEX METRIC — 1 metric hardcoded
# AI publication count per country per year.
# Concept ID C154945302 confirmed from OpenAlex API:
#   https://api.openalex.org/concepts/C154945302
# Name and description taken directly from that API response.
# ═══════════════════════════════════════════════════════════════
def seed_openalex_metrics(conn):
    print("\n── OpenAlex ──")

    upsert_metric(conn, make_metric(
        metric_id   = 'openalex.ai_publication_count',
        metric_name = 'AI Publications Count',
        source_id   = 'openalex',
        category    = 'AI Research Output',
        unit        = 'count',
        description = (
            'Number of AI-related academic publications per country per year. '
            'Counted by filtering OpenAlex works on concept C154945302 '
            '(Artificial Intelligence: field of computer science and '
            'engineering practices for intelligence demonstrated by machines '
            'and intelligent agents) and grouping by the country of the '
            "authors' affiliated institutions. "
            'A paper with authors from multiple countries is counted '
            'once per country. '
            'Source: api.openalex.org, concept ID C154945302'
        )
    ))

    print("  ✓ Upserted 1 OpenAlex metric: openalex.ai_publication_count")


# ═══════════════════════════════════════════════════════════════
# 6. WIPO IP METRIC — 1 metric hardcoded
# AI patent applications per country per year.
# IPC class G06N confirmed from WIPO documentation as the
# classification for Computer Science / AI patents.
# Description from WIPO IP Statistics documentation.
# ═══════════════════════════════════════════════════════════════
def seed_wipo_metrics(conn):
    print("\n── WIPO IP ──")

    upsert_metric(conn, make_metric(
        metric_id   = 'wipo.ai_patent_count',
        metric_name = 'AI Patent Applications',
        source_id   = 'wipo_ip',
        category    = 'AI Production',
        unit        = 'count',
        description = (
            'Number of patent applications classified under IPC class G06N '
            '(Computing; Calculating; Counting — Artificial Intelligence) '
            'by country of origin per year. '
            'Sourced from WIPO IP Statistics complete patent dataset. '
            'Country of origin refers to the country of the first-named '
            'applicant. '
            'Source: wipo.int/en/web/ip-statistics'
        )
    ))

    print("  ✓ Upserted 1 WIPO IP metric: wipo.ai_patent_count")


# ═══════════════════════════════════════════════════════════════
# 7. OECD MSTI METRICS — 3 metrics hardcoded
# Variable codes confirmed from OECD MSTI documentation.
# Names and descriptions from OECD Frascati Manual and MSTI docs.
#
# NOTE: OECD MSTI covers 38 OECD members + selected non-members.
# Not global coverage. Documented in notes and description.
#
# TODO: Add exact SDMX API URL once confirmed from OECD Data Explorer.
# ═══════════════════════════════════════════════════════════════
def seed_oecd_msti_metrics(conn):
    print("\n── OECD MSTI ──")

    metrics = [
        make_metric(
            metric_id   = 'oecd.berd_gdp',
            metric_name = 'Business Enterprise R&D Expenditure as % of GDP',
            source_id   = 'oecd_msti',
            category    = 'Research & Development',
            unit        = '% of GDP',
            description = (
                'Business enterprise expenditure on research and experimental '
                'development (BERD) as a percentage of gross domestic product. '
                'OECD variable code: B_XGDP. '
                'Coverage: 38 OECD members + selected non-members. '
                'Source: OECD Main Science and Technology Indicators (MSTI)'
            )
        ),
        make_metric(
            metric_id   = 'oecd.goverd_gdp',
            metric_name = 'Government R&D Expenditure as % of GDP',
            source_id   = 'oecd_msti',
            category    = 'Research & Development',
            unit        = '% of GDP',
            description = (
                'Government intramural expenditure on research and experimental '
                'development (GOVERD) as a percentage of gross domestic product. '
                'OECD variable code: GOV_XGDP. '
                'Coverage: 38 OECD members + selected non-members. '
                'Source: OECD Main Science and Technology Indicators (MSTI)'
            )
        ),
        make_metric(
            metric_id   = 'oecd.researchers_per_thousand',
            metric_name = 'Researchers per Thousand Employed',
            source_id   = 'oecd_msti',
            category    = 'Research & Development',
            unit        = 'per thousand employed',
            description = (
                'Total researchers (full-time equivalent) per thousand '
                'total employment. '
                'OECD variable code: TP_RSXLF. '
                'Researchers are professionals engaged in the conception or '
                'creation of new knowledge. '
                'Coverage: 38 OECD members + selected non-members. '
                'Source: OECD Main Science and Technology Indicators (MSTI)'
            )
        )
    ]

    for metric in metrics:
        upsert_metric(conn, metric)

    print(f"  ✓ Upserted {len(metrics)} OECD MSTI metrics")


# ═══════════════════════════════════════════════════════════════
# MAIN — RUN ALL IN ORDER
# ═══════════════════════════════════════════════════════════════
print("Seeding metadata.metrics...")
print("=" * 50)

with engine.connect() as conn:
    seed_world_bank_metrics(conn)
    seed_imf_metrics(conn)
    seed_pwt_metrics(conn)
    seed_oxford_metrics(conn)
    seed_openalex_metrics(conn)
    seed_wipo_metrics(conn)
    seed_oecd_msti_metrics(conn)
    conn.commit()

print("\n" + "=" * 50)

# ─────────────────────────────────────────────────────────────
# VERIFY
# ─────────────────────────────────────────────────────────────
result = pd.read_sql("""
    SELECT source_id, COUNT(*) AS metric_count
    FROM metadata.metrics
    GROUP BY source_id
    ORDER BY source_id
""", engine)

print("\nMetrics per source:")
print(result.to_string(index=False))

total = pd.read_sql(
    "SELECT COUNT(*) AS total FROM metadata.metrics", engine
)
print(f"\nTotal metrics: {total['total'].iloc[0]}")
