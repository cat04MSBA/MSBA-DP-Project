"""
seed_sources.py
===============
Populates metadata.sources — one row per data source.

FIRST seed script to run. Everything else depends on sources
existing first due to foreign key constraints.

Safe to re-run — uses upsert logic throughout.
Existing rows updated, new rows inserted, nothing deleted.

Sources (8 confirmed):
    world_bank  World Bank WDI
    imf         IMF (WEO, IFS, Fiscal Monitor, AI Prep Index)
    oxford      Oxford Insights GAIRI
    pwt         Penn World Tables
    epoch_ai    Epoch AI Large-Scale AI Models
    openalex    OpenAlex AI Publications
    wipo_ip     WIPO IP Statistics (AI Patents)
    oecd_msti   OECD Main Science and Technology Indicators

UN sources added separately after API keys obtained.
"""

import pandas as pd
from sqlalchemy import text
from database.connection import get_engine

engine = get_engine()

sources = [
    {
        'source_id':        'world_bank',
        'full_name':        'World Bank World Development Indicators',
        'url':              'https://databank.worldbank.org/source/world-development-indicators',
        'access_method':    'REST API',
        'update_frequency': 'annual',
        'license':          'CC-BY 4.0',
        'last_retrieved':   None,
        'notes':            (
            'Accessed via wbgapi Python library. '
            'Scope: Economy & Growth (topic 1), Trade (21), '
            'Infrastructure (9), Energy & Mining (5), '
            'Science & Technology (14), Education (4), '
            'Financial Sector (2). '
            'Uses ".." for missing values — treated as no row in ingestion. '
            'Supports lastUpdated parameter for revision detection.'
        )
    },
    {
        'source_id':        'imf',
        'full_name':        'International Monetary Fund Data',
        'url':              'https://data.imf.org',
        'access_method':    'REST API',
        'update_frequency': 'annual',
        'license':          'CC-BY 4.0',
        'last_retrieved':   None,
        'notes':            (
            'Accessed via IMF DataMapper API. '
            'Covers: World Economic Outlook (WEO), '
            'International Financial Statistics (IFS), '
            'Fiscal Monitor, '
            'AI Preparedness Index (2023 single-year snapshot). '
            'WEO published April and October each year. '
            'Re-pull last 5 years annually for revision detection.'
        )
    },
    {
        'source_id':        'oxford',
        'full_name':        'Oxford Insights Government AI Readiness Index',
        'url':              'https://oxfordinsights.com/ai-readiness/ai-readiness-index/',
        'access_method':    'XLSX download',
        'update_frequency': 'annual',
        'license':          'CC-BY-SA 4.0',
        'last_retrieved':   None,
        'notes':            (
            'No API available. '
            'Download XLSX manually from Oxford Insights website each year. '
            'Only the overall AI Readiness Index score is ingested. '
            'Available from 2019. Covers 188-195 countries per edition. '
            'Compare full annual XLSX to stored values for revision detection.'
        )
    },
    {
        'source_id':        'pwt',
        'full_name':        'Penn World Tables',
        'url':              'https://www.rug.nl/ggdc/productivity/pwt/',
        'access_method':    'Stata file download',
        'update_frequency': 'every few years',
        'license':          'CC-BY 4.0',
        'last_retrieved':   None,
        'notes':            (
            'No API available. '
            'Downloaded as Stata .dta file directly from rug.nl. '
            'Variable labels embedded in file, extracted via StataReader. '
            'PWT 11.0: 185 countries, 1950-2023. '
            'Full file compare on new version detection for revision detection.'
        )
    },
    {
        'source_id':        'openalex',
        'full_name':        'OpenAlex AI Publications Dataset',
        'url':              'https://api.openalex.org',
        'access_method':    'REST API',
        'update_frequency': 'continuous (downloaded annually)',
        'license':          'CC0',
        'last_retrieved':   None,
        'notes':            (
            'Free API. Requires free API key from openalex.org/settings/api. '
            'Filters works by AI concept ID C154945302 (Artificial Intelligence). '
            'Groups by authorships.institutions.country_code for '
            'per-country per-year AI publication counts. '
            'Returns ISO2 country codes — mapped to ISO3 via pycountry '
            'and stored in metadata.country_codes at seeding time. '
            '100,000 API credits per day with free key.'
        )
    },
    {
        'source_id':        'wipo_ip',
        'full_name':        'WIPO IP Statistics — AI Patent Applications',
        'url':              'https://www.wipo.int/en/web/ip-statistics',
        'access_method':    'bulk download',
        'update_frequency': 'annual',
        'license':          'CC-BY 4.0',
        'last_retrieved':   None,
        'notes':            (
            'No REST API for patent statistics. '
            'Download complete datasets → patent by technology ZIP '
            'from https://www.wipo.int/en/web/ip-statistics. '
            'Filter to IPC class G06N (Computer Science, AI). '
            'AI patent applications by country of origin per year. '
            'Compare full annual file to stored values for revision detection.'
        )
    },
    {
        'source_id':        'oecd_msti',
        'full_name':        'OECD Main Science and Technology Indicators',
        'url':              'https://data-explorer.oecd.org',
        'access_method':    'REST API',
        'update_frequency': 'biannual',
        'license':          'CC-BY 4.0',
        'last_retrieved':   None,
        'notes':            (
            'Free SDMX REST API at sdmx.oecd.org/public/rest/. '
            'No API key required. '
            'Coverage: 38 OECD members + selected non-members '
            '(Argentina, China, Singapore, South Africa, others). '
            'Three variables: B_XGDP (BERD % GDP), '
            'GOV_XGDP (GOVERD % GDP), TP_RSXLF (researchers per thousand). '
            'Published March and September. '
            'Re-pull last 3 years annually for revision detection.'
        )
    }
]

print("Seeding metadata.sources...")

with engine.connect() as conn:
    for source in sources:
        conn.execute(text("""
            INSERT INTO metadata.sources (
                source_id, full_name, url, access_method,
                update_frequency, license, last_retrieved, notes
            )
            VALUES (
                :source_id, :full_name, :url, :access_method,
                :update_frequency, :license, :last_retrieved, :notes
            )
            ON CONFLICT (source_id) DO UPDATE SET
                full_name        = EXCLUDED.full_name,
                url              = EXCLUDED.url,
                access_method    = EXCLUDED.access_method,
                update_frequency = EXCLUDED.update_frequency,
                license          = EXCLUDED.license,
                notes            = EXCLUDED.notes
        """), source)
    conn.commit()

print(f"✓ Upserted {len(sources)} sources into metadata.sources")

result = pd.read_sql("""
    SELECT source_id, full_name, access_method
    FROM metadata.sources
    ORDER BY source_id
""", engine)

print("\nCurrent metadata.sources:")
print(result.to_string(index=False))
