"""
seed_countries.py
=================
Populates metadata.countries from World Bank API.

SECOND seed script to run.
metadata.country_codes depends on this existing first.

Safe to re-run — uses upsert logic throughout.

Schema: iso3, country_name, region, income_group, notes
No iso2 column — source-specific codes including ISO2
are handled entirely by metadata.country_codes.

Edge cases documented in notes column (5 countries only):
    XKX  Kosovo    — informal ISO code, not officially assigned
    TWN  Taiwan    — listed as separate economy by World Bank
    PSE  Palestine — listed as West Bank and Gaza
    HKG  Hong Kong — listed separately from mainland China
    MAC  Macao     — listed separately from mainland China
"""

import wbgapi as wb
import pandas as pd
from sqlalchemy import text
from database.connection import get_engine

engine = get_engine()

# ─────────────────────────────────────────────────────────────
# CONTINENT MAPPING — ISO3 → continent
#
# Static mapping. Continents never change for a country.
# Covers all ~249 ISO 3166-1 alpha-3 codes including territories
# and special codes used by World Bank (XKX Kosovo etc.).
#
# WHY HARDCODED HERE (not fetched from an API):
#   There is no authoritative free API for ISO3 → continent.
#   pycountry does not expose continent. This mapping is a fixed
#   geographic fact — it does not need to be fetched at runtime.
#   Hardcoding it here means zero network dependency and zero
#   latency at seed time. If a new territory is added to the
#   system, add it here.
#
# CONTINENT VALUES (7-continent model):
#   'Africa', 'Asia', 'Europe', 'North America',
#   'South America', 'Oceania', 'Antarctica'
# ─────────────────────────────────────────────────────────────
ISO3_TO_CONTINENT = {
    # ── Africa ───────────────────────────────────────────────
    'AGO': 'Africa', 'BEN': 'Africa', 'BWA': 'Africa',
    'BFA': 'Africa', 'BDI': 'Africa', 'CMR': 'Africa',
    'CPV': 'Africa', 'CAF': 'Africa', 'TCD': 'Africa',
    'COM': 'Africa', 'COD': 'Africa', 'COG': 'Africa',
    'CIV': 'Africa', 'DJI': 'Africa', 'EGY': 'Africa',
    'GNQ': 'Africa', 'ERI': 'Africa', 'SWZ': 'Africa',
    'ETH': 'Africa', 'GAB': 'Africa', 'GMB': 'Africa',
    'GHA': 'Africa', 'GIN': 'Africa', 'GNB': 'Africa',
    'KEN': 'Africa', 'LSO': 'Africa', 'LBR': 'Africa',
    'LBY': 'Africa', 'MDG': 'Africa', 'MWI': 'Africa',
    'MLI': 'Africa', 'MRT': 'Africa', 'MUS': 'Africa',
    'MAR': 'Africa', 'MOZ': 'Africa', 'NAM': 'Africa',
    'NER': 'Africa', 'NGA': 'Africa', 'RWA': 'Africa',
    'STP': 'Africa', 'SEN': 'Africa', 'SLE': 'Africa',
    'SOM': 'Africa', 'ZAF': 'Africa', 'SSD': 'Africa',
    'SDN': 'Africa', 'TZA': 'Africa', 'TGO': 'Africa',
    'TUN': 'Africa', 'UGA': 'Africa', 'ZMB': 'Africa',
    'ZWE': 'Africa', 'REU': 'Africa', 'MYT': 'Africa',
    'SHN': 'Africa', 'DZA': 'Africa',
    # ── Asia ─────────────────────────────────────────────────
    'AFG': 'Asia', 'ARM': 'Asia', 'AZE': 'Asia',
    'BHR': 'Asia', 'BGD': 'Asia', 'BTN': 'Asia',
    'BRN': 'Asia', 'KHM': 'Asia', 'CHN': 'Asia',
    'CYP': 'Asia', 'GEO': 'Asia', 'HKG': 'Asia',
    'IND': 'Asia', 'IDN': 'Asia', 'IRN': 'Asia',
    'IRQ': 'Asia', 'ISR': 'Asia', 'JPN': 'Asia',
    'JOR': 'Asia', 'KAZ': 'Asia', 'KWT': 'Asia',
    'KGZ': 'Asia', 'LAO': 'Asia', 'LBN': 'Asia',
    'MAC': 'Asia', 'MYS': 'Asia', 'MDV': 'Asia',
    'MNG': 'Asia', 'MMR': 'Asia', 'NPL': 'Asia',
    'PRK': 'Asia', 'OMN': 'Asia', 'PAK': 'Asia',
    'PSE': 'Asia', 'PHL': 'Asia', 'QAT': 'Asia',
    'SAU': 'Asia', 'SGP': 'Asia', 'KOR': 'Asia',
    'LKA': 'Asia', 'SYR': 'Asia', 'TWN': 'Asia',
    'TJK': 'Asia', 'THA': 'Asia', 'TLS': 'Asia',
    'TUR': 'Asia', 'TKM': 'Asia', 'ARE': 'Asia',
    'UZB': 'Asia', 'VNM': 'Asia', 'YEM': 'Asia',
    'XKX': 'Europe',  # Kosovo — geographically Europe
    # ── Europe ───────────────────────────────────────────────
    'ALB': 'Europe', 'AND': 'Europe', 'AUT': 'Europe',
    'BLR': 'Europe', 'BEL': 'Europe', 'BIH': 'Europe',
    'BGR': 'Europe', 'HRV': 'Europe', 'CZE': 'Europe',
    'DNK': 'Europe', 'EST': 'Europe', 'FIN': 'Europe',
    'FRA': 'Europe', 'DEU': 'Europe', 'GRC': 'Europe',
    'HUN': 'Europe', 'ISL': 'Europe', 'IRL': 'Europe',
    'ITA': 'Europe', 'XKX': 'Europe', 'LVA': 'Europe',
    'LIE': 'Europe', 'LTU': 'Europe', 'LUX': 'Europe',
    'MLT': 'Europe', 'MDA': 'Europe', 'MCO': 'Europe',
    'MNE': 'Europe', 'NLD': 'Europe', 'MKD': 'Europe',
    'NOR': 'Europe', 'POL': 'Europe', 'PRT': 'Europe',
    'ROU': 'Europe', 'RUS': 'Europe', 'SMR': 'Europe',
    'SRB': 'Europe', 'SVK': 'Europe', 'SVN': 'Europe',
    'ESP': 'Europe', 'SWE': 'Europe', 'CHE': 'Europe',
    'UKR': 'Europe', 'GBR': 'Europe', 'VAT': 'Europe',
    'FRO': 'Europe', 'GIB': 'Europe', 'GGY': 'Europe',
    'IMN': 'Europe', 'JEY': 'Europe', 'ALA': 'Europe',
    # ── North America ────────────────────────────────────────
    'ATG': 'North America', 'BHS': 'North America',
    'BRB': 'North America', 'BLZ': 'North America',
    'CAN': 'North America', 'CRI': 'North America',
    'CUB': 'North America', 'DMA': 'North America',
    'DOM': 'North America', 'SLV': 'North America',
    'GRD': 'North America', 'GTM': 'North America',
    'HTI': 'North America', 'HND': 'North America',
    'JAM': 'North America', 'MEX': 'North America',
    'NIC': 'North America', 'PAN': 'North America',
    'KNA': 'North America', 'LCA': 'North America',
    'VCT': 'North America', 'TTO': 'North America',
    'USA': 'North America', 'AIA': 'North America',
    'BMU': 'North America', 'VGB': 'North America',
    'CYM': 'North America', 'GRL': 'North America',
    'MTQ': 'North America', 'MSR': 'North America',
    'PRI': 'North America', 'SPM': 'North America',
    'TCA': 'North America', 'VIR': 'North America',
    'ABW': 'North America', 'CUW': 'North America',
    'SXM': 'North America', 'BES': 'North America',
    'MAF': 'North America', 'BLM': 'North America',
    # ── South America ────────────────────────────────────────
    'ARG': 'South America', 'BOL': 'South America',
    'BRA': 'South America', 'CHL': 'South America',
    'COL': 'South America', 'ECU': 'South America',
    'GUY': 'South America', 'PRY': 'South America',
    'PER': 'South America', 'SUR': 'South America',
    'URY': 'South America', 'VEN': 'South America',
    'FLK': 'South America', 'GUF': 'South America',
    # ── Oceania ──────────────────────────────────────────────
    'AUS': 'Oceania', 'FJI': 'Oceania', 'KIR': 'Oceania',
    'MHL': 'Oceania', 'FSM': 'Oceania', 'NRU': 'Oceania',
    'NZL': 'Oceania', 'PLW': 'Oceania', 'PNG': 'Oceania',
    'WSM': 'Oceania', 'SLB': 'Oceania', 'TON': 'Oceania',
    'TUV': 'Oceania', 'VUT': 'Oceania', 'COK': 'Oceania',
    'PYF': 'Oceania', 'GUM': 'Oceania', 'NCL': 'Oceania',
    'NIU': 'Oceania', 'MNP': 'Oceania', 'ASM': 'Oceania',
    'TKL': 'Oceania', 'WLF': 'Oceania',
    # ── Antarctica ───────────────────────────────────────────
    'ATA': 'Antarctica',
    # ── European territories often missed ────────────────────
    'CHI': 'Europe',    # Channel Islands (Crown dependency, off Normandy)
    'SYC': 'Africa',    # Seychelles (island nation, Indian Ocean)
}


# ─────────────────────────────────────────────────────────────
# STEP 1 — PULL COUNTRIES FROM WORLD BANK API
# wbgapi.economy.DataFrame() returns all economies including
# aggregates (e.g. "World", "Sub-Saharan Africa").
# We filter to country-level only.
# ─────────────────────────────────────────────────────────────
print("Fetching countries from World Bank API...")

economies = wb.economy.DataFrame()
economies = economies.reset_index()

print(f"  Raw columns from wbgapi: {list(economies.columns)}")

# ─────────────────────────────────────────────────────────────
# STEP 2 — MAP COLUMN NAMES
# wbgapi column names vary by version.
# We detect and map them defensively.
# ─────────────────────────────────────────────────────────────
actual_cols = list(economies.columns)

col_map = {}

# iso3 — always the index, called 'id' after reset_index()
if 'id' in actual_cols:
    col_map['id'] = 'iso3'

# country name
for candidate in ['name', 'value', 'economy']:
    if candidate in actual_cols:
        col_map[candidate] = 'country_name'
        break

# region
for candidate in ['region', 'regionValue']:
    if candidate in actual_cols:
        col_map[candidate] = 'region'
        break

# income group
for candidate in ['incomeLevel', 'incomeLevelValue', 'income']:
    if candidate in actual_cols:
        col_map[candidate] = 'income_group'
        break

print(f"  Column mapping: {col_map}")

economies = economies.rename(columns=col_map)

# Verify required columns exist
required = ['iso3', 'country_name', 'region']
missing = [c for c in required if c not in economies.columns]
if missing:
    raise ValueError(
        f"Could not find required columns: {missing}. "
        f"Available: {list(economies.columns)}"
    )

if 'income_group' not in economies.columns:
    economies['income_group'] = None
    print("  ⚠ income_group column not found — setting to NULL")

# ─────────────────────────────────────────────────────────────
# STEP 3 — EXTRACT REGION AND INCOME_GROUP STRINGS
# wbgapi sometimes returns dicts instead of strings
# depending on version. We handle both cases.
# ─────────────────────────────────────────────────────────────
def extract_str(val):
    """Extract string from dict or return string directly."""
    if val is None:
        return None
    if isinstance(val, dict):
        # wbgapi may return {'id': 'LCN', 'value': 'Latin America'}
        return val.get('value') or val.get('id') or None
    s = str(val).strip()
    return None if s in ('', 'nan', 'None') else s

economies['region']       = economies['region'].apply(extract_str)
economies['income_group'] = economies['income_group'].apply(extract_str)
economies['iso3']         = economies['iso3'].apply(extract_str)
economies['country_name'] = economies['country_name'].apply(extract_str)

# ─────────────────────────────────────────────────────────────
# STEP 4 — FILTER OUT AGGREGATES
# World Bank marks aggregates with region = 'Aggregates'
# or with an income_group of None (regional groupings).
# We keep only individual country-level economies.
# ─────────────────────────────────────────────────────────────
countries = economies[
    economies['region'].notna() &
    ~economies['region'].str.contains('Aggregate', case=False, na=False)
].copy()

print(f"  Found {len(countries)} countries after excluding aggregates")

# Keep only schema columns
countries = countries[['iso3', 'country_name', 'region', 'income_group']].copy()

# Drop rows without iso3 (should not happen but defensive)
countries = countries[countries['iso3'].notna()].copy()

# ─────────────────────────────────────────────────────────────
# STEP 5 — ADD CONTINENT FROM STATIC MAPPING
# continent is a fixed geographic fact — never changes.
# Derived from ISO3_TO_CONTINENT defined above.
# Countries with no mapping get NULL (logged as a warning).
# ─────────────────────────────────────────────────────────────
countries['continent'] = countries['iso3'].map(ISO3_TO_CONTINENT)

no_continent = countries[countries['continent'].isna()]['iso3'].tolist()
if no_continent:
    print(
        f"  ⚠ {len(no_continent)} countries have no continent mapping "
        f"(will be NULL): {no_continent}. "
        f"Add them to ISO3_TO_CONTINENT in seed_countries.py."
    )
else:
    print(f"  ✓ All countries have continent mapping")

# ─────────────────────────────────────────────────────────────
# STEP 6 — ADD NOTES FOR EDGE CASES ONLY
# Notes are added ONLY for the 5 known edge cases.
# All other countries get NULL notes.
# ─────────────────────────────────────────────────────────────
notes_map = {
    'XKX': 'Kosovo — informal ISO code XKX used by World Bank. Not officially assigned by ISO 3166.',
    'TWN': 'Taiwan — listed as separate economy by World Bank.',
    'PSE': 'Palestine — listed as West Bank and Gaza by World Bank.',
    'HKG': 'Hong Kong SAR, China — listed separately from mainland China.',
    'MAC': 'Macao SAR, China — listed separately from mainland China.',
}

# Only apply notes to the 5 edge cases — all others get NULL
countries['notes'] = countries['iso3'].apply(
    lambda x: notes_map.get(x, None)
)

edge_case_count = countries['notes'].notna().sum()
print(f"  Edge cases with notes: {edge_case_count} (expected 5 or fewer)")

# ─────────────────────────────────────────────────────────────
# STEP 7 — SANITIZE NaN → None BEFORE DATABASE INSERT
#
# WHY THIS IS NECESSARY:
#   pandas.DataFrame.map() and .apply() produce float NaN for
#   unmatched values (e.g. a country with no continent mapping).
#   psycopg2 cannot adapt float NaN to a PostgreSQL NULL — it
#   raises a statement timeout or type error. Replacing NaN with
#   Python None before building the records list ensures every
#   missing value reaches the DB as a proper NULL.
# ─────────────────────────────────────────────────────────────
countries = countries.where(countries.notna(), other=None)

# ─────────────────────────────────────────────────────────────
# STEP 8 — BULK UPSERT INTO metadata.countries
#
# WHY ONE BULK INSERT NOT ROW-BY-ROW:
#   Supabase free tier has a statement_timeout (default 8s).
#   Row-by-row inserts in a loop can occasionally hit this limit
#   if the connection is slow. A single VALUES(...),(...),(...)
#   INSERT sends all 217 rows in one round trip, completing in
#   well under 1 second regardless of latency. This is also
#   significantly faster — one network round trip vs 217.
# ─────────────────────────────────────────────────────────────
print("Upserting into metadata.countries...")

records = countries.to_dict(orient='records')

with engine.connect() as conn:
    conn.execute(text("""
        INSERT INTO metadata.countries (
            iso3, country_name, region, continent, income_group, notes
        )
        VALUES (
            :iso3, :country_name, :region, :continent, :income_group, :notes
        )
        ON CONFLICT (iso3) DO UPDATE SET
            country_name = EXCLUDED.country_name,
            region       = EXCLUDED.region,
            continent    = EXCLUDED.continent,
            income_group = EXCLUDED.income_group,
            notes        = EXCLUDED.notes
    """), records)
    conn.commit()

print(f"✓ Upserted {len(countries)} countries into metadata.countries")

# ─────────────────────────────────────────────────────────────
# STEP 7 — VERIFY
# ─────────────────────────────────────────────────────────────
result = pd.read_sql("""
    SELECT
        COUNT(*)                                       AS total_countries,
        COUNT(DISTINCT region)                         AS regions,
        COUNT(DISTINCT continent)                      AS continents,
        COUNT(DISTINCT income_group)                   AS income_groups,
        COUNT(*) FILTER (WHERE continent IS NULL)      AS missing_continent,
        COUNT(*) FILTER (WHERE notes IS NOT NULL)      AS countries_with_notes
    FROM metadata.countries
""", engine)

print("\nSummary:")
print(result.to_string(index=False))

# Verify edge cases
edge_cases = pd.read_sql("""
    SELECT iso3, country_name, notes
    FROM metadata.countries
    WHERE notes IS NOT NULL
    ORDER BY iso3
""", engine)

if len(edge_cases) > 0:
    print(f"\nCountries with notes ({len(edge_cases)}):")
    print(edge_cases.to_string(index=False))
else:
    print("\nNo countries with notes found.")
