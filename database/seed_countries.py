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
import requests
import pandas as pd
from sqlalchemy import text
from database.connection import get_engine

engine = get_engine()

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
# STEP 5 — ADD NOTES FOR EDGE CASES ONLY
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
# STEP 6 — UPSERT INTO metadata.countries
# ─────────────────────────────────────────────────────────────
print("Upserting into metadata.countries...")

with engine.connect() as conn:
    for _, row in countries.iterrows():
        conn.execute(text("""
            INSERT INTO metadata.countries (
                iso3, country_name, region, income_group, notes
            )
            VALUES (
                :iso3, :country_name, :region, :income_group, :notes
            )
            ON CONFLICT (iso3) DO UPDATE SET
                country_name = EXCLUDED.country_name,
                region       = EXCLUDED.region,
                income_group = EXCLUDED.income_group,
                notes        = EXCLUDED.notes
        """), row.to_dict())
    conn.commit()

print(f"✓ Upserted {len(countries)} countries into metadata.countries")

# ─────────────────────────────────────────────────────────────
# STEP 7 — VERIFY
# ─────────────────────────────────────────────────────────────
result = pd.read_sql("""
    SELECT
        COUNT(*)                                       AS total_countries,
        COUNT(DISTINCT region)                         AS regions,
        COUNT(DISTINCT income_group)                   AS income_groups,
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
