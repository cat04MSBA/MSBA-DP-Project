"""
seed_country_codes.py
=====================
Populates metadata.country_codes — the crosswalk between
every source's country identifier and ISO3.

FOURTH seed script to run.
Requires metadata.sources and metadata.countries to exist first.

Safe to re-run — uses upsert logic throughout.

HOW EACH SOURCE IS HANDLED:
    world_bank  Uses ISO3 directly → trivial 1:1 mapping
    imf         Uses ISO2-like codes → mapped via pycountry
    pwt         Uses country names → read from Stata file
    oxford      Uses ISO3 directly → trivial 1:1 mapping
    openalex    Uses ISO2 → mapped via pycountry
    wipo_ip     Uses ISO3 directly → trivial 1:1 mapping
    oecd_msti   Uses ISO3 directly → trivial 1:1 mapping

pycountry is used ONLY at seeding time to convert codes.
After seeding, transformation scripts use this table directly —
no pycountry calls needed in ingestion scripts.

This design means adding any new source (ISO2, numeric, names,
custom) just requires adding rows here. Zero schema changes ever.
"""

import requests
import pandas as pd
import pycountry
from sqlalchemy import text
from database.connection import get_engine

engine = get_engine()


# ═══════════════════════════════════════════════════════════════
# HELPER: UPSERT ONE COUNTRY CODE
# ═══════════════════════════════════════════════════════════════
def upsert_code(conn, iso3: str, source_id: str, code: str):
    """Insert or update one row in metadata.country_codes."""
    conn.execute(text("""
        INSERT INTO metadata.country_codes (iso3, source_id, code)
        VALUES (:iso3, :source_id, :code)
        ON CONFLICT (iso3, source_id) DO UPDATE SET
            code = EXCLUDED.code
    """), {'iso3': iso3, 'source_id': source_id, 'code': code})


# ═══════════════════════════════════════════════════════════════
# HELPER: LOAD ALL ISO3 CODES FROM DATABASE
# Used as the validation set — we only insert codes for
# countries that exist in metadata.countries.
# ═══════════════════════════════════════════════════════════════
def load_countries():
    return pd.read_sql("""
        SELECT iso3, country_name
        FROM metadata.countries
        ORDER BY iso3
    """, engine)


# ═══════════════════════════════════════════════════════════════
# HELPER: ISO2 TO ISO3 VIA PYCOUNTRY
# Returns None if code cannot be resolved.
# ═══════════════════════════════════════════════════════════════
def iso2_to_iso3(iso2_code: str):
    """Convert ISO2 code to ISO3 using pycountry."""
    try:
        country = pycountry.countries.get(alpha_2=iso2_code.upper())
        return country.alpha_3 if country else None
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════
# 1. WORLD BANK COUNTRY CODES
# World Bank uses ISO3 directly — trivial 1:1 mapping.
# For every country in our table, wb_code = iso3.
# ═══════════════════════════════════════════════════════════════
def seed_wb_country_codes(conn, countries: pd.DataFrame):
    print("\n── World Bank country codes ──")
    count = 0
    for _, row in countries.iterrows():
        upsert_code(conn, row['iso3'], 'world_bank', row['iso3'])
        count += 1
    print(f"  ✓ Upserted {count} World Bank country codes")


# ═══════════════════════════════════════════════════════════════
# 2. IMF COUNTRY CODES
# IMF DataMapper API returns country codes.
# IMF uses mostly ISO2-like codes with some custom ones.
# We match to ISO3 via pycountry first, then by name.
# ═══════════════════════════════════════════════════════════════
def seed_imf_country_codes(conn, countries: pd.DataFrame):
    print("\n── IMF country codes ──")

    url = "https://www.imf.org/external/datamapper/api/v1/countries"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"  ✗ Could not fetch IMF countries: {e}")
        raise

    imf_countries = data.get('countries', {})

    # Build name → iso3 lookup from our countries table
    name_to_iso3 = {
        row['country_name'].lower().strip(): row['iso3']
        for _, row in countries.iterrows()
    }

    # Build iso3 set for fast lookup
    valid_iso3 = set(countries['iso3'].tolist())

    # Manual overrides for IMF name variants that don't resolve
    # via ISO code lookup or name matching
    IMF_MANUAL = {
        'taiwan province of china': 'TWN',
    }

    count = 0
    unmatched = []

    for imf_code, info in imf_countries.items():
        if not info or not isinstance(info, dict) or not imf_code:
            continue
        label = info.get('label') or ''
        if not isinstance(label, str):
            label = str(label)
        label = label.strip()
        if not label:
            continue

        iso3 = None

        # Try 1: if 3-letter code, try it directly as ISO3
        if len(imf_code) == 3 and imf_code in valid_iso3:
            iso3 = imf_code

        # Try 2: if 2-letter code, convert via pycountry
        if not iso3 and len(imf_code) == 2:
            iso3 = iso2_to_iso3(imf_code)

        # Try 3: match by country name
        if not iso3 or iso3 not in valid_iso3:
            iso3 = name_to_iso3.get(label.lower())

        # Try 4: manual overrides
        if not iso3 or iso3 not in valid_iso3:
            iso3 = IMF_MANUAL.get(label.lower())

        if iso3 and iso3 in valid_iso3:
            upsert_code(conn, iso3, 'imf', imf_code)
            count += 1
        else:
            unmatched.append(f"{imf_code}: {label}")

    print(f"  ✓ Upserted {count} IMF country codes")
    if unmatched:
        print(f"  ⚠ {len(unmatched)} IMF codes could not be matched to ISO3:")
        for u in unmatched[:5]:
            print(f"    - {u}")
        if len(unmatched) > 5:
            print(f"    ... and {len(unmatched) - 5} more")



# ═══════════════════════════════════════════════════════════════
# 3. PWT COUNTRY CODES
# PWT uses country names as identifiers (e.g. "Lebanon").
# We read the Stata file to get the iso3 → name mapping.
# PWT's countrycode column IS ISO3 — we use it directly.
# ═══════════════════════════════════════════════════════════════
# PWT 11.0 country name mapping — extracted from pwt110.dta
# Hardcoded to avoid dependency on file download at seeding time.
# PWT uses its own country names as identifiers in the data file.
# Transformation scripts look up by this name via metadata.country_codes.
PWT_COUNTRY_NAMES = {
    'ABW': 'Aruba', 'AGO': 'Angola', 'AIA': 'Anguilla',
    'ALB': 'Albania', 'ARE': 'United Arab Emirates', 'ARG': 'Argentina',
    'ARM': 'Armenia', 'ATG': 'Antigua and Barbuda', 'AUS': 'Australia',
    'AUT': 'Austria', 'AZE': 'Azerbaijan', 'BDI': 'Burundi',
    'BEL': 'Belgium', 'BEN': 'Benin', 'BFA': 'Burkina Faso',
    'BGD': 'Bangladesh', 'BGR': 'Bulgaria', 'BHR': 'Bahrain',
    'BHS': 'Bahamas', 'BIH': 'Bosnia and Herzegovina', 'BLR': 'Belarus',
    'BLZ': 'Belize', 'BMU': 'Bermuda', 'BOL': 'Bolivia (Plurinational State of)',
    'BRA': 'Brazil', 'BRB': 'Barbados', 'BRN': 'Brunei Darussalam',
    'BTN': 'Bhutan', 'BWA': 'Botswana', 'CAF': 'Central African Republic',
    'CAN': 'Canada', 'CHE': 'Switzerland', 'CHL': 'Chile',
    'CHN': 'China', 'CIV': "Côte d'Ivoire", 'CMR': 'Cameroon',
    'COD': 'D.R. of the Congo', 'COG': 'Congo', 'COL': 'Colombia',
    'COM': 'Comoros', 'CPV': 'Cabo Verde', 'CRI': 'Costa Rica',
    'CUW': 'Curaçao', 'CYM': 'Cayman Islands', 'CYP': 'Cyprus',
    'CZE': 'Czechia', 'DEU': 'Germany', 'DJI': 'Djibouti',
    'DMA': 'Dominica', 'DNK': 'Denmark', 'DOM': 'Dominican Republic',
    'DZA': 'Algeria', 'ECU': 'Ecuador', 'EGY': 'Egypt',
    'ESP': 'Spain', 'EST': 'Estonia', 'ETH': 'Ethiopia',
    'FIN': 'Finland', 'FJI': 'Fiji', 'FRA': 'France',
    'GAB': 'Gabon', 'GBR': 'United Kingdom', 'GEO': 'Georgia',
    'GHA': 'Ghana', 'GIN': 'Guinea', 'GMB': 'Gambia',
    'GNB': 'Guinea-Bissau', 'GNQ': 'Equatorial Guinea', 'GRC': 'Greece',
    'GRD': 'Grenada', 'GTM': 'Guatemala', 'GUY': 'Guyana',
    'HKG': 'China, Hong Kong SAR', 'HND': 'Honduras', 'HRV': 'Croatia',
    'HTI': 'Haiti', 'HUN': 'Hungary', 'IDN': 'Indonesia',
    'IND': 'India', 'IRL': 'Ireland', 'IRN': 'Iran (Islamic Republic of)',
    'IRQ': 'Iraq', 'ISL': 'Iceland', 'ISR': 'Israel',
    'ITA': 'Italy', 'JAM': 'Jamaica', 'JOR': 'Jordan',
    'JPN': 'Japan', 'KAZ': 'Kazakhstan', 'KEN': 'Kenya',
    'KGZ': 'Kyrgyzstan', 'KHM': 'Cambodia', 'KNA': 'Saint Kitts and Nevis',
    'KOR': 'Republic of Korea', 'KWT': 'Kuwait', 'LAO': "Lao People's DR",
    'LBN': 'Lebanon', 'LBR': 'Liberia', 'LCA': 'Saint Lucia',
    'LKA': 'Sri Lanka', 'LSO': 'Lesotho', 'LTU': 'Lithuania',
    'LUX': 'Luxembourg', 'LVA': 'Latvia', 'MAC': 'China, Macao SAR',
    'MAR': 'Morocco', 'MDA': 'Republic of Moldova', 'MDG': 'Madagascar',
    'MDV': 'Maldives', 'MEX': 'Mexico', 'MKD': 'North Macedonia',
    'MLI': 'Mali', 'MLT': 'Malta', 'MMR': 'Myanmar',
    'MNE': 'Montenegro', 'MNG': 'Mongolia', 'MOZ': 'Mozambique',
    'MRT': 'Mauritania', 'MSR': 'Montserrat', 'MUS': 'Mauritius',
    'MWI': 'Malawi', 'MYS': 'Malaysia', 'NAM': 'Namibia',
    'NER': 'Niger', 'NGA': 'Nigeria', 'NIC': 'Nicaragua',
    'NLD': 'Netherlands', 'NOR': 'Norway', 'NPL': 'Nepal',
    'NZL': 'New Zealand', 'OMN': 'Oman', 'PAK': 'Pakistan',
    'PAN': 'Panama', 'PER': 'Peru', 'PHL': 'Philippines',
    'POL': 'Poland', 'PRT': 'Portugal', 'PRY': 'Paraguay',
    'PSE': 'State of Palestine', 'QAT': 'Qatar', 'ROU': 'Romania',
    'RUS': 'Russian Federation', 'RWA': 'Rwanda', 'SAU': 'Saudi Arabia',
    'SDN': 'Sudan', 'SEN': 'Senegal', 'SGP': 'Singapore',
    'SLE': 'Sierra Leone', 'SLV': 'El Salvador', 'SOM': 'Somalia',
    'SRB': 'Serbia', 'SSD': 'South Sudan', 'STP': 'Sao Tome and Principe',
    'SUR': 'Suriname', 'SVK': 'Slovakia', 'SVN': 'Slovenia',
    'SWE': 'Sweden', 'SWZ': 'Eswatini', 'SXM': 'Sint Maarten (Dutch part)',
    'SYC': 'Seychelles', 'SYR': 'Syrian Arab Republic', 'TCA': 'Turks and Caicos Islands',
    'TCD': 'Chad', 'TGO': 'Togo', 'THA': 'Thailand',
    'TJK': 'Tajikistan', 'TKM': 'Turkmenistan', 'TTO': 'Trinidad and Tobago',
    'TUN': 'Tunisia', 'TUR': 'Türkiye', 'TWN': 'Taiwan',
    'TZA': 'U.R. of Tanzania: Mainland', 'UGA': 'Uganda', 'UKR': 'Ukraine',
    'URY': 'Uruguay', 'USA': 'United States', 'UZB': 'Uzbekistan',
    'VCT': 'St. Vincent and the Grenadines', 'VEN': 'Venezuela (Bolivarian Republic of)',
    'VGB': 'British Virgin Islands', 'VNM': 'Viet Nam',
    'YEM': 'Yemen', 'ZAF': 'South Africa', 'ZMB': 'Zambia', 'ZWE': 'Zimbabwe',
}


def seed_pwt_country_codes(conn, countries: pd.DataFrame):
    print("\n── PWT country codes ──")

    valid_iso3 = set(countries['iso3'].tolist())
    count = 0
    unmatched = []

    for iso3, pwt_name in PWT_COUNTRY_NAMES.items():
        if iso3 in valid_iso3:
            upsert_code(conn, iso3, 'pwt', pwt_name)
            count += 1
        else:
            unmatched.append(f"{iso3}: {pwt_name}")

    print(f"  ✓ Upserted {count} PWT country codes")
    if unmatched:
        print(f"  ⚠ {len(unmatched)} PWT countries not in our table:")
        for u in unmatched[:5]:
            print(f"    - {u}")


# ═══════════════════════════════════════════════════════════════
# 4. OXFORD COUNTRY CODES
# Oxford uses ISO3 directly — trivial 1:1 mapping.
# ═══════════════════════════════════════════════════════════════
def seed_oxford_country_codes(conn, countries: pd.DataFrame):
    print("\n── Oxford country codes ──")
    count = 0
    for _, row in countries.iterrows():
        upsert_code(conn, row['iso3'], 'oxford', row['iso3'])
        count += 1
    print(f"  ✓ Upserted {count} Oxford country codes")


# ═══════════════════════════════════════════════════════════════
# 5. OPENALEX COUNTRY CODES
# OpenAlex returns ISO2 codes (e.g. CN, US, LB).
# We convert ISO2 → ISO3 via pycountry and store both.
# This means transformation scripts can look up any OpenAlex
# ISO2 code in metadata.country_codes to get ISO3.
# ═══════════════════════════════════════════════════════════════
def seed_openalex_country_codes(conn, countries: pd.DataFrame):
    print("\n── OpenAlex country codes ──")

    valid_iso3 = set(countries['iso3'].tolist())
    count = 0
    unmatched = []

    # Get all countries from pycountry and create ISO2 → ISO3 mapping
    # for every country in our table
    for _, row in countries.iterrows():
        iso3 = row['iso3']

        # Try to find ISO2 via pycountry
        try:
            country = pycountry.countries.get(alpha_3=iso3)
            if country and hasattr(country, 'alpha_2'):
                iso2 = country.alpha_2
                upsert_code(conn, iso3, 'openalex', iso2)
                count += 1
            else:
                unmatched.append(iso3)
        except Exception:
            unmatched.append(iso3)

    print(f"  ✓ Upserted {count} OpenAlex country codes")
    if unmatched:
        print(f"  ⚠ {len(unmatched)} countries have no ISO2 mapping via pycountry")


# ═══════════════════════════════════════════════════════════════
# 6. WIPO IP COUNTRY CODES
# WIPO IP Statistics uses ISO3 directly — trivial 1:1 mapping.
# ═══════════════════════════════════════════════════════════════
def seed_wipo_country_codes(conn, countries: pd.DataFrame):
    print("\n── WIPO IP country codes ──")
    count = 0
    for _, row in countries.iterrows():
        upsert_code(conn, row['iso3'], 'wipo_ip', row['iso3'])
        count += 1
    print(f"  ✓ Upserted {count} WIPO IP country codes")


# ═══════════════════════════════════════════════════════════════
# 7. OECD MSTI COUNTRY CODES
# OECD MSTI uses ISO3 directly — trivial 1:1 mapping.
# Only ~38 OECD members + selected non-members have data.
# We still insert all countries from our table for consistency.
# Countries with no OECD data simply have no observations —
# that is handled at ingestion time, not here.
# ═══════════════════════════════════════════════════════════════
def seed_oecd_country_codes(conn, countries: pd.DataFrame):
    print("\n── OECD MSTI country codes ──")
    count = 0
    for _, row in countries.iterrows():
        upsert_code(conn, row['iso3'], 'oecd_msti', row['iso3'])
        count += 1
    print(f"  ✓ Upserted {count} OECD MSTI country codes")


# ═══════════════════════════════════════════════════════════════
# MAIN — RUN ALL IN ORDER
# ═══════════════════════════════════════════════════════════════
print("Seeding metadata.country_codes...")
print("=" * 50)

# Load all countries once — used by all functions
countries = load_countries()
print(f"Loaded {len(countries)} countries from metadata.countries")

with engine.connect() as conn:
    seed_wb_country_codes(conn, countries)
    seed_imf_country_codes(conn, countries)
    seed_pwt_country_codes(conn, countries)
    seed_oxford_country_codes(conn, countries)
    seed_openalex_country_codes(conn, countries)
    seed_wipo_country_codes(conn, countries)
    seed_oecd_country_codes(conn, countries)
    conn.commit()

print("\n" + "=" * 50)

# ─────────────────────────────────────────────────────────────
# VERIFY
# ─────────────────────────────────────────────────────────────
result = pd.read_sql("""
    SELECT source_id, COUNT(*) AS country_code_count
    FROM metadata.country_codes
    GROUP BY source_id
    ORDER BY source_id
""", engine)

print("\nCountry codes per source:")
print(result.to_string(index=False))

total = pd.read_sql(
    "SELECT COUNT(*) AS total FROM metadata.country_codes", engine
)
print(f"\nTotal country codes: {total['total'].iloc[0]}")