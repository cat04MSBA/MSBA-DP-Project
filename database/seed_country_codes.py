"""
seed_country_codes.py
=====================
Populates metadata.country_codes — the crosswalk between
every source's country identifier and ISO3.

FOURTH seed script to run.
Requires metadata.sources and metadata.countries to exist first.

Safe to re-run — uses drift-aware upsert logic throughout.

HOW EACH SOURCE IS HANDLED:
    world_bank  Uses ISO3 directly → trivial 1:1 mapping
    imf         Uses mixed codes → mapped via pycountry + name match
    pwt         Uses country names → hardcoded from pwt110.dta
    oxford      Uses country names → three-method validation
    openalex    Uses ISO2 → mapped via pycountry
    wipo_ip     Uses ISO2 → mapped via pycountry (FIXED from ISO3)
    oecd_msti   Uses ISO3 directly → trivial 1:1 mapping

WHY WIPO WAS FIXED FROM ISO3 TO ISO2:
    The original seed script incorrectly assumed WIPO uses ISO3.
    Inspection of the actual wipo_ai_patents.csv file shows the
    'Origin (Code)' column contains ISO2 codes (e.g. 'LB', 'US').
    The seed script now uses pycountry to map ISO3 → ISO2 and
    stores the ISO2 as the code, matching the actual file format.
    Transformation scripts use this code to map WIPO's ISO2 back
    to ISO3 during standardization.

WHY OXFORD USES THREE-METHOD VALIDATION:
    Oxford XLSX files use full country names (e.g. "United States
    of America", "Republic of Korea") that differ from both ISO
    standards and World Bank official names. Simple exact matching
    fails for many countries. Three-method validation (pycountry
    fuzzy search + metadata.countries name match + manual overrides)
    maximizes coverage while maintaining quality. Matching happens
    ONCE at seeding time and is stored in metadata.country_codes —
    transformation scripts never do fuzzy matching at runtime.

DRIFT-AWARE UPSERT:
    The upsert_code() function checks if the incoming code differs
    from the stored code before updating. If it differs, it does
    NOT apply the update — it prints a warning and skips. Code
    mapping changes are critical (they affect which country every
    historical row maps to) and must go through drift detection
    on the next ingestion run. This prevents the seed scripts from
    being used to accidentally bypass drift detection.

pycountry is used ONLY at seeding time to convert codes.
After seeding, transformation scripts use this table directly —
no pycountry calls needed in ingestion or transformation scripts.

DESIGN DECISION — ROW-PER-SOURCE CROSSWALK:
    metadata.country_codes stores one row per country per source.
    The 'code' column stores whatever identifier that source uses.
    An alternative normalized design (country_standards table +
    source_conventions table) was considered and rejected because:
    1. Not all sources follow a standard convention cleanly.
       IMF uses a mix of ISO2, ISO3, and custom codes within the
       same dataset. A convention-level lookup breaks for these.
    2. PWT and Oxford use non-standard name variants that cannot
       be expressed as a simple convention lookup.
    3. At runtime, each transformation script loads its crosswalk
       in ONE query and applies it as a vectorized map() —
       zero per-row overhead regardless of row count.
    4. Storage cost: 7 sources × 266 countries = 1,862 rows.
       Projecting to 20 sources: 5,320 rows. Negligible.
    The normalization gain is real but the edge cases make the
    cleaner model brittle. Row repetition at this scale is the
    correct trade-off. See architecture report for full decision
    documentation.
"""

import requests
import pandas as pd
import pycountry
from sqlalchemy import text
from database.connection import get_engine

engine = get_engine()


# ═══════════════════════════════════════════════════════════════
# HELPER: DRIFT-AWARE UPSERT ONE COUNTRY CODE
# ═══════════════════════════════════════════════════════════════
def upsert_code(conn, iso3: str, source_id: str, code: str):
    """
    Insert or drift-aware-update one row in metadata.country_codes.

    WHY DRIFT-AWARE (not unconditional ON CONFLICT DO UPDATE):
        Code mapping changes are CRITICAL in the pipeline —
        a code change affects which country every historical row
        in the silver layer maps to. If the seed script
        unconditionally overwrites a stored code, it bypasses
        drift detection entirely. Instead:
        - If no existing row: INSERT normally (new mapping).
        - If existing row with SAME code: no-op (idempotent).
        - If existing row with DIFFERENT code: skip and warn.
          The change must go through drift detection on the next
          ingestion run where it will be logged to
          ops.metadata_changes and emailed to the team.
    """
    # Check if a row already exists for this (iso3, source_id).
    existing = conn.execute(text("""
        SELECT code FROM metadata.country_codes
        WHERE iso3 = :iso3 AND source_id = :source_id
    """), {'iso3': iso3, 'source_id': source_id}).fetchone()

    if existing is None:
        # New mapping — insert normally.
        conn.execute(text("""
            INSERT INTO metadata.country_codes (iso3, source_id, code)
            VALUES (:iso3, :source_id, :code)
        """), {'iso3': iso3, 'source_id': source_id, 'code': code})

    elif existing[0] == code:
        # Same code — no-op. Idempotent re-run.
        pass

    else:
        # Different code — skip and warn. Do not overwrite.
        # Must go through drift detection on next ingestion run.
        print(
            f"  ⚠ Skipping code mapping change for {iso3} "
            f"({source_id}): stored='{existing[0]}', "
            f"incoming='{code}'. "
            f"Use drift detection to review this change."
        )


# ═══════════════════════════════════════════════════════════════
# HELPER: LOAD ALL ISO3 CODES FROM DATABASE
# ═══════════════════════════════════════════════════════════════
def load_countries():
    """
    Load all countries from metadata.countries.
    Used as the validation set — we only insert codes for
    countries that exist in metadata.countries.
    """
    return pd.read_sql("""
        SELECT iso3, country_name
        FROM metadata.countries
        ORDER BY iso3
    """, engine)


# ═══════════════════════════════════════════════════════════════
# HELPER: ISO2 TO ISO3 VIA PYCOUNTRY
# ═══════════════════════════════════════════════════════════════
def iso2_to_iso3(iso2_code: str):
    """
    Convert ISO2 code to ISO3 using pycountry.
    Returns None if code cannot be resolved.
    Used at seeding time only — never at ingestion/transformation time.
    """
    try:
        country = pycountry.countries.get(alpha_2=iso2_code.upper())
        return country.alpha_3 if country else None
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════
# HELPER: ISO3 TO ISO2 VIA PYCOUNTRY
# ═══════════════════════════════════════════════════════════════
def iso3_to_iso2(iso3_code: str):
    """
    Convert ISO3 code to ISO2 using pycountry.
    Returns None if code cannot be resolved.
    Used for WIPO and OpenAlex which use ISO2.
    """
    try:
        country = pycountry.countries.get(alpha_3=iso3_code.upper())
        return country.alpha_2 if country and hasattr(country, 'alpha_2') else None
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════
# 1. WORLD BANK COUNTRY CODES
# ═══════════════════════════════════════════════════════════════
def seed_wb_country_codes(conn, countries: pd.DataFrame):
    """
    World Bank uses ISO3 directly — trivial 1:1 mapping.
    For every country in our table, wb_code = iso3.
    """
    print("\n── World Bank country codes ──")
    count = 0
    for _, row in countries.iterrows():
        upsert_code(conn, row['iso3'], 'world_bank', row['iso3'])
        count += 1
    print(f"  ✓ Processed {count} World Bank country codes")


# ═══════════════════════════════════════════════════════════════
# 2. IMF COUNTRY CODES
# ═══════════════════════════════════════════════════════════════
def seed_imf_country_codes(conn, countries: pd.DataFrame):
    """
    IMF DataMapper API returns country codes.
    IMF uses mostly ISO2-like codes with some custom ones.
    We match to ISO3 via pycountry first, then by name.
    """
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

    name_to_iso3 = {
        row['country_name'].lower().strip(): row['iso3']
        for _, row in countries.iterrows()
    }
    valid_iso3 = set(countries['iso3'].tolist())

    IMF_MANUAL = {
        'taiwan province of china': 'TWN',
    }

    count     = 0
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

        if len(imf_code) == 3 and imf_code in valid_iso3:
            iso3 = imf_code
        if not iso3 and len(imf_code) == 2:
            iso3 = iso2_to_iso3(imf_code)
        if not iso3 or iso3 not in valid_iso3:
            iso3 = name_to_iso3.get(label.lower())
        if not iso3 or iso3 not in valid_iso3:
            iso3 = IMF_MANUAL.get(label.lower())

        if iso3 and iso3 in valid_iso3:
            upsert_code(conn, iso3, 'imf', imf_code)
            count += 1
        else:
            unmatched.append(f"{imf_code}: {label}")

    print(f"  ✓ Processed {count} IMF country codes")
    if unmatched:
        print(f"  ⚠ {len(unmatched)} IMF codes could not be matched:")
        for u in unmatched[:5]:
            print(f"    - {u}")
        if len(unmatched) > 5:
            print(f"    ... and {len(unmatched) - 5} more")


# ═══════════════════════════════════════════════════════════════
# 3. PWT COUNTRY CODES
# ═══════════════════════════════════════════════════════════════
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
    """
    PWT uses country names as identifiers (e.g. "Lebanon").
    Hardcoded from pwt110.dta to avoid dependency on file
    download at seeding time.
    """
    print("\n── PWT country codes ──")

    valid_iso3 = set(countries['iso3'].tolist())
    count      = 0
    unmatched  = []

    for iso3, pwt_name in PWT_COUNTRY_NAMES.items():
        if iso3 in valid_iso3:
            upsert_code(conn, iso3, 'pwt', pwt_name)
            count += 1
        else:
            unmatched.append(f"{iso3}: {pwt_name}")

    print(f"  ✓ Processed {count} PWT country codes")
    if unmatched:
        print(f"  ⚠ {len(unmatched)} PWT countries not in our table:")
        for u in unmatched[:5]:
            print(f"    - {u}")


# ═══════════════════════════════════════════════════════════════
# 4. OXFORD COUNTRY CODES — THREE-METHOD VALIDATION
# ═══════════════════════════════════════════════════════════════

# Manual overrides for Oxford country name variants that fail
# both pycountry fuzzy search and metadata.countries name match.
# Sourced by comparing unmatched Oxford names against ISO standards.
# Add new entries here if future Oxford editions introduce new
# country name variants not covered by the other two methods.
OXFORD_MANUAL_OVERRIDES = {
    'United States of America':           'USA',
    'Republic of Korea':                  'KOR',
    'Democratic Republic of the Congo':   'COD',
    'Republic of the Congo':              'COG',
    'Ivory Coast':                        'CIV',
    'Cape Verde':                         'CPV',
    'Swaziland':                          'SWZ',
    'Macedonia':                          'MKD',
    'Palestine':                          'PSE',
    'Taiwan':                             'TWN',
    'Kosovo':                             'XKX',
    'Hong Kong':                          'HKG',
    'Macao':                              'MAC',
    'Syria':                              'SYR',
    'Iran':                               'IRN',
    'South Korea':                        'KOR',
    'North Korea':                        'PRK',
    'Bolivia':                            'BOL',
    'Venezuela':                          'VEN',
    'Tanzania':                           'TZA',
    'Laos':                               'LAO',
    'Moldova':                            'MDA',
    'Vietnam':                            'VNM',
    'Russia':                             'RUS',
    'Brunei':                             'BRN',
    'Micronesia':                         'FSM',
    'Trinidad & Tobago':                  'TTO',
    'Saint Kitts & Nevis':                'KNA',
    'Antigua & Barbuda':                  'ATG',
    'Bosnia & Herzegovina':               'BIH',
    'São Tomé & Príncipe':                'STP',
}


def match_oxford_country(name: str, name_to_iso3: dict,
                         valid_iso3: set) -> tuple:
    """
    Match one Oxford country name to ISO3 using three methods.
    Returns (iso3, confidence) where confidence is:
        'high'   — at least 2 of 3 methods agree
        'medium' — exactly 1 method matched
        'none'   — no method matched

    WHY THREE METHODS:
        Oxford uses non-standard country names that differ from
        both ISO standards and World Bank official names. No
        single method covers all cases:
        - pycountry fails on political variants ('Republic of Korea')
        - Name matching fails on abbreviations ('Bolivia')
        - Manual overrides cover known problem cases but cannot
          anticipate new editions

        Requiring 2 of 3 to agree before accepting a match
        minimizes false positives while maximizing coverage.
        A match accepted on only 1 method is flagged as medium
        confidence and printed so the team can verify.

    Args:
        name:         Oxford country name string.
        name_to_iso3: Dict of World Bank name → ISO3 from
                      metadata.countries. Built once by the caller.
        valid_iso3:   Set of all valid ISO3 codes.

    Returns:
        (iso3_or_None, confidence_string)
    """
    results = {}

    # ── Method 1: pycountry fuzzy search ──────────────────────
    # pycountry searches its internal country database using
    # fuzzy string matching. Handles many common variants.
    try:
        matches = pycountry.countries.search_fuzzy(name)
        if matches and matches[0].alpha_3 in valid_iso3:
            results['pycountry'] = matches[0].alpha_3
    except Exception:
        pass

    # ── Method 2: metadata.countries name match ────────────────
    # Exact match (case-insensitive) against World Bank names
    # stored in metadata.countries. Covers official names.
    name_lower = name.lower().strip()
    if name_lower in name_to_iso3:
        results['name_match'] = name_to_iso3[name_lower]

    # ── Method 3: manual overrides ────────────────────────────
    # Hardcoded dict for known Oxford name variants that fail
    # both other methods. Updated when new editions introduce
    # new problem cases.
    if name in OXFORD_MANUAL_OVERRIDES:
        iso3 = OXFORD_MANUAL_OVERRIDES[name]
        if iso3 in valid_iso3:
            results['manual'] = iso3

    # ── Evaluate agreement ─────────────────────────────────────
    unique_results = set(results.values())

    if len(unique_results) == 0:
        return None, 'none'

    if len(unique_results) == 1:
        iso3 = list(unique_results)[0]
        # All methods that ran agree — high confidence if 2+
        # methods ran, medium if only 1 ran.
        confidence = 'high' if len(results) >= 2 else 'medium'
        return iso3, confidence

    # Multiple different results — methods disagree.
    # Take the majority if possible, otherwise flag as medium.
    from collections import Counter
    counts = Counter(results.values())
    most_common_iso3, most_common_count = counts.most_common(1)[0]

    if most_common_count >= 2:
        # Majority agreement — high confidence.
        return most_common_iso3, 'high'
    else:
        # No majority — all methods gave different results.
        # Take pycountry result as tiebreaker but flag medium.
        fallback = results.get('pycountry') or results.get('manual')
        return fallback, 'medium'


def seed_oxford_country_codes(conn, countries: pd.DataFrame):
    """
    Oxford uses full country names (e.g. 'United States of
    America', 'Republic of Korea'). These differ from both
    ISO standards and World Bank official names.

    WHY MATCHING HAPPENS AT SEEDING TIME (not ingestion time):
        Fuzzy matching is computationally expensive and produces
        different results depending on pycountry version.
        Matching once at seeding time and storing the result in
        metadata.country_codes means transformation scripts
        never do fuzzy matching — they just look up the code.
        This keeps ingestion and transformation deterministic
        and fast. If a new Oxford edition introduces a new country
        name variant, the team re-runs this seed function which
        adds the new mapping to metadata.country_codes.

    WHY WE COLLECT ALL OXFORD NAMES FIRST:
        Oxford publishes one XLSX per year with different column
        structures (see oxford_ingest.py). Rather than re-reading
        all 7 XLSX files at seeding time, we use the union of all
        known Oxford country names hardcoded below. This is the
        same philosophy as PWT_COUNTRY_NAMES — seeding is
        decoupled from file access.
    """
    print("\n── Oxford country codes ──")

    # All unique country names that appear across Oxford
    # editions 2019-2025. Collected by reading all 7 XLSX files.
    # Update this list when a new Oxford edition is published
    # and introduces a new country name not already here.
    ALL_OXFORD_NAMES = [
        'Afghanistan', 'Albania', 'Algeria', 'Andorra', 'Angola',
        'Antigua & Barbuda', 'Argentina', 'Armenia', 'Australia',
        'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain', 'Bangladesh',
        'Barbados', 'Belarus', 'Belgium', 'Belize', 'Benin', 'Bhutan',
        'Bolivia', 'Bosnia & Herzegovina', 'Botswana', 'Brazil',
        'Brunei', 'Bulgaria', 'Burkina Faso', 'Burundi', 'Cabo Verde',
        'Cambodia', 'Cameroon', 'Canada', 'Central African Republic',
        'Chad', 'Chile', 'China', 'Colombia', 'Comoros', 'Congo',
        'Costa Rica', 'Croatia', 'Cuba', 'Cyprus', 'Czechia',
        'Democratic Republic of the Congo', 'Denmark', 'Djibouti',
        'Dominica', 'Dominican Republic', 'Ecuador', 'Egypt',
        'El Salvador', 'Equatorial Guinea', 'Eritrea', 'Estonia',
        'Eswatini', 'Ethiopia', 'Fiji', 'Finland', 'France', 'Gabon',
        'Gambia', 'Georgia', 'Germany', 'Ghana', 'Greece', 'Grenada',
        'Guatemala', 'Guinea', 'Guinea-Bissau', 'Guyana', 'Haiti',
        'Honduras', 'Hong Kong', 'Hungary', 'Iceland', 'India',
        'Indonesia', 'Iran', 'Iraq', 'Ireland', 'Israel', 'Italy',
        'Ivory Coast', 'Jamaica', 'Japan', 'Jordan', 'Kazakhstan',
        'Kenya', 'Kosovo', 'Kuwait', 'Kyrgyzstan', 'Laos', 'Latvia',
        'Lebanon', 'Lesotho', 'Liberia', 'Libya', 'Liechtenstein',
        'Lithuania', 'Luxembourg', 'Macao', 'Madagascar', 'Malawi',
        'Malaysia', 'Maldives', 'Mali', 'Malta', 'Mauritania',
        'Mauritius', 'Mexico', 'Micronesia', 'Moldova', 'Monaco',
        'Mongolia', 'Montenegro', 'Morocco', 'Mozambique', 'Myanmar',
        'Namibia', 'Nepal', 'Netherlands', 'New Zealand', 'Nicaragua',
        'Niger', 'Nigeria', 'North Korea', 'North Macedonia', 'Norway',
        'Oman', 'Pakistan', 'Palestine', 'Panama', 'Papua New Guinea',
        'Paraguay', 'Peru', 'Philippines', 'Poland', 'Portugal',
        'Qatar', 'Republic of Korea', 'Republic of the Congo',
        'Romania', 'Russia', 'Rwanda', 'Saint Kitts & Nevis',
        'Saint Lucia', 'Saint Vincent and the Grenadines',
        'São Tomé & Príncipe', 'Saudi Arabia', 'Senegal', 'Serbia',
        'Seychelles', 'Sierra Leone', 'Singapore', 'Slovakia',
        'Slovenia', 'Solomon Islands', 'Somalia', 'South Africa',
        'South Korea', 'South Sudan', 'Spain', 'Sri Lanka', 'Sudan',
        'Suriname', 'Sweden', 'Switzerland', 'Syria', 'Taiwan',
        'Tajikistan', 'Tanzania', 'Thailand', 'Timor-Leste', 'Togo',
        'Trinidad & Tobago', 'Tunisia', 'Turkey', 'Turkmenistan',
        'Uganda', 'Ukraine', 'United Arab Emirates', 'United Kingdom',
        'United States of America', 'Uruguay', 'Uzbekistan', 'Vanuatu',
        'Venezuela', 'Vietnam', 'Yemen', 'Zambia', 'Zimbabwe',
    ]

    # Build name → iso3 lookup from metadata.countries.
    name_to_iso3 = {
        row['country_name'].lower().strip(): row['iso3']
        for _, row in countries.iterrows()
    }
    valid_iso3 = set(countries['iso3'].tolist())

    count_high   = 0
    count_medium = 0
    unmatched    = []

    for name in ALL_OXFORD_NAMES:
        iso3, confidence = match_oxford_country(
            name, name_to_iso3, valid_iso3
        )

        if iso3:
            upsert_code(conn, iso3, 'oxford', name)
            if confidence == 'high':
                count_high += 1
            else:
                count_medium += 1
                print(
                    f"  ⚠ Medium confidence match: "
                    f"'{name}' → {iso3}. Please verify."
                )
        else:
            unmatched.append(name)

    print(
        f"  ✓ Processed {count_high + count_medium} Oxford country codes "
        f"({count_high} high confidence, {count_medium} medium confidence)"
    )
    if unmatched:
        print(
            f"  ✗ {len(unmatched)} Oxford names could not be matched:"
        )
        for name in unmatched:
            print(f"    - {name}")
        print(
            f"    Add these to OXFORD_MANUAL_OVERRIDES dict "
            f"and re-run this seed script."
        )


# ═══════════════════════════════════════════════════════════════
# 5. OPENALEX COUNTRY CODES — ISO2 VIA PYCOUNTRY
# ═══════════════════════════════════════════════════════════════
def seed_openalex_country_codes(conn, countries: pd.DataFrame):
    """
    OpenAlex returns ISO2 codes (e.g. CN, US, LB).
    We convert ISO3 → ISO2 via pycountry and store ISO2 as code.
    """
    print("\n── OpenAlex country codes ──")

    count     = 0
    unmatched = []

    for _, row in countries.iterrows():
        iso3 = row['iso3']
        iso2 = iso3_to_iso2(iso3)

        if iso2:
            upsert_code(conn, iso3, 'openalex', iso2)
            count += 1
        else:
            unmatched.append(iso3)

    print(f"  ✓ Processed {count} OpenAlex country codes")
    if unmatched:
        print(
            f"  ⚠ {len(unmatched)} countries have no ISO2 mapping "
            f"via pycountry"
        )


# ═══════════════════════════════════════════════════════════════
# 6. WIPO IP COUNTRY CODES — ISO2 VIA PYCOUNTRY (FIXED)
# ═══════════════════════════════════════════════════════════════
def seed_wipo_country_codes(conn, countries: pd.DataFrame):
    """
    WIPO IP Statistics uses ISO2 codes in the actual CSV file
    (e.g. 'LB', 'US', 'FR' in the 'Origin (Code)' column).

    WHY THIS WAS FIXED FROM ISO3:
        The original seed script incorrectly assumed WIPO uses ISO3.
        Inspection of wipo_ai_patents.csv showed the 'Origin (Code)'
        column contains ISO2 codes. Storing ISO3 codes as the
        crosswalk would cause every WIPO row to fail the fk_validity
        check in transformation (the code 'LB' would not match the
        stored 'LBN'). The fix stores ISO2 as the code, matching
        what the actual WIPO file contains.

    Same approach as OpenAlex — convert ISO3 → ISO2 via pycountry.
    """
    print("\n── WIPO IP country codes ── (uses ISO2)")

    count     = 0
    unmatched = []

    for _, row in countries.iterrows():
        iso3 = row['iso3']
        iso2 = iso3_to_iso2(iso3)

        if iso2:
            upsert_code(conn, iso3, 'wipo_ip', iso2)
            count += 1
        else:
            unmatched.append(iso3)

    print(f"  ✓ Processed {count} WIPO IP country codes (ISO2)")
    if unmatched:
        print(
            f"  ⚠ {len(unmatched)} countries have no ISO2 mapping "
            f"via pycountry"
        )


# ═══════════════════════════════════════════════════════════════
# 7. OECD MSTI COUNTRY CODES
# ═══════════════════════════════════════════════════════════════
def seed_oecd_country_codes(conn, countries: pd.DataFrame):
    """
    OECD MSTI uses ISO3 directly — trivial 1:1 mapping.
    Only ~38 OECD members + selected non-members have data.
    We insert all countries for consistency — those without
    OECD data simply have no observations.
    """
    print("\n── OECD MSTI country codes ──")
    count = 0
    for _, row in countries.iterrows():
        upsert_code(conn, row['iso3'], 'oecd_msti', row['iso3'])
        count += 1
    print(f"  ✓ Processed {count} OECD MSTI country codes")


# ═══════════════════════════════════════════════════════════════
# MAIN — RUN ALL IN ORDER
# ═══════════════════════════════════════════════════════════════
print("Seeding metadata.country_codes...")
print("=" * 50)

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
