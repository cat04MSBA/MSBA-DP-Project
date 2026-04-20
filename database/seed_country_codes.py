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
    pwt         Reads countrycode/country pairs from pwt110.dta
                directly — no hardcoded list
    oxford      Reads Country column from all XLSX files in
                data/raw/oxford/ — no hardcoded list
    openalex    Uses ISO2 → mapped via pycountry
    wipo_ip     Uses ISO2 → mapped via pycountry (fixed from ISO3)
    oecd_msti   Uses ISO3 directly → trivial 1:1 mapping

WHY PWT AND OXFORD READ FROM ACTUAL FILES (not hardcoded lists):
    The original seed script hardcoded country names for both
    sources. This was wrong for two reasons:
    1. When PWT releases a new version or Oxford adds a new
       country, the hardcoded list becomes stale — the team
       must remember to update it manually.
    2. The actual files are already on disk at data/raw/.
       Reading from them is more accurate (uses the exact name
       variants the source uses) and self-maintaining.

    PWT: reads countrycode (ISO3) and country (name) columns
    directly from pwt110.dta. countrycode IS the ISO3 key.
    country is stored as the code — this is what PWT uses as
    its identifier in the data file.

    Oxford: reads the Country column from all XLSX files in
    data/raw/oxford/. Gets the union of all unique names across
    all editions, handles column inconsistency per edition,
    then matches names to ISO3 via three-method validation.

FILE PATHS:
    PWT:    data/raw/pwt/pwt110.dta
    Oxford: data/raw/oxford/oxford_*.xlsx
    These paths are relative to the project root directory.
    Run this script from the project root.

DRIFT-AWARE UPSERT:
    upsert_code() checks if the incoming code differs from the
    stored code before updating. If it differs, it does NOT
    apply the update — it prints a warning and skips.
    Code mapping changes must go through drift detection.

OXFORD MANUAL OVERRIDES:
    Handles name variants that pycountry cannot resolve
    automatically. Split into two categories:
    - CONFIRMED: verified correct matches added after
      reviewing medium-confidence matches from previous runs
    - These should be extended whenever a new Oxford edition
      introduces a name variant not covered by pycountry

DESIGN DECISION — ROW-PER-SOURCE CROSSWALK:
    metadata.country_codes stores one row per country per source.
    An alternative normalized design was considered and rejected.
    See architecture report for full decision documentation.
    Short version: sources like IMF and PWT use non-standard
    name variants that cannot be expressed as a simple convention
    lookup. Row-per-source handles all cases uniformly at
    negligible storage cost (7 sources × 266 countries = 1,862 rows).
"""

import requests
import pandas as pd
import pycountry
from collections import Counter
from pathlib import Path
from sqlalchemy import text
from database.connection import get_engine

engine = get_engine()

# ─────────────────────────────────────────────────────────────
# FILE PATHS — relative to project root
# ─────────────────────────────────────────────────────────────
PWT_FILE_PATH    = Path('data/raw/pwt/pwt110.dta')
OXFORD_DIR_PATH  = Path('data/raw/oxford')

# Sheet names used across Oxford editions 2019-2025.
OXFORD_SHEET_NAMES = [
    'Global rankings', 'Global ranking', 'Global Rankings',
    'Rankings', 'Ranking',
]


# ═══════════════════════════════════════════════════════════════
# HELPER: DRIFT-AWARE UPSERT ONE COUNTRY CODE
# ═══════════════════════════════════════════════════════════════
def upsert_code(conn, iso3: str, source_id: str, code: str):
    """
    Insert one row in metadata.country_codes if it does not
    already exist. Idempotent — safe to re-run.

    WHY THE LOGIC CHANGED WITH THE NEW PRIMARY KEY:
        The primary key is now (iso3, source_id, code).
        This means the same country can have multiple rows
        for the same source — one per name variant. This is
        needed for Oxford which uses different name variants
        across editions (e.g. 'Antigua & Barbuda' in 2019
        and 'Antigua and Barbuda' in 2022, both → ATG).

        Drift detection for country codes now means:
        - A new (iso3, source_id, code) triple → INSERT (new variant)
        - Same triple already exists → no-op (idempotent)
        - A code that previously mapped to a DIFFERENT iso3 →
          blocked by UNIQUE (source_id, code) constraint.
          This is the real drift case — a code changing country.

        For non-Oxford sources (one code per country):
        We check if a different code already exists for this
        (iso3, source_id) pair. If yes, the source changed its
        convention — that is drift and should be reviewed.
        For Oxford specifically, multiple codes per iso3 are
        expected and normal.
    """
    # Check if this exact (iso3, source_id, code) already exists.
    exact_match = conn.execute(text("""
        SELECT 1 FROM metadata.country_codes
        WHERE iso3 = :iso3 AND source_id = :source_id AND code = :code
    """), {'iso3': iso3, 'source_id': source_id, 'code': code}).fetchone()

    if exact_match:
        # Already exists — no-op. Idempotent re-run safe.
        return

    # For non-Oxford sources: check if a different code is already
    # stored for this (iso3, source_id). If so, this is a potential
    # drift — the source changed its country identifier convention.
    # Oxford is explicitly excluded because multiple codes per
    # country are expected and normal for that source.
    if source_id != 'oxford':
        existing = conn.execute(text("""
            SELECT code FROM metadata.country_codes
            WHERE iso3 = :iso3 AND source_id = :source_id
        """), {'iso3': iso3, 'source_id': source_id}).fetchone()

        if existing and existing[0] != code:
            print(
                f"  ⚠ Skipping code mapping change for {iso3} "
                f"({source_id}): stored='{existing[0]}', "
                f"incoming='{code}'. "
                f"Use drift detection to review this change."
            )
            return

    # Insert the new row.
    conn.execute(text("""
        INSERT INTO metadata.country_codes (iso3, source_id, code)
        VALUES (:iso3, :source_id, :code)
        ON CONFLICT (iso3, source_id, code) DO NOTHING
    """), {'iso3': iso3, 'source_id': source_id, 'code': code})


# ═══════════════════════════════════════════════════════════════
# HELPER: LOAD ALL ISO3 CODES FROM DATABASE
# ═══════════════════════════════════════════════════════════════
def load_countries():
    return pd.read_sql("""
        SELECT iso3, country_name
        FROM metadata.countries
        ORDER BY iso3
    """, engine)


# ═══════════════════════════════════════════════════════════════
# HELPERS: ISO CONVERSION VIA PYCOUNTRY
# ═══════════════════════════════════════════════════════════════
def iso2_to_iso3(iso2_code: str):
    """Convert ISO2 → ISO3. Returns None if unresolvable."""
    try:
        country = pycountry.countries.get(alpha_2=iso2_code.upper())
        return country.alpha_3 if country else None
    except Exception:
        return None


def iso3_to_iso2(iso3_code: str):
    """Convert ISO3 → ISO2. Returns None if unresolvable."""
    try:
        country = pycountry.countries.get(alpha_3=iso3_code.upper())
        return country.alpha_2 if country and hasattr(country, 'alpha_2') else None
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════
# 1. WORLD BANK
# ═══════════════════════════════════════════════════════════════
def seed_wb_country_codes(conn, countries: pd.DataFrame):
    """World Bank uses ISO3 directly — trivial 1:1 mapping."""
    print("\n── World Bank country codes ──")
    count = 0
    for _, row in countries.iterrows():
        upsert_code(conn, row['iso3'], 'world_bank', row['iso3'])
        count += 1
    print(f"  ✓ Processed {count} World Bank country codes")


# ═══════════════════════════════════════════════════════════════
# 2. IMF
# ═══════════════════════════════════════════════════════════════
def seed_imf_country_codes(conn, countries: pd.DataFrame):
    """
    IMF uses mixed codes (ISO2-like + custom).
    Matched via pycountry first, then name matching.
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
    name_to_iso3  = {
        row['country_name'].lower().strip(): row['iso3']
        for _, row in countries.iterrows()
    }
    valid_iso3 = set(countries['iso3'].tolist())

    IMF_MANUAL = {'taiwan province of china': 'TWN'}

    count     = 0
    unmatched = []

    for imf_code, info in imf_countries.items():
        if not info or not isinstance(info, dict) or not imf_code:
            continue
        label = str(info.get('label') or '').strip()
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
        print(f"  ⚠ {len(unmatched)} IMF codes unmatched:")
        for u in unmatched[:5]:
            print(f"    - {u}")
        if len(unmatched) > 5:
            print(f"    ... and {len(unmatched)-5} more")


# ═══════════════════════════════════════════════════════════════
# 3. PWT — READ FROM ACTUAL .dta FILE
# ═══════════════════════════════════════════════════════════════
def seed_pwt_country_codes(conn, countries: pd.DataFrame):
    """
    Read countrycode and country columns directly from pwt110.dta.

    WHY READ FROM FILE (not hardcoded list):
        The actual .dta file contains the exact name variants
        PWT uses as identifiers. Reading from the file means:
        1. No manual maintenance — a new PWT version with
           different country names is handled automatically
           by re-running this seed script.
        2. Perfect accuracy — uses exactly the names in the
           data file, not our approximation of them.

    HOW IT WORKS:
        countrycode is ISO3 directly (e.g. 'LBN').
        country is the name PWT uses (e.g. 'Lebanon').
        We store country as the code mapping to countrycode.
        Transformation scripts look up by country name to
        find ISO3 via metadata.country_codes.

    FILE PATH: data/raw/pwt/pwt110.dta (relative to project root)
    """
    print("\n── PWT country codes ──")

    if not PWT_FILE_PATH.exists():
        print(
            f"  ✗ PWT file not found at {PWT_FILE_PATH}. "
            f"Ensure pwt110.dta is in data/raw/pwt/."
        )
        return

    # Read only the two columns we need — faster than loading
    # the full dataset (13,690 rows × 51 columns).
    df = pd.read_stata(
        PWT_FILE_PATH,
        columns              = ['countrycode', 'country'],
        convert_categoricals = False,
    )

    # Get unique (iso3, name) pairs — one per country.
    pairs = df[['countrycode', 'country']].drop_duplicates()

    valid_iso3 = set(countries['iso3'].tolist())
    count      = 0
    unmatched  = []

    for _, row in pairs.iterrows():
        iso3     = str(row['countrycode']).strip()
        pwt_name = str(row['country']).strip()

        if iso3 in valid_iso3:
            upsert_code(conn, iso3, 'pwt', pwt_name)
            count += 1
        else:
            # ISO3 from PWT not in our metadata.countries.
            # This happens for territories not in the World Bank
            # country list (Anguilla AIA, Montserrat MSR, Taiwan TWN).
            # These need to be manually added to metadata.countries
            # before this mapping can be inserted.
            unmatched.append(f"{iso3}: {pwt_name}")

    print(f"  ✓ Processed {count} PWT country codes (read from {PWT_FILE_PATH})")
    if unmatched:
        print(f"  ⚠ {len(unmatched)} PWT countries not in metadata.countries:")
        for u in unmatched:
            print(f"    - {u}")
        print(
            f"    Insert these into metadata.countries manually "
            f"then re-run this script."
        )


# ═══════════════════════════════════════════════════════════════
# 4. OXFORD — READ FROM ACTUAL XLSX FILES
# ═══════════════════════════════════════════════════════════════

# Manual overrides for Oxford name variants that fail both
# pycountry fuzzy search and metadata.countries name match.
#
# TWO CATEGORIES:
#   CONFIRMED: reviewed and verified correct from previous runs
#   FIXES:     corrected wrong automatic matches
#
# Extend this dict when a new Oxford edition introduces a new
# country name not covered by pycountry.
OXFORD_MANUAL_OVERRIDES = {
    # ── Confirmed correct from medium-confidence matches ──────
    'Antigua & Barbuda':              'ATG',
    'Bahamas':                        'BHS',
    'Bosnia & Herzegovina':           'BIH',
    'Congo':                          'COG',
    'Democratic Republic of the Congo': 'COD',
    'Egypt':                          'EGY',
    'Gambia':                         'GMB',
    'Ivory Coast':                    'CIV',
    'Kyrgyzstan':                     'KGZ',
    'Saint Kitts & Nevis':            'KNA',
    'Saint Lucia':                    'LCA',
    'Saint Vincent and the Grenadines': 'VCT',
    'São Tomé & Príncipe':            'STP',
    'Slovakia':                       'SVK',
    'Somalia':                        'SOM',
    'Trinidad & Tobago':              'TTO',
    'Yemen':                          'YEM',
    # ── Fixes for wrong automatic matches ─────────────────────
    'Niger':                          'NER',  # was wrongly → NGA (Nigeria)
    'Republic of Korea':              'KOR',  # was wrongly → PRK (North Korea)
    'Republic of the Congo':          'COG',  # Congo Brazzaville
    # ── Names pycountry cannot resolve at all ─────────────────
    'United States of America':       'USA',
    'Palestine':                      'PSE',
    'Taiwan':                         'TWN',
    'Turkey':                         'TUR',  # official name now Türkiye
    'Kosovo':                         'XKX',
    'Hong Kong':                      'HKG',
    'Macao':                          'MAC',
    'Syria':                          'SYR',
    'Iran':                           'IRN',
    'South Korea':                    'KOR',
    'North Korea':                    'PRK',
    'Bolivia':                        'BOL',
    'Venezuela':                      'VEN',
    'Tanzania':                       'TZA',
    'Laos':                           'LAO',
    'Moldova':                        'MDA',
    'Vietnam':                        'VNM',
    'Russia':                         'RUS',
    'Brunei':                         'BRN',
    'Micronesia':                     'FSM',
    'Cape Verde':                     'CPV',
    'Swaziland':                      'SWZ',
    'Macedonia':                      'MKD',
    # ── Name variants across Oxford editions ──────────────────
    # These are different names Oxford uses for the same country
    # in different editions. All are valid — stored as separate
    # rows in metadata.country_codes, all mapping to same ISO3.
    'Antigua and Barbuda':            'ATG',  # 2022+ variant
    'Bosnia and Herzegovina':         'BIH',  # 2022+ variant
    'Brunei Darussalam':              'BRN',  # formal name variant
    'Cabo Verde':                     'CPV',  # official name (vs Cape Verde)
    'Czech Republic':                 'CZE',  # older name (now Czechia)
    "Côte D'Ivoire":                  'CIV',  # French variant (capital D)
    "Côte d'Ivoire":                  'CIV',  # French variant (lowercase d)
    "Democratic People's Republic of Korea": 'PRK',  # formal North Korea name
    "Lao People's Democratic Republic": 'LAO',  # formal Laos name
    'Republic of Moldova':            'MDA',  # formal Moldova name
    'Republic of North Macedonia':    'MKD',  # formal name variant
    'Russian Federation':             'RUS',  # formal Russia name
    'Saint Kitts and Nevis':          'KNA',  # 2022+ variant
    'Sao Tome and Principe':          'STP',  # without accents variant
    'State of Palestine':             'PSE',  # formal Palestine name
    'Syrian Arab Republic':           'SYR',  # formal Syria name
    'Trinidad and Tobago':            'TTO',  # 2022+ variant
    'Türkiye':                        'TUR',  # official name since 2022
    'United Kingdom of Great Britain and Northern Ireland': 'GBR',
    'United Republic of Tanzania':    'TZA',  # formal Tanzania name
    'Venezuela, Bolivarian Republic of': 'VEN',  # formal Venezuela name
    'Viet Nam':                       'VNM',  # formal Vietnam name
    # ── Previously unmatched — now resolved ───────────────────
    'Bolivia (Plurinational State of)': 'BOL',
    'Gambia (Republic of The)':       'GMB',
    'Guinea Bissau':                  'GNB',  # without hyphen variant
    'Iran (Islamic Republic of)':     'IRN',
    'Micronesia (Federated States of)': 'FSM',
}


def _read_oxford_country_names() -> set:
    """
    Read all unique country names from all Oxford XLSX files
    in data/raw/oxford/.

    WHY READ FROM FILES (not hardcoded list):
        1. Self-maintaining — new Oxford editions with new
           country names are picked up automatically.
        2. Accurate — uses the exact names Oxford uses,
           not our approximation.
        3. No manual maintenance when Oxford adds/removes
           countries between editions.

    Returns:
        Set of unique country name strings across all editions.
    """
    if not OXFORD_DIR_PATH.exists():
        print(
            f"  ✗ Oxford directory not found at {OXFORD_DIR_PATH}. "
            f"Ensure oxford_*.xlsx files are in data/raw/oxford/."
        )
        return set()

    xlsx_files = sorted(OXFORD_DIR_PATH.glob('oxford_*.xlsx'))

    if not xlsx_files:
        print(f"  ✗ No oxford_*.xlsx files found in {OXFORD_DIR_PATH}.")
        return set()

    all_names = set()

    for xlsx_path in xlsx_files:
        try:
            xl = pd.ExcelFile(xlsx_path)

            # Find the correct sheet — Oxford changes sheet names
            # between editions.
            sheet_name = None
            for candidate in OXFORD_SHEET_NAMES:
                if candidate in xl.sheet_names:
                    sheet_name = candidate
                    break

            if sheet_name is None:
                print(
                    f"    ⚠ No recognised sheet in {xlsx_path.name}. "
                    f"Sheets: {xl.sheet_names}. Skipping."
                )
                continue

            df = pd.read_excel(xlsx_path, sheet_name=sheet_name, dtype=str)

            if 'Country' not in df.columns:
                print(
                    f"    ⚠ No 'Country' column in {xlsx_path.name}. "
                    f"Columns: {list(df.columns)}. Skipping."
                )
                continue

            # Extract non-null country names.
            # Filter out 'AVERAGE' which appears as a summary
            # row in some Oxford editions — not a country.
            names = df['Country'].dropna().str.strip()
            names = names[(names != '') & (names != 'AVERAGE')]
            all_names.update(names.tolist())

            print(
                f"    ✓ {xlsx_path.name}: "
                f"{len(names.unique())} unique country names"
            )

        except Exception as e:
            print(f"    ✗ Could not read {xlsx_path.name}: {e}")

    return all_names


def _match_oxford_name(name: str, name_to_iso3: dict,
                       valid_iso3: set) -> tuple:
    """
    Match one Oxford country name to ISO3 using three methods.
    Returns (iso3_or_None, confidence).

    Methods (in order):
    1. Manual overrides — highest priority, handles known problem cases
    2. metadata.countries name match — exact match on World Bank names
    3. pycountry fuzzy search — handles many standard variants

    Confidence:
        'high'   — manual override OR 2+ methods agree
        'medium' — exactly 1 method matched
        'none'   — no method matched
    """
    # ── Method 1: Manual override — check first ───────────────
    # Manual overrides take priority because they encode
    # human-verified correct answers. If an override exists,
    # we trust it over pycountry or name matching.
    if name in OXFORD_MANUAL_OVERRIDES:
        iso3 = OXFORD_MANUAL_OVERRIDES[name]
        if iso3 in valid_iso3:
            return iso3, 'high'

    results = {}

    # ── Method 2: metadata.countries name match ───────────────
    name_lower = name.lower().strip()
    if name_lower in name_to_iso3:
        results['name_match'] = name_to_iso3[name_lower]

    # ── Method 3: pycountry fuzzy search ──────────────────────
    try:
        matches = pycountry.countries.search_fuzzy(name)
        if matches and matches[0].alpha_3 in valid_iso3:
            results['pycountry'] = matches[0].alpha_3
    except Exception:
        pass

    # ── Evaluate agreement ─────────────────────────────────────
    unique_results = set(results.values())

    if not unique_results:
        return None, 'none'

    if len(unique_results) == 1:
        iso3       = list(unique_results)[0]
        confidence = 'high' if len(results) >= 2 else 'medium'
        return iso3, confidence

    # Methods disagree — take majority or pycountry as tiebreaker.
    counts = Counter(results.values())
    most_common_iso3, most_common_count = counts.most_common(1)[0]
    if most_common_count >= 2:
        return most_common_iso3, 'high'
    fallback = results.get('pycountry') or results.get('name_match')
    return fallback, 'medium'


def seed_oxford_country_codes(conn, countries: pd.DataFrame):
    """
    Read all unique country names from Oxford XLSX files and
    map them to ISO3 using three-method validation.

    WHY MATCHING HAPPENS AT SEEDING TIME (not ingestion time):
        Fuzzy matching is expensive and non-deterministic across
        pycountry versions. Matching once at seeding time and
        storing in metadata.country_codes means ingestion scripts
        do one fast dict lookup — no fuzzy matching at runtime.
        Re-seeding is needed only when a new Oxford edition
        introduces a new country name not in the overrides dict.
    """
    print("\n── Oxford country codes ──")

    # ── Step 1: Read all names from XLSX files ─────────────────
    all_names = _read_oxford_country_names()

    if not all_names:
        print("  ✗ No Oxford country names found. Check file paths.")
        return

    print(f"  Found {len(all_names)} unique country names across all editions")

    # Build name → iso3 lookup from metadata.countries.
    name_to_iso3 = {
        row['country_name'].lower().strip(): row['iso3']
        for _, row in countries.iterrows()
    }
    valid_iso3 = set(countries['iso3'].tolist())

    # ── Step 2: Match each name to ISO3 ───────────────────────
    count_high   = 0
    count_medium = 0
    unmatched    = []

    for name in sorted(all_names):
        iso3, confidence = _match_oxford_name(
            name, name_to_iso3, valid_iso3
        )

        if iso3:
            upsert_code(conn, iso3, 'oxford', name)
            if confidence == 'high':
                count_high += 1
            else:
                count_medium += 1
                print(
                    f"  ⚠ Medium confidence: '{name}' → {iso3}. "
                    f"Please verify and add to OXFORD_MANUAL_OVERRIDES."
                )
        else:
            unmatched.append(name)

    print(
        f"  ✓ Processed {count_high + count_medium} Oxford country codes "
        f"({count_high} high confidence, {count_medium} medium confidence)"
    )
    if unmatched:
        print(f"  ✗ {len(unmatched)} Oxford names could not be matched:")
        for name in sorted(unmatched):
            print(f"    - {name}")
        print(
            f"    Add these to OXFORD_MANUAL_OVERRIDES in "
            f"seed_country_codes.py and re-run."
        )


# ═══════════════════════════════════════════════════════════════
# 5. OPENALEX — ISO2 VIA PYCOUNTRY
# ═══════════════════════════════════════════════════════════════
def seed_openalex_country_codes(conn, countries: pd.DataFrame):
    """OpenAlex returns ISO2 codes. Convert ISO3 → ISO2 via pycountry."""
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
        print(f"  ⚠ {len(unmatched)} countries have no ISO2 mapping")


# ═══════════════════════════════════════════════════════════════
# 6. WIPO — ISO2 VIA PYCOUNTRY (fixed from ISO3)
# ═══════════════════════════════════════════════════════════════
def seed_wipo_country_codes(conn, countries: pd.DataFrame):
    """
    WIPO uses ISO2 codes in the actual CSV file.

    WHY FIXED FROM ISO3:
        The original seed script incorrectly assumed WIPO uses ISO3.
        Inspection of wipo_ai_patents.csv confirmed the
        'Origin (Code)' column contains ISO2 codes (e.g. 'LB', 'US').
        Storing ISO3 codes would cause every WIPO row to fail
        the fk_validity check during transformation.
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
        print(f"  ⚠ {len(unmatched)} countries have no ISO2 mapping")


# ═══════════════════════════════════════════════════════════════
# 7. OECD MSTI
# ═══════════════════════════════════════════════════════════════
def seed_oecd_country_codes(conn, countries: pd.DataFrame):
    """OECD MSTI uses ISO3 directly — trivial 1:1 mapping."""
    print("\n── OECD MSTI country codes ──")
    count = 0
    for _, row in countries.iterrows():
        upsert_code(conn, row['iso3'], 'oecd_msti', row['iso3'])
        count += 1
    print(f"  ✓ Processed {count} OECD MSTI country codes")


# ═══════════════════════════════════════════════════════════════
# MAIN
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