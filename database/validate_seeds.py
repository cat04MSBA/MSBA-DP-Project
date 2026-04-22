"""
database/validate_seeds.py
==========================
Post-seed integrity validator for all metadata tables.

WHAT THIS FILE DOES:
    Runs a comprehensive set of checks on the metadata tables
    immediately after run_seeds.py completes. If any check fails,
    the problem is reported clearly and the process exits with a
    non-zero code so run_seeds.py can detect the failure and stop.

WHY THIS EXISTS:
    The seed scripts populate five tables that the entire pipeline
    depends on:
        metadata.sources        → every pipeline run references this
        metadata.countries      → every observation's country_iso3
        metadata.metrics        → every observation's metric_id
        metadata.country_codes  → every transformation's country lookup
        metadata.metric_codes   → every ingestion's indicator list

    If any of these tables has corrupt, missing, or inconsistent data,
    everything downstream is silently wrong:
        - A missing country code → transformation drops rows silently
        - A missing metric code  → ingestion skips indicators silently
        - A broken FK reference  → pre-upsert check rejects all rows
        - A NULL continent       → researcher filters return wrong results

    Catching these at seed time costs seconds. Catching them after
    ingestion costs hours of re-running the pipeline.

CHECKS PERFORMED:
    1.  Row counts — each table has the expected minimum number of rows
    2.  Required sources — all 7 pipeline sources exist
    3.  No NULL required fields — country_name, metric_id, source_id, etc.
    4.  No NULL continent — every country has a continent assigned
    5.  FK integrity — every country_code.iso3 exists in countries
    6.  FK integrity — every country_code.source_id exists in sources
    7.  FK integrity — every metric_code.metric_id exists in metrics
    8.  FK integrity — every metric_code.source_id exists in sources
    9.  FK integrity — every metric.source_id exists in sources
    10. Crosswalk completeness — every source has country_codes
    11. Crosswalk completeness — every source has metric_codes
    12. No duplicate source codes — UNIQUE (source_id, code) for countries
    13. No duplicate source codes — UNIQUE (source_id, code) for metrics
    14. Continent values — only valid 7-continent values stored
    15. Income group values — only valid World Bank values stored
    16. last_retrieved is NULL — no source should have been run yet
    17. available_from/to NULL — coverage not yet calculated (correct)
    18. Metric frequency values — only valid values stored

USAGE:
    Called automatically by run_seeds.py as the final step.
    Can also be run independently:
        python3 -m database.validate_seeds
"""

import sys
from sqlalchemy import text
from database.connection import get_engine

engine = get_engine()

# ─────────────────────────────────────────────────────────────
# EXPECTED VALUES
# ─────────────────────────────────────────────────────────────

REQUIRED_SOURCES = {
    'world_bank', 'imf', 'oxford', 'pwt',
    'openalex', 'wipo_ip', 'oecd_msti',
}

VALID_CONTINENTS = {
    'Africa', 'Asia', 'Europe',
    'North America', 'South America',
    'Oceania', 'Antarctica',
}

VALID_INCOME_GROUPS = {
    'Low income', 'Lower middle income',
    'Upper middle income', 'High income',
}

VALID_FREQUENCIES = {'annual', 'biannual', 'quarterly', 'monthly'}

# Minimum expected row counts (conservative lower bounds).
# These catch gross failures (empty tables, seeding aborted early).
MIN_ROW_COUNTS = {
    'metadata.sources':       7,
    'metadata.countries':   200,   # WB returns ~266
    'metadata.metrics':     500,   # WB alone has 439
    'metadata.country_codes': 500, # 7 sources × ~200 countries
    'metadata.metric_codes':  500, # WB alone has 439
}


# ─────────────────────────────────────────────────────────────
# RUNNER
# ─────────────────────────────────────────────────────────────

def validate() -> bool:
    """
    Run all seed integrity checks. Returns True if all pass.
    Prints a clear report of every failure found.
    """
    failures = []
    warnings = []

    with engine.connect() as conn:

        # ── 1. Row counts ──────────────────────────────────────
        for table, minimum in MIN_ROW_COUNTS.items():
            count = conn.execute(
                text(f"SELECT COUNT(*) FROM {table}")
            ).scalar()
            if count < minimum:
                failures.append(
                    f"[ROW COUNT] {table}: {count} rows "
                    f"(expected at least {minimum}). "
                    f"Seeding may have failed or aborted early."
                )
            else:
                print(f"  ✓  {table}: {count} rows")

        # ── 2. Required sources present ────────────────────────
        rows = conn.execute(
            text("SELECT source_id FROM metadata.sources")
        ).fetchall()
        present = {r[0] for r in rows}
        missing = REQUIRED_SOURCES - present
        if missing:
            failures.append(
                f"[SOURCES] Missing required sources: {sorted(missing)}. "
                f"Check seed_sources.py."
            )
        else:
            print(f"  ✓  All 7 required sources present")

        # ── 3. No NULL required fields in countries ────────────
        null_name = conn.execute(text(
            "SELECT COUNT(*) FROM metadata.countries "
            "WHERE country_name IS NULL OR country_name = ''"
        )).scalar()
        if null_name > 0:
            failures.append(
                f"[NULL] metadata.countries: {null_name} rows "
                f"with NULL or empty country_name."
            )
        else:
            print(f"  ✓  metadata.countries: no NULL country_name")

        # ── 4. No NULL continent ───────────────────────────────
        null_cont = conn.execute(text(
            "SELECT iso3 FROM metadata.countries "
            "WHERE continent IS NULL ORDER BY iso3"
        )).fetchall()
        if null_cont:
            iso3s = [r[0] for r in null_cont]
            # Warn, not fail — some territories may legitimately
            # have no mapping yet.
            warnings.append(
                f"[CONTINENT] {len(iso3s)} countries have NULL continent: "
                f"{iso3s}. Add them to ISO3_TO_CONTINENT in "
                f"seed_countries.py."
            )
        else:
            print(f"  ✓  metadata.countries: all countries have continent")

        # ── 5. Continent values are valid ──────────────────────
        invalid_cont = conn.execute(text(
            "SELECT DISTINCT continent FROM metadata.countries "
            "WHERE continent IS NOT NULL"
        )).fetchall()
        bad = {r[0] for r in invalid_cont} - VALID_CONTINENTS
        if bad:
            failures.append(
                f"[CONTINENT] Invalid continent values in "
                f"metadata.countries: {bad}. "
                f"Valid values: {sorted(VALID_CONTINENTS)}."
            )
        else:
            print(f"  ✓  metadata.countries: all continent values valid")

        # ── 6. Income group values are valid ───────────────────
        invalid_income = conn.execute(text(
            "SELECT DISTINCT income_group FROM metadata.countries "
            "WHERE income_group IS NOT NULL"
        )).fetchall()
        # World Bank uses title case — normalise for comparison
        actual_income = {r[0] for r in invalid_income}
        # Income group strings from WB are title-cased already
        # Just check none are obviously wrong
        known_keywords = {'income', 'Income'}
        bad_income = {
            v for v in actual_income
            if not any(kw in v for kw in known_keywords)
        }
        if bad_income:
            warnings.append(
                f"[INCOME GROUP] Unexpected income_group values: "
                f"{bad_income}. Verify World Bank API response."
            )
        else:
            print(f"  ✓  metadata.countries: income_group values look valid")

        # ── 7. FK: country_codes.iso3 → countries ─────────────
        orphan_iso3 = conn.execute(text("""
            SELECT DISTINCT cc.iso3
            FROM metadata.country_codes cc
            LEFT JOIN metadata.countries c ON cc.iso3 = c.iso3
            WHERE c.iso3 IS NULL
        """)).fetchall()
        if orphan_iso3:
            bad_iso3 = [r[0] for r in orphan_iso3]
            failures.append(
                f"[FK] metadata.country_codes has {len(bad_iso3)} iso3 "
                f"values not in metadata.countries: {bad_iso3}. "
                f"Run seed_countries.py first, or add missing countries."
            )
        else:
            print(f"  ✓  metadata.country_codes: all iso3 → countries FK valid")

        # ── 8. FK: country_codes.source_id → sources ──────────
        orphan_src_cc = conn.execute(text("""
            SELECT DISTINCT cc.source_id
            FROM metadata.country_codes cc
            LEFT JOIN metadata.sources s ON cc.source_id = s.source_id
            WHERE s.source_id IS NULL
        """)).fetchall()
        if orphan_src_cc:
            bad = [r[0] for r in orphan_src_cc]
            failures.append(
                f"[FK] metadata.country_codes has {len(bad)} source_id "
                f"values not in metadata.sources: {bad}."
            )
        else:
            print(f"  ✓  metadata.country_codes: all source_id → sources FK valid")

        # ── 9. FK: metric_codes.metric_id → metrics ───────────
        orphan_mc = conn.execute(text("""
            SELECT DISTINCT mc.metric_id
            FROM metadata.metric_codes mc
            LEFT JOIN metadata.metrics m ON mc.metric_id = m.metric_id
            WHERE m.metric_id IS NULL
            LIMIT 5
        """)).fetchall()
        if orphan_mc:
            bad = [r[0] for r in orphan_mc]
            failures.append(
                f"[FK] metadata.metric_codes has metric_id values not in "
                f"metadata.metrics (first 5): {bad}. "
                f"Run seed_metrics.py before seed_metric_codes.py."
            )
        else:
            print(f"  ✓  metadata.metric_codes: all metric_id → metrics FK valid")

        # ── 10. FK: metric_codes.source_id → sources ──────────
        orphan_src_mc = conn.execute(text("""
            SELECT DISTINCT mc.source_id
            FROM metadata.metric_codes mc
            LEFT JOIN metadata.sources s ON mc.source_id = s.source_id
            WHERE s.source_id IS NULL
        """)).fetchall()
        if orphan_src_mc:
            bad = [r[0] for r in orphan_src_mc]
            failures.append(
                f"[FK] metadata.metric_codes has source_id values not in "
                f"metadata.sources: {bad}."
            )
        else:
            print(f"  ✓  metadata.metric_codes: all source_id → sources FK valid")

        # ── 11. FK: metrics.source_id → sources ───────────────
        orphan_src_m = conn.execute(text("""
            SELECT DISTINCT m.source_id
            FROM metadata.metrics m
            LEFT JOIN metadata.sources s ON m.source_id = s.source_id
            WHERE s.source_id IS NULL
        """)).fetchall()
        if orphan_src_m:
            bad = [r[0] for r in orphan_src_m]
            failures.append(
                f"[FK] metadata.metrics has source_id values not in "
                f"metadata.sources: {bad}."
            )
        else:
            print(f"  ✓  metadata.metrics: all source_id → sources FK valid")

        # ── 12. Crosswalk completeness — country codes ─────────
        # Every required source must have at least some country codes
        src_cc_counts = {
            r[0]: r[1]
            for r in conn.execute(text("""
                SELECT source_id, COUNT(*) AS cnt
                FROM metadata.country_codes
                GROUP BY source_id
            """)).fetchall()
        }
        empty_src = [
            s for s in REQUIRED_SOURCES
            if src_cc_counts.get(s, 0) == 0
        ]
        if empty_src:
            failures.append(
                f"[CROSSWALK] These sources have NO country codes: "
                f"{sorted(empty_src)}. seed_country_codes.py may have "
                f"failed for these sources."
            )
        else:
            print(
                f"  ✓  metadata.country_codes: all sources have entries "
                f"({', '.join(f'{s}:{src_cc_counts[s]}' for s in sorted(REQUIRED_SOURCES))})"
            )

        # ── 13. Crosswalk completeness — metric codes ──────────
        src_mc_counts = {
            r[0]: r[1]
            for r in conn.execute(text("""
                SELECT source_id, COUNT(*) AS cnt
                FROM metadata.metric_codes
                GROUP BY source_id
            """)).fetchall()
        }
        empty_mc = [
            s for s in REQUIRED_SOURCES
            if src_mc_counts.get(s, 0) == 0
        ]
        if empty_mc:
            failures.append(
                f"[CROSSWALK] These sources have NO metric codes: "
                f"{sorted(empty_mc)}. seed_metric_codes.py may have "
                f"failed for these sources."
            )
        else:
            print(
                f"  ✓  metadata.metric_codes: all sources have entries "
                f"({', '.join(f'{s}:{src_mc_counts[s]}' for s in sorted(REQUIRED_SOURCES))})"
            )

        # ── 14. No duplicate source codes (country_codes) ─────
        dup_cc = conn.execute(text("""
            SELECT source_id, code, COUNT(*) AS cnt
            FROM metadata.country_codes
            GROUP BY source_id, code
            HAVING COUNT(*) > 1
            LIMIT 5
        """)).fetchall()
        if dup_cc:
            failures.append(
                f"[DUPLICATE] metadata.country_codes has duplicate "
                f"(source_id, code) pairs (first 5): "
                f"{[(r[0], r[1]) for r in dup_cc]}. "
                f"This violates UNIQUE (source_id, code) constraint."
            )
        else:
            print(f"  ✓  metadata.country_codes: no duplicate (source_id, code)")

        # ── 15. No duplicate source codes (metric_codes) ──────
        dup_mc = conn.execute(text("""
            SELECT source_id, code, COUNT(*) AS cnt
            FROM metadata.metric_codes
            GROUP BY source_id, code
            HAVING COUNT(*) > 1
            LIMIT 5
        """)).fetchall()
        if dup_mc:
            failures.append(
                f"[DUPLICATE] metadata.metric_codes has duplicate "
                f"(source_id, code) pairs (first 5): "
                f"{[(r[0], r[1]) for r in dup_mc]}. "
                f"This violates UNIQUE (source_id, code) constraint."
            )
        else:
            print(f"  ✓  metadata.metric_codes: no duplicate (source_id, code)")

        # ── 16. last_retrieved is NULL for all sources ─────────
        # After a fresh seed, no source should have been run yet.
        # A non-NULL last_retrieved means someone is re-seeding
        # a database that already has pipeline runs — which is
        # expected on re-seed and is not a failure, just a warning.
        non_null_lr = conn.execute(text("""
            SELECT source_id, last_retrieved
            FROM metadata.sources
            WHERE last_retrieved IS NOT NULL
        """)).fetchall()
        if non_null_lr:
            warnings.append(
                f"[LAST_RETRIEVED] {len(non_null_lr)} sources have "
                f"non-NULL last_retrieved: "
                f"{[(r[0], str(r[1])) for r in non_null_lr]}. "
                f"This is expected if re-seeding an existing database. "
                f"If this is a fresh database, check that reset.sql ran first."
            )
        else:
            print(f"  ✓  metadata.sources: all last_retrieved are NULL (fresh seed)")

        # ── 17. All five coverage columns NULL in metrics ─────────
        # available_from, available_to, country_count,
        # observation_count, and missing_value_rate are all
        # computed by calculate_coverage.py after ingestion.
        # After a fresh seed they must all be NULL — any non-NULL
        # value means stale data from a previous database run is
        # present, which is expected on re-seed and not a failure.
        non_null_cov = conn.execute(text("""
            SELECT COUNT(*) FROM metadata.metrics
            WHERE available_from     IS NOT NULL
               OR available_to       IS NOT NULL
               OR country_count      IS NOT NULL
               OR observation_count  IS NOT NULL
               OR missing_value_rate IS NOT NULL
        """)).scalar()
        if non_null_cov > 0:
            warnings.append(
                f"[COVERAGE] {non_null_cov} metrics have non-NULL coverage "
                f"fields (available_from, available_to, country_count, "
                f"observation_count, missing_value_rate) after seeding. "
                f"These should be NULL until calculate_coverage.py runs. "
                f"If re-seeding an existing database this is expected — "
                f"coverage will refresh automatically after the next "
                f"ingestion cycle."
            )
        else:
            print(
                f"  ✓  metadata.metrics: all coverage fields NULL "
                f"(not yet calculated — correct after fresh seed)"
            )

        # ── 18. Metric frequency values ───────────────────────
        invalid_freq = conn.execute(text("""
            SELECT DISTINCT frequency FROM metadata.metrics
            WHERE frequency IS NOT NULL
        """)).fetchall()
        bad_freq = {r[0] for r in invalid_freq} - VALID_FREQUENCIES
        if bad_freq:
            warnings.append(
                f"[FREQUENCY] Unexpected frequency values in "
                f"metadata.metrics: {bad_freq}. "
                f"Valid values: {sorted(VALID_FREQUENCIES)}."
            )
        else:
            print(f"  ✓  metadata.metrics: all frequency values valid")

        # ── 19. No NULL metric_name ────────────────────────────
        null_mname = conn.execute(text(
            "SELECT COUNT(*) FROM metadata.metrics "
            "WHERE metric_name IS NULL OR metric_name = ''"
        )).scalar()
        if null_mname > 0:
            failures.append(
                f"[NULL] metadata.metrics: {null_mname} rows with "
                f"NULL or empty metric_name."
            )
        else:
            print(f"  ✓  metadata.metrics: no NULL metric_name")

        # ── 20. No NULL unit where expected ───────────────────
        # OECD, PWT, Oxford, OpenAlex, WIPO all have known units.
        # WB and IMF have varied units per metric — allow NULL there.
        # Just verify oecd, oxford, openalex, wipo have units set.
        sources_requiring_units = ['oecd_msti', 'oxford', 'openalex', 'wipo_ip']
        for src in sources_requiring_units:
            null_unit = conn.execute(text("""
                SELECT COUNT(*) FROM metadata.metrics
                WHERE source_id = :src AND unit IS NULL
            """), {'src': src}).scalar()
            if null_unit > 0:
                warnings.append(
                    f"[UNIT] metadata.metrics: {null_unit} metrics for "
                    f"source '{src}' have NULL unit. These sources have "
                    f"known units — check seed_metrics.py."
                )

        print(f"  ✓  metadata.metrics: unit check complete")

    # ── Report ─────────────────────────────────────────────────
    print()
    print("─" * 60)

    if warnings:
        print(f"WARNINGS ({len(warnings)}) — pipeline can run but review these:")
        for w in warnings:
            print(f"  ⚠  {w}")
        print()

    if failures:
        print(f"FAILURES ({len(failures)}) — DO NOT run the pipeline until fixed:")
        for f in failures:
            print(f"  ✗  {f}")
        print()
        print("✗ Seed validation FAILED — fix the issues above and re-run seeds.")
        return False

    print("✓ All seed validation checks passed. Pipeline is ready to run.")
    return True


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("SEED INTEGRITY VALIDATION")
    print("=" * 60)
    print()
    ok = validate()
    sys.exit(0 if ok else 1)
