# Streamlit App

The researcher-facing interface. Three pages: a landing hero with live stats, a query builder with searchable filters, and an interactive results dashboard with auto-typed charts and CSV export.

---

## Run

```bash
# from project root, with the venv active and .env populated:
streamlit run app/Home.py
```

Opens at <http://localhost:8501>. Auto-reloads on file save — you can edit a page in your editor and the browser refreshes immediately.

## Pages

### `Home.py` · landing

The hero displays four live stats pulled from Supabase (sources, observations, countries, year span) — cached for 10 minutes. Falls back to static placeholders if the database is unreachable so the page never errors out completely. CTA button takes you to the query builder.

### `pages/1_Query.py` · query builder

Four filter sections in order:

1. **Sources** — only sources with data in `standardized.observations` appear. Sources still being ingested are surfaced in an info banner.
2. **Metrics** — checkbox table with metadata columns (source, name, unit, year coverage, country count). Search input above the table narrows visible rows; previously-checked metrics survive the filter so you never silently lose selections.
3. **Countries & regions** — quick-select by region or income group fills the country list automatically; individual countries are then editable. Search input above the country picker.
4. **Year range** — slider auto-bounds based on selected metrics' availability; warns about coverage gaps before you submit.

The "Get Data →" button is disabled until at least one metric is picked; before submitting, the page shows a row-count estimate and warns on queries projected to exceed 500k rows.

### `pages/2_Results.py` · results dashboard

- **Wide-format preview** (10 rows) and CSV download with sanitized column names: `country_iso3 | country_name | year | <source>.<metric> | …`
- **Coverage overview** — four stat cards (countries, metrics returned, years, missing %), plus expanders for per-metric and per-country missing-rate breakdowns.
- **Two charts** wrapped in `@st.fragment` (changing one chart's controls doesn't re-render the other):
  - Each metric is auto-classified as `binary` (values strictly in {0, 1}) or `continuous`.
  - Chart 1 defaults to a line chart of continuous metrics with mean aggregation.
  - Chart 2 defaults to a bar chart of binary metrics (sum = count of 1-countries per year). If no binary metrics, falls back to a scatter comparing two continuous metrics, or a second line chart.
  - Chart type and aggregation can be overridden per chart.
- **Summary statistics** table + drill-down (min/mean/max grouped by country or year for one selected metric).
- **Metric definitions** — expandable cards with unit, category, frequency, coverage span, country count, observation count, missing rate, and the source's own description.

## Caching strategy

| Function | TTL | Reason |
|---|---|---|
| `load_landing_stats` (Home) | 10 min | Hero is the most-viewed page; refreshing every 10 min is plenty |
| `load_sources`, `load_metrics_with_data`, `load_countries` (Query) | 5 min | User dwells on Query for minutes; stale dropdowns are tolerable |
| `run_query` (Results) | 2 min | Tighter TTL because users iterate on filters |
| `load_metric_meta` (Results) | 5 min | Reused across charts and definitions section |

`@st.cache_resource` wraps the SQLAlchemy engine so connection pooling persists across reruns.

## Performance & UX details

- **`@st.fragment` on chart sections** — Streamlit's default page-rerun model would re-execute the Supabase query and re-compute summary stats every time a chart filter changes. Fragments scope the rerun to just the chart that changed.
- **Wide-format pivot client-side** — the underlying `df` stays in long format because charts and stats need it that way. The wide preview/CSV is built with a pandas pivot on already-loaded rows — no extra DB round-trip.
- **Search-then-pick pattern** for metric and country filters — Streamlit's native `st.multiselect` closes after every selection, which is irritating with 300+ options. The search input above each picker pre-narrows the list so picking 5 metrics from a list of 8 doesn't feel like a dropdown at all. The metrics filter further uses `st.data_editor` with a checkbox column so you can tick multiple without anything closing.
- **Sanitized CSV column names** — newlines and carriage returns are stripped from `<source>.<metric>` headers so no CSV parser breaks on import.

## Customization

### Theme

The app uses **DM Serif Display** (display font) and **DM Sans** (body) loaded from Google Fonts. Color palette is dark with a blue accent — see the `<style>` block at the top of each page. To change globally, edit the CSS in all three pages (it's deliberately duplicated so each page is self-contained).

### Adding a new chart

Open `2_Results.py`, find the `_build_chart_section` helper, and call it inside a new fragment-wrapped function. Each new chart needs a unique `chart_num` (drives the widget keys and the `st.plotly_chart` key).

### Changing default filter values

Filter defaults live in `pages/1_Query.py` — region quick-select fills `_country_default` in session state; year slider defaults come from `available_from` / `available_to` in `metadata.metrics`.

## Common gotchas

- **`StreamlitDuplicateElementId` on `plotly_chart`** — happens if two `st.plotly_chart` calls have identical params (Streamlit auto-generates IDs from element type + params). Always pass a unique `key=` to each plotly chart.
- **Charts disappear after an error elsewhere** — Streamlit aborts the entire page render on the first uncaught exception. If a section below the error vanishes, look for a red traceback above it.
- **Selections silently reset between pages** — values that need to survive page navigation must be stored in `st.session_state` explicitly. The Query page persists its selections that way; if you add a new filter, do the same.
- **Slow first load** — on a fresh Supabase Small tier, `load_metrics_with_data` can take 5–15s the first time because it scans 3M+ observations. Subsequent loads hit the cache and are instant for 5 min.

## File layout

```
app/
├── README.md          ← you are here
├── Home.py            ← landing page · live hero stats
└── pages/
    ├── 1_Query.py     ← filter builder (4 sections + Get Data button)
    └── 2_Results.py   ← preview + charts + stats + downloads
```

Streamlit auto-discovers files in `pages/` and lists them in the sidebar (which we hide via CSS — navigation is handled by buttons instead).
