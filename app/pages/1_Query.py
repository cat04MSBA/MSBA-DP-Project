"""
app/pages/1_Query.py
====================
Page 2 — Query builder.
"""

import streamlit as st
import pandas as pd
from sqlalchemy import text
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from database.connection import get_engine

st.set_page_config(
    page_title="Query — AI & Economic Data",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500&display=swap');
  html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
  .block-container { padding-top: 32px !important; max-width: 1200px !important; }
  #MainMenu, footer, header { visibility: hidden; }
  .page-title { font-family: 'DM Serif Display', serif; font-size: 38px; color: #f0f4ff; margin-bottom: 4px; font-weight: 400; }
  .page-sub { font-size: 15px; color: #718096; margin-bottom: 36px; font-weight: 300; }
  .section-label { font-size: 11px; font-weight: 500; letter-spacing: 2.5px; text-transform: uppercase; color: #a0aec0; margin-bottom: 10px; margin-top: 28px; }
  .warn-box { background: #fffbeb; border-left: 3px solid #f6ad55; padding: 12px 16px; border-radius: 2px; font-size: 13px; color: #744210; margin: 12px 0; }
  .info-box { background: #ebf8ff; border-left: 3px solid #63b3ed; padding: 12px 16px; border-radius: 2px; font-size: 13px; color: #2c5282; margin: 12px 0; }
  hr.qs { border: none; border-top: 1px solid #2d3748; margin: 24px 0; }
</style>
""", unsafe_allow_html=True)

# ── Helpers ───────────────────────────────────────────────────────────────────
INCOME_DECODE = {
    'HIC': 'High income',
    'UMC': 'Upper middle income',
    'LMC': 'Lower middle income',
    'LIC': 'Low income',
    'INX': 'Not classified',
}

def clean_str(val):
    """
    Safely extract a plain string from whatever the DB returns.
    Handles the case where wbgapi stored a dict representation
    as a string e.g. "{'id': 'LCN', 'value': 'Latin America & Caribbean'}".
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s or s in ('nan', 'None', 'null'):
        return None
    # Detect stringified dict — extract the 'value' key
    if s.startswith('{') and "'value'" in s:
        import ast
        try:
            d = ast.literal_eval(s)
            return d.get('value') or d.get('id') or None
        except Exception:
            pass
    return s

def decode_income(val):
    s = clean_str(val)
    if s is None:
        return 'Not classified'
    return INCOME_DECODE.get(s, s)

# ── DB ────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_db():
    return get_engine()

@st.cache_data(ttl=300)
def load_sources():
    with get_db().connect() as conn:
        rows = conn.execute(text(
            "SELECT source_id, full_name FROM metadata.sources ORDER BY full_name"
        )).fetchall()
    return {r[0]: r[1] for r in rows}

@st.cache_data(ttl=300)
def load_available_sources():
    """
    Load only sources that actually have data in standardized.observations.
    This prevents the researcher from selecting metrics from sources
    that haven't been ingested yet.
    """
    with get_db().connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT s.source_id, s.full_name
            FROM metadata.sources s
            WHERE EXISTS (
                SELECT 1 FROM standardized.observations o
                WHERE o.source_id = s.source_id
                LIMIT 1
            )
            ORDER BY s.full_name
        """)).fetchall()
    return {r[0]: r[1] for r in rows}

@st.cache_data(ttl=300)
def load_metrics_with_data():
    """
    Load only metrics that have actual observations in the silver layer.
    Uses a JOIN to standardized.observations so we never show a metric
    that has no data — selecting it would always return an empty result.
    """
    with get_db().connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT ON (m.metric_id)
                m.metric_id, m.metric_name, m.source_id, m.category,
                m.unit, m.available_from, m.available_to,
                m.country_count, m.missing_value_rate
            FROM metadata.metrics m
            WHERE EXISTS (
                SELECT 1 FROM standardized.observations o
                WHERE o.metric_id = m.metric_id
                LIMIT 1
            )
            ORDER BY m.metric_id, m.category NULLS LAST, m.metric_name
        """)).fetchall()
    df = pd.DataFrame(rows, columns=[
        'metric_id','metric_name','source_id','category',
        'unit','available_from','available_to',
        'country_count','missing_value_rate'
    ])
    df['available_from'] = pd.to_numeric(df['available_from'], errors='coerce')
    df['available_to']   = pd.to_numeric(df['available_to'],   errors='coerce')
    return df

@st.cache_data(ttl=300)
def load_countries():
    with get_db().connect() as conn:
        rows = conn.execute(text(
            "SELECT iso3, country_name, region, income_group FROM metadata.countries ORDER BY country_name"
        )).fetchall()
    df = pd.DataFrame(rows, columns=['iso3','country_name','region','income_group'])
    # Clean region and income — may be stringified dicts from wbgapi
    df['region']       = df['region'].apply(clean_str)
    df['income_label'] = df['income_group'].apply(decode_income)
    return df

# ── Load ──────────────────────────────────────────────────────────────────────
try:
    all_sources_map   = load_sources()
    avail_sources_map = load_available_sources()
    all_metrics       = load_metrics_with_data()
    all_countries     = load_countries()
except Exception as e:
    st.error(f"Cannot connect to database: {e}")
    st.stop()

# ── Header ────────────────────────────────────────────────────────────────────
col_hdr, col_back = st.columns([5, 1])
with col_hdr:
    st.markdown('<p class="page-title">Build Your Query</p>', unsafe_allow_html=True)
    st.markdown('<p class="page-sub">Select what you want to explore. Only metrics with available data are shown.</p>', unsafe_allow_html=True)
with col_back:
    st.markdown("<br><br>", unsafe_allow_html=True)
    if st.button("← Home", use_container_width=True):
        st.switch_page("Home.py")

# ══════════════════════════════════════════════════════════════════════════════
# FILTER 1 — SOURCES (only shows sources with data)
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-label">① Data Sources</div>', unsafe_allow_html=True)

n_total   = len(all_sources_map)
n_avail   = len(avail_sources_map)
n_pending = n_total - n_avail

if n_pending > 0:
    st.markdown(
        f'<div class="info-box">ℹ {n_avail} of {n_total} sources have data available. '
        f'{n_pending} source(s) are still being ingested and will appear here once ready.</div>',
        unsafe_allow_html=True,
    )

avail_source_options = list(avail_sources_map.keys())
avail_source_labels  = [avail_sources_map[s] for s in avail_source_options]

selected_source_labels = st.multiselect(
    label="Sources",
    options=avail_source_labels,
    default=None,
    placeholder="All available sources",
    label_visibility="collapsed",
)
selected_sources = [s for s, l in zip(avail_source_options, avail_source_labels) if l in selected_source_labels]

display_metrics = (
    all_metrics[all_metrics['source_id'].isin(selected_sources)].copy()
    if selected_sources else all_metrics.copy()
)

st.markdown('<hr class="qs">', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# FILTER 2 — METRICS (only metrics with actual data, no max_selections)
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-label">② Metrics</div>', unsafe_allow_html=True)

def build_metric_map(df):
    """
    Build display-label → metric_id mapping.
    Every label is unique — duplicates get metric_id suffix appended.
    """
    label_to_id    = {}
    ordered_labels = []
    seen           = {}
    for _, row in df.iterrows():
        src  = row['source_id'].upper()
        unit = f" ({row['unit']})" if pd.notna(row['unit']) and str(row['unit']).strip() else ''
        base = f"[{src}] {row['metric_name']}{unit}"
        label = f"{base} · {row['metric_id']}" if base in seen else base
        seen[base] = True
        label_to_id[label] = row['metric_id']
        ordered_labels.append(label)
    return label_to_id, ordered_labels

label_to_id, ordered_labels = build_metric_map(display_metrics)

if selected_sources:
    st.caption(f"{len(display_metrics)} metrics with data from selected sources.")
else:
    st.caption(f"{len(display_metrics)} metrics with available data across all ingested sources.")

# ── Metric picker as checkbox table ───────────────────────────────────────────
# The prior multiselect had a real UX problem: Streamlit's native dropdown
# closes after every selection, so with 300+ metrics picking 5 of them
# meant scroll-reopen-scroll-reopen. A data_editor with a checkbox column
# is the right primitive here — all rows stay visible, multiple checks
# happen without the component ever closing, and the metadata columns
# (source, unit, coverage years, country count) let researchers decide
# what's useful at a glance. Selection state persists in session so prior
# picks survive search/source filter changes.
SEL_KEY = '_selected_metric_ids'
if SEL_KEY not in st.session_state:
    st.session_state[SEL_KEY] = []

# Drop any prior selections that no longer appear in the current
# source-filtered set — e.g. user unchecked IMF after picking IMF metrics.
valid_ids_in_view = set(display_metrics['metric_id'])
st.session_state[SEL_KEY] = [m for m in st.session_state[SEL_KEY] if m in valid_ids_in_view]

# Optional text search narrows the visible rows. Checked rows that
# fall out of view still persist in session_state (no silent deselect).
metric_search = st.text_input(
    "Search metrics",
    placeholder="Type to filter (e.g. 'gdp', 'co2', 'trade')",
    key="metric_search",
    label_visibility="collapsed",
)

all_rows = display_metrics.set_index('metric_id').copy()
if metric_search and metric_search.strip():
    q = metric_search.strip().lower()
    mask = (
        all_rows['metric_name'].str.lower().str.contains(q, na=False) |
        all_rows['source_id'].str.lower().str.contains(q, na=False) |
        all_rows.index.to_series().str.lower().str.contains(q, na=False)
    )
    visible_rows = all_rows[mask]
    st.caption(f"{len(visible_rows)} metric(s) match \"{metric_search}\".")
else:
    visible_rows = all_rows

disp = visible_rows.copy()
disp.insert(0, 'Select', disp.index.isin(st.session_state[SEL_KEY]))
disp = disp[['Select', 'source_id', 'metric_name', 'unit',
             'available_from', 'available_to', 'country_count']]

edited = st.data_editor(
    disp,
    use_container_width=True,
    hide_index=True,
    height=380,
    column_config={
        'Select':         st.column_config.CheckboxColumn(" ", width="small"),
        'source_id':      st.column_config.TextColumn("Source", width="small"),
        'metric_name':    st.column_config.TextColumn("Metric", width="large"),
        'unit':           st.column_config.TextColumn("Unit", width="small"),
        'available_from': st.column_config.NumberColumn("From", format="%d", width="small"),
        'available_to':   st.column_config.NumberColumn("To", format="%d", width="small"),
        'country_count':  st.column_config.NumberColumn("Countries", format="%d", width="small"),
    },
    disabled=['source_id', 'metric_name', 'unit',
              'available_from', 'available_to', 'country_count'],
    key="metric_editor",
)

# Sync back: only the rows currently visible can flip state this rerun;
# invisible (filtered-out) selections are preserved from session_state.
visible_ids     = set(visible_rows.index)
now_checked     = set(edited[edited['Select']].index)
now_unchecked   = visible_ids - now_checked

prior = set(st.session_state[SEL_KEY])
prior -= now_unchecked
prior |= now_checked
st.session_state[SEL_KEY] = sorted(prior)

selected_metric_ids    = list(st.session_state[SEL_KEY])
# Keep labels around for the Results page's "missing metrics" warning.
id_to_label            = {v: k for k, v in label_to_id.items()}
selected_metric_labels = [id_to_label[m] for m in selected_metric_ids if m in id_to_label]
st.session_state['_selected_metric_labels'] = selected_metric_labels

st.caption(
    f"**{len(selected_metric_ids)}** metric(s) selected"
    + (f" (showing {len(visible_rows)} of {len(all_rows)})" if metric_search else "")
)

if len(selected_metric_ids) > 20:
    st.markdown(
        f'<div class="warn-box">⚠ {len(selected_metric_ids)} metrics selected. '
        f'Large selections may be slow. Consider splitting into multiple queries.</div>',
        unsafe_allow_html=True,
    )

st.markdown('<hr class="qs">', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# FILTER 3 — COUNTRIES & REGIONS
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-label">③ Countries & Regions</div>', unsafe_allow_html=True)

col_left, col_right = st.columns([1, 2], gap="large")

with col_left:
    st.caption("Quick-select by region")
    # Clean regions — drop None and empty strings
    available_regions = sorted([
        r for r in all_countries['region'].unique()
        if r is not None and str(r).strip() not in ('', 'nan', 'None')
    ])
    selected_regions = st.multiselect(
        label="Regions",
        options=available_regions,
        default=None,
        placeholder="All regions",
        label_visibility="collapsed",
        key="filter_regions",
    )

    st.caption("Filter by income group")
    income_label_options = sorted([
        v for v in all_countries['income_label'].unique()
        if v is not None and str(v).strip() not in ('', 'nan', 'None')
    ])
    selected_income_labels = st.multiselect(
        label="Income groups",
        options=income_label_options,
        default=None,
        placeholder="All income groups",
        label_visibility="collapsed",
        key="filter_income",
    )

# Pre-populate country pool from region + income
country_pool = all_countries.copy()
if selected_regions:
    country_pool = country_pool[country_pool['region'].isin(selected_regions)]
if selected_income_labels:
    country_pool = country_pool[country_pool['income_label'].isin(selected_income_labels)]

# Reset country default when filters change
filter_sig = f"{','.join(sorted(selected_regions))}__{','.join(sorted(selected_income_labels))}"
if st.session_state.get('_filter_sig') != filter_sig:
    st.session_state['_filter_sig'] = filter_sig
    st.session_state['_country_default'] = (
        sorted(country_pool['country_name'].tolist())
        if (selected_regions or selected_income_labels) else []
    )

all_country_names = sorted(all_countries['country_name'].tolist())
country_default   = st.session_state.get('_country_default', [])

with col_right:
    st.caption("Individual countries (adjustable after region/income selection)")

    # Search pre-filter for the country list. Same rationale as the metric
    # search — the native multiselect closes after each pick, so narrowing
    # the option list up-front turns picking from 200+ countries into
    # picking from a short shortlist.
    country_search = st.text_input(
        "Search countries",
        placeholder="Type to filter (e.g. 'south', 'arab', 'lebanon')",
        key="country_search",
        label_visibility="collapsed",
    )
    if country_search and country_search.strip():
        q = country_search.strip().lower()
        shown_countries = [c for c in all_country_names if q in c.lower()]
        st.caption(f"{len(shown_countries)} countr{'y' if len(shown_countries) == 1 else 'ies'} match \"{country_search}\".")
    else:
        shown_countries = all_country_names

    # Preserve prior selections across search changes.
    preserved = [c for c in (country_default or []) if c in all_country_names]
    country_options = list(dict.fromkeys(preserved + shown_countries))

    selected_country_names = st.multiselect(
        label="Countries",
        options=country_options,
        default=country_default or None,
        placeholder="All countries (no filter applied)",
        label_visibility="collapsed",
        key="filter_countries",
    )

if selected_country_names:
    selected_iso3 = all_countries[
        all_countries['country_name'].isin(selected_country_names)
    ]['iso3'].tolist()
    st.caption(f"{len(selected_iso3)} {'country' if len(selected_iso3) == 1 else 'countries'} selected.")
else:
    selected_iso3 = []
    st.caption("No country filter — all countries will be included.")

if (selected_regions or selected_income_labels) and not selected_country_names:
    st.markdown(
        '<div class="info-box">ℹ No countries matched your combination. All countries will be queried.</div>',
        unsafe_allow_html=True,
    )

st.markdown('<hr class="qs">', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# FILTER 4 — YEAR RANGE
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-label">④ Year Range</div>', unsafe_allow_html=True)

if selected_metric_ids:
    mi     = all_metrics[all_metrics['metric_id'].isin(selected_metric_ids)]
    from_v = mi['available_from'].dropna()
    to_v   = mi['available_to'].dropna()
    min_yr = max(1950, min(int(from_v.min()), 2026)) if not from_v.empty else 1950
    max_yr = max(1950, min(int(to_v.max()),   2026)) if not to_v.empty   else 2026
    if min_yr > max_yr:
        min_yr, max_yr = 1950, 2026
else:
    min_yr, max_yr = 1950, 2026

year_range = st.slider(
    label="Year range",
    min_value=1950,
    max_value=2026,
    value=(min_yr, max_yr),
    step=1,
    label_visibility="collapsed",
)

if selected_metric_ids:
    mi   = all_metrics[all_metrics['metric_id'].isin(selected_metric_ids)]
    gaps = []
    for _, row in mi.iterrows():
        af = row['available_from']
        at = row['available_to']
        if pd.notna(af) and int(af) > year_range[0]:
            gaps.append(f"{row['metric_name']} (starts {int(af)})")
        elif pd.notna(at) and int(at) < year_range[1]:
            gaps.append(f"{row['metric_name']} (ends {int(at)})")
    if gaps:
        shown = ', '.join(gaps[:3])
        more  = f" and {len(gaps)-3} more" if len(gaps) > 3 else ""
        st.markdown(
            f'<div class="info-box">ℹ Coverage gaps expected for: {shown}{more}.</div>',
            unsafe_allow_html=True,
        )

st.markdown('<hr class="qs">', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# QUERY SUMMARY + GET DATA
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-label">Query Summary</div>', unsafe_allow_html=True)

n_s = len(selected_sources)    if selected_sources    else "All"
n_m = len(selected_metric_ids) if selected_metric_ids else 0
n_c = len(selected_iso3)       if selected_iso3       else "All"

c1, c2, c3, c4 = st.columns(4)
c1.metric("Sources",   n_s)
c2.metric("Metrics",   n_m)
c3.metric("Countries", n_c)
c4.metric("Years",     f"{year_range[0]}–{year_range[1]}")

ready = bool(selected_metric_ids)
if not ready:
    st.markdown(
        '<div class="warn-box">⚠ Select at least one metric to run the query.</div>',
        unsafe_allow_html=True,
    )

if ready and selected_iso3:
    estimated = n_m * len(selected_iso3) * (year_range[1] - year_range[0] + 1)
    if estimated > 500_000:
        st.markdown(
            f'<div class="warn-box">⚠ Estimated ~{estimated:,} rows. Query will run but may be slow.</div>',
            unsafe_allow_html=True,
        )

st.markdown("<br>", unsafe_allow_html=True)
_, col_btn, _ = st.columns([1, 2, 1])
with col_btn:
    if st.button(
        "Get Data →" if ready else "Select at least one metric to continue",
        disabled=not ready,
        use_container_width=True,
        type="primary",
    ):
        st.session_state['query'] = {
            'sources':       selected_sources,
            'metric_ids':    selected_metric_ids,
            'metric_labels': selected_metric_labels,
            'iso3':          selected_iso3,
            'country_names': selected_country_names,
            'year_min':      year_range[0],
            'year_max':      year_range[1],
        }
        st.switch_page("pages/2_Results.py")

st.markdown("<br><br>", unsafe_allow_html=True)
