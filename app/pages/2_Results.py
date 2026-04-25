"""
app/pages/2_Results.py
======================
Page 3 — Query results.
All sections (charts, stats, drill-down) are scoped strictly
to metrics that returned actual data, never to the full metrics table.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from database.connection import get_engine

st.set_page_config(
    page_title="Results — AI & Economic Data",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500&display=swap');
  html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
  .block-container { padding-top: 32px !important; max-width: 1400px !important; }
  #MainMenu, footer, header { visibility: hidden; }
  .page-title { font-family: 'DM Serif Display', serif; font-size: 38px; color: #0a0e1a; margin-bottom: 4px; font-weight: 400; }
  .page-sub { font-size: 15px; color: #718096; margin-bottom: 28px; font-weight: 300; }
  .section-label { font-size: 11px; font-weight: 500; letter-spacing: 2.5px; text-transform: uppercase; color: #a0aec0; margin-bottom: 10px; margin-top: 32px; }
  .stat-card { background: #f7fafc; border: 1px solid #e2e8f0; border-radius: 4px; padding: 20px 24px; text-align: center; }
  .stat-card-num { font-family: 'DM Serif Display', serif; font-size: 32px; color: #0a0e1a; line-height: 1; }
  .stat-card-label { font-size: 12px; color: #a0aec0; margin-top: 6px; letter-spacing: 1px; text-transform: uppercase; }
  .stat-card-sub { font-size: 12px; color: #718096; margin-top: 4px; }
  .missing-bar-wrap { margin-top: 12px; }
  .missing-bar-bg { background: #edf2f7; border-radius: 3px; height: 8px; overflow: hidden; }
  .missing-bar-fill { height: 8px; border-radius: 3px; }
  .info-box { background: #ebf8ff; border-left: 3px solid #63b3ed; padding: 10px 14px; border-radius: 2px; font-size: 13px; color: #2c5282; margin: 8px 0; }
  .warn-box { background: #fffbeb; border-left: 3px solid #f6ad55; padding: 10px 14px; border-radius: 2px; font-size: 13px; color: #744210; margin: 8px 0; }
  hr.rs { border: none; border-top: 1px solid #edf2f7; margin: 28px 0; }
</style>
""", unsafe_allow_html=True)

# ── Guard ─────────────────────────────────────────────────────────────────────
if 'query' not in st.session_state:
    st.markdown('<p class="page-title">No query found</p>', unsafe_allow_html=True)
    st.write("Please build a query first.")
    if st.button("← Go to Query page"):
        st.switch_page("pages/1_Query.py")
    st.stop()

q = st.session_state['query']

@st.cache_resource
def get_db():
    return get_engine()

# ── Query ─────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=120, show_spinner="Fetching data...")
def run_query(metric_ids_tuple, iso3_tuple, year_min, year_max):
    with get_db().connect() as conn:
        params = {
            'metrics':  list(metric_ids_tuple),
            'year_min': year_min,
            'year_max': year_max,
        }
        country_clause = ""
        if iso3_tuple:
            params['iso3s'] = list(iso3_tuple)
            country_clause  = "AND o.country_iso3 = ANY(:iso3s)"

        df = pd.read_sql(text(f"""
            SELECT
                o.country_iso3,
                c.country_name,
                c.region,
                o.year,
                o.metric_id,
                m.metric_name,
                m.source_id,
                m.unit,
                m.category,
                o.value
            FROM standardized.observations o
            JOIN metadata.countries c ON c.iso3 = o.country_iso3
            JOIN metadata.metrics   m ON m.metric_id = o.metric_id
            WHERE o.metric_id = ANY(:metrics)
              AND o.year BETWEEN :year_min AND :year_max
              {country_clause}
            ORDER BY o.year, c.country_name, o.metric_id
        """), conn, params=params)

    # Safe numeric cast — never crashes on non-numeric values
    df['value_num'] = pd.to_numeric(df['value'], errors='coerce')
    return df

@st.cache_data(ttl=300)
def load_metric_meta(metric_ids_tuple):
    with get_db().connect() as conn:
        rows = conn.execute(text("""
            SELECT metric_id, metric_name, source_id, unit,
                   category, description, frequency,
                   available_from, available_to,
                   country_count, observation_count, missing_value_rate
            FROM metadata.metrics
            WHERE metric_id = ANY(:mids)
            ORDER BY metric_name
        """), {'mids': list(metric_ids_tuple)}).fetchall()
    df = pd.DataFrame(rows, columns=[
        'metric_id','metric_name','source_id','unit','category',
        'description','frequency','available_from','available_to',
        'country_count','observation_count','missing_value_rate'
    ])
    df['available_from'] = pd.to_numeric(df['available_from'], errors='coerce')
    df['available_to']   = pd.to_numeric(df['available_to'],   errors='coerce')
    return df

# ── Header ────────────────────────────────────────────────────────────────────
col_title, col_refine = st.columns([5, 1])
with col_title:
    st.markdown('<p class="page-title">Query Results</p>', unsafe_allow_html=True)
    preview = q['metric_labels'][:3]
    suffix  = f" +{len(q['metric_labels'])-3} more" if len(q['metric_labels']) > 3 else ""
    st.markdown(
        f'<p class="page-sub">{", ".join(preview)}{suffix}'
        f' &nbsp;·&nbsp; {q["year_min"]}–{q["year_max"]}</p>',
        unsafe_allow_html=True,
    )
with col_refine:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("← Refine Query", use_container_width=True):
        st.switch_page("pages/1_Query.py")

# ── Fetch ─────────────────────────────────────────────────────────────────────
try:
    df = run_query(
        metric_ids_tuple = tuple(q['metric_ids']),
        iso3_tuple       = tuple(q['iso3']) if q['iso3'] else None,
        year_min         = q['year_min'],
        year_max         = q['year_max'],
    )
    # Load metadata ONLY for metrics that actually returned data
    # This ensures all dropdowns, charts, and stats are scoped
    # to the real result set — never to the full metrics table
    metrics_with_data = tuple(df['metric_id'].unique().tolist())
    meta_full = load_metric_meta(metrics_with_data) if metrics_with_data else pd.DataFrame()

except Exception as e:
    st.error(f"Query failed: {e}")
    if st.button("← Refine Query"):
        st.switch_page("pages/1_Query.py")
    st.stop()

# ── No data at all ────────────────────────────────────────────────────────────
if df.empty:
    st.markdown(
        '<div class="warn-box">⚠ No data found for your filters. '
        'Try broadening the year range, adding more countries, or selecting different metrics.</div>',
        unsafe_allow_html=True,
    )
    if st.button("← Refine Query"):
        st.switch_page("pages/1_Query.py")
    st.stop()

# ── Warn about metrics that returned no data ──────────────────────────────────
requested_ids  = set(q['metric_ids'])
returned_ids   = set(df['metric_id'].unique())
missing_ids    = requested_ids - returned_ids

if missing_ids:
    # Get the human-readable names from the labels the user selected
    label_map = {v: k for k, v in zip(q['metric_labels'], q['metric_ids'])}
    missing_names = [label_map.get(mid, mid) for mid in missing_ids]
    shown = ', '.join(missing_names[:5])
    more  = f" and {len(missing_names)-5} more" if len(missing_names) > 5 else ""
    st.markdown(
        f'<div class="warn-box">⚠ {len(missing_ids)} selected metric(s) returned no data for your '
        f'filters and are excluded from all results below: {shown}{more}. '
        f'This usually means those sources have not been ingested yet, or there is no data '
        f'for your selected countries/year range.</div>',
        unsafe_allow_html=True,
    )

# ── Summary stats ─────────────────────────────────────────────────────────────
n_countries_found = df['country_iso3'].nunique()
n_metrics_found   = df['metric_id'].nunique()
year_min_found    = int(df['year'].min())
year_max_found    = int(df['year'].max())
total_rows        = len(df)
yr_span           = q['year_max'] - q['year_min'] + 1
total_possible    = n_countries_found * yr_span * n_metrics_found
missing_pct       = max(0.0, round(100 * (1 - total_rows / total_possible), 1)) if total_possible > 0 else 0.0

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — DATA PREVIEW + DOWNLOADS + COVERAGE
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-label">Data Preview</div>', unsafe_allow_html=True)

# ── Wide-format pivot ─────────────────────────────────────────────────────────
# The underlying df stays long format because charts, summary stats, and
# drill-down all depend on it. We build a separate wide view purely for the
# preview table and the CSV download:
#     country_iso3 | country_name | year | <source_id>.<metric_name> | ...
# Column names are sanitized (newlines/carriage-returns stripped) so they
# never break CSV parsers. The unit column is intentionally omitted — unit
# is per-metric, not per-cell, and lives in the metadata CSV already.
def _safe_col(source_id, metric_name):
    s = f"{source_id}.{metric_name}"
    return s.replace('\n', ' ').replace('\r', ' ').strip()

df_wide_src = df.copy()
df_wide_src['_col'] = df_wide_src.apply(
    lambda r: _safe_col(r['source_id'], r['metric_name']), axis=1
)
df_wide = (
    df_wide_src
    .pivot_table(
        index   = ['country_iso3', 'country_name', 'year'],
        columns = '_col',
        values  = 'value',
        aggfunc = 'first',   # one row per (country, year, metric) by PK; 'first' is safe
    )
    .reset_index()
    .sort_values(['country_name', 'year'])
)
df_wide.columns.name = None

col_preview, col_summary = st.columns([3, 2], gap="large")

with col_preview:
    st.dataframe(
        df_wide.head(10),
        use_container_width=True,
        height=300,
    )

    dl1, dl2 = st.columns(2)
    with dl1:
        st.download_button(
            label=f"⬇ Download CSV ({len(df_wide):,} rows)",
            data=df_wide.to_csv(index=False).encode('utf-8'),
            file_name=f"ai_economic_data_{pd.Timestamp.now():%Y%m%d}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with dl2:
        if not meta_full.empty:
            # Add query-specific coverage columns
            qry_cov = df.groupby('metric_id').agg(
                query_first_year   = ('year',        'min'),
                query_last_year    = ('year',        'max'),
                query_countries    = ('country_iso3','nunique'),
                query_observations = ('value',       'count'),
            ).reset_index()
            meta_export = meta_full.merge(qry_cov, on='metric_id', how='left')
            meta_csv    = meta_export.to_csv(index=False).encode('utf-8')
        else:
            meta_csv = b"No metadata available"
        st.download_button(
            label="⬇ Download Metadata",
            data=meta_csv,
            file_name="ai_economic_metadata.csv",
            mime="text/csv",
            use_container_width=True,
        )

with col_summary:
    st.markdown('<div class="section-label">Coverage Overview</div>', unsafe_allow_html=True)

    r1c1, r1c2 = st.columns(2)
    r2c1, r2c2 = st.columns(2)

    with r1c1:
        st.markdown(f"""<div class="stat-card">
          <div class="stat-card-num">{n_countries_found}</div>
          <div class="stat-card-label">Countries</div>
          <div class="stat-card-sub">with data</div></div>""", unsafe_allow_html=True)
    with r1c2:
        st.markdown(f"""<div class="stat-card">
          <div class="stat-card-num">{n_metrics_found}</div>
          <div class="stat-card-label">Metrics</div>
          <div class="stat-card-sub">returned data</div></div>""", unsafe_allow_html=True)
    with r2c1:
        st.markdown(f"""<div class="stat-card">
          <div class="stat-card-num">{year_min_found}–{year_max_found}</div>
          <div class="stat-card-label">Years</div>
          <div class="stat-card-sub">covered</div></div>""", unsafe_allow_html=True)
    with r2c2:
        bar_color = "#fc8181" if missing_pct > 30 else "#f6ad55" if missing_pct > 10 else "#68d391"
        st.markdown(f"""<div class="stat-card">
          <div class="stat-card-num">{missing_pct}%</div>
          <div class="stat-card-label">Missing Values</div>
          <div class="missing-bar-wrap"><div class="missing-bar-bg">
            <div class="missing-bar-fill" style="width:{missing_pct}%;background:{bar_color};"></div>
          </div></div></div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    with st.expander("Missing values per metric"):
        for mid in df['metric_id'].unique():
            mdf   = df[df['metric_id'] == mid]
            mname = mdf['metric_name'].iloc[0]
            denom = n_countries_found * yr_span
            pct   = max(0.0, round(100 * (1 - len(mdf) / denom), 1)) if denom > 0 else 0.0
            icon  = "🔴" if pct > 30 else "🟡" if pct > 10 else "🟢"
            st.markdown(f"{icon} **{mname}** — {pct}% missing")

    with st.expander("Countries with incomplete metric coverage"):
        per_country = df.groupby('country_name')['metric_id'].nunique()
        incomplete  = per_country[per_country < n_metrics_found].sort_values()
        if incomplete.empty:
            st.markdown("All countries have data for all returned metrics.")
        else:
            for country, cnt in incomplete.items():
                st.markdown(f"**{country}** — {cnt}/{n_metrics_found} metrics")

st.markdown('<hr class="rs">', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — VISUALISATIONS
# Chart type is auto-picked per metric type:
#   binary metrics (values ⊆ {0, 1})  → bar chart with SUM aggregation
#                                       (count of 1-countries per year)
#   continuous metrics                 → line chart with MEAN aggregation
# The user can still override chart type and aggregation per chart.
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-label">Visualisations</div>', unsafe_allow_html=True)

# Metrics present in the actual result — used for ALL chart pickers
result_metric_ids  = df['metric_id'].unique().tolist()
result_mid_to_name = {
    mid: df[df['metric_id'] == mid]['metric_name'].iloc[0]
    for mid in result_metric_ids
}

# ── Metric type detection ─────────────────────────────────────────────────────
# A metric is classified binary iff its numeric values in this result
# set are strictly in {0, 1}. Anything else (including metrics with a
# tiny discrete set of values that happens not to include 0/1) is
# treated as continuous. The detection is per-result so the same metric
# could classify differently in different queries — that's correct
# behavior: a 0/1-valued indicator filtered to countries where it's
# always 1 isn't meaningfully binary for that query.
BINARY_ALLOWED = {0, 1, 0.0, 1.0}

def classify_metric(metric_id):
    vals = df[df['metric_id'] == metric_id]['value_num'].dropna()
    if vals.empty:
        return 'continuous'
    return 'binary' if set(vals.unique()).issubset(BINARY_ALLOWED) else 'continuous'

metric_type_map    = {mid: classify_metric(mid) for mid in result_metric_ids}
continuous_metrics = [m for m in result_metric_ids if metric_type_map[m] == 'continuous']
binary_metrics     = [m for m in result_metric_ids if metric_type_map[m] == 'binary']

# Shared year range slider (used by both charts)
available_years = sorted(df['year'].unique().tolist())
if len(available_years) > 1:
    chart_year_range = st.select_slider(
        "Chart year range",
        options=available_years,
        value=(available_years[0], available_years[-1]),
    )
else:
    chart_year_range = (available_years[0], available_years[0])
    st.caption(f"Single year available: {available_years[0]}")

if binary_metrics and continuous_metrics:
    st.caption(
        f"Detected {len(continuous_metrics)} continuous metric(s) "
        f"and {len(binary_metrics)} binary metric(s). "
        "Chart defaults are auto-picked by type — override freely below."
    )
elif binary_metrics:
    st.caption(f"All {len(binary_metrics)} selected metric(s) are binary — defaulting to frequency bar charts.")

chart_df = df[df['year'].between(chart_year_range[0], chart_year_range[1])].copy()

CHART_COLORS = px.colors.qualitative.Set2

def make_chart(chart_type, cdf, metrics_sel, agg_fn, country_filter=None):
    """Build a plotly figure. Returns None if data insufficient."""
    d = cdf[cdf['metric_id'].isin(metrics_sel)].copy()
    if country_filter:
        d = d[d['country_name'].isin(country_filter)]
    if d.empty:
        return None

    if chart_type in ("Line chart", "Bar chart"):
        agg = (
            d.groupby(['year','metric_id','metric_name'])['value_num']
            .agg(agg_fn)
            .reset_index()
            .rename(columns={'value_num': 'val'})
        )
        agg = agg.dropna(subset=['val'])
        if agg.empty:
            return None
        # Label the y-axis more helpfully when binary metrics are summed
        # — 'Sum value' reads as 'count of 1-countries' in that context.
        all_binary = all(metric_type_map.get(m) == 'binary' for m in metrics_sel)
        if chart_type == "Bar chart" and agg_fn == 'sum' and all_binary:
            label_y = "Countries with value = 1"
        else:
            label_y = f"{agg_fn.title()} value"
        if chart_type == "Line chart":
            fig = px.line(agg, x='year', y='val', color='metric_name', markers=True,
                          labels={'val': label_y, 'year': 'Year', 'metric_name': 'Metric'},
                          color_discrete_sequence=CHART_COLORS)
        else:
            fig = px.bar(agg, x='year', y='val', color='metric_name', barmode='group',
                         labels={'val': label_y, 'year': 'Year', 'metric_name': 'Metric'},
                         color_discrete_sequence=CHART_COLORS)

    else:  # Scatter
        if len(metrics_sel) < 2:
            return "need_two"
        m1, m2 = metrics_sel[0], metrics_sel[1]
        n1, n2 = result_mid_to_name.get(m1, m1), result_mid_to_name.get(m2, m2)
        pivot  = d[d['metric_id'].isin([m1, m2])].pivot_table(
            index=['country_name','year'], columns='metric_id', values='value_num'
        ).reset_index()
        pivot.columns.name = None
        if m1 not in pivot.columns or m2 not in pivot.columns:
            return None
        pivot = pivot.dropna(subset=[m1, m2])
        if pivot.empty:
            return None
        fig = px.scatter(pivot, x=m1, y=m2, color='country_name',
                         hover_data=['year','country_name'],
                         labels={m1: n1, m2: n2},
                         color_discrete_sequence=CHART_COLORS)

    fig.update_layout(
        plot_bgcolor='white', paper_bgcolor='white',
        font_family='DM Sans', legend_title_text='',
        margin=dict(l=20, r=20, t=20, b=20), height=380,
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(gridcolor='#f0f4f8')
    return fig

# Fragment decorator — each chart reruns independently when its own
# controls change, instead of forcing a full page rerun that re-executes
# the Supabase fetch and the summary-stats groupby. Graceful fallback
# to a no-op decorator on older Streamlit versions.
_fragment = getattr(st, 'fragment', None) or getattr(st, 'experimental_fragment', None) or (lambda f: f)

def _build_chart_section(chart_num, default_metrics, default_type, default_agg):
    st.markdown(f"**Chart {chart_num}**")
    ca, cb, cc, cd = st.columns([2, 3, 2, 1])

    # Order chart_types so the preferred default is first (Streamlit
    # selectbox picks index 0 on first render).
    all_types = ["Line chart", "Bar chart", "Scatter plot"]
    ordered_types = [default_type] + [t for t in all_types if t != default_type]
    all_aggs = ["Mean", "Sum", "Median"]
    ordered_aggs = [default_agg] + [a for a in all_aggs if a != default_agg]

    with ca:
        ctype = st.selectbox("Chart type", ordered_types, key=f"c{chart_num}_type")
    with cb:
        sel = st.multiselect(
            "Metrics (max 5)",
            options=result_metric_ids,
            format_func=lambda x: result_mid_to_name.get(x, x) + (
                " · binary" if metric_type_map.get(x) == 'binary' else ""
            ),
            default=default_metrics[:min(len(default_metrics), 3)],
            max_selections=5,
            key=f"c{chart_num}_metrics",
        )
    with cc:
        countries_avail = sorted(chart_df['country_name'].unique().tolist())
        cfilt = st.multiselect(
            "Filter countries", options=countries_avail,
            default=None, placeholder="All countries",
            key=f"c{chart_num}_countries",
        )
    with cd:
        agg = st.selectbox("Aggregate", ordered_aggs, key=f"c{chart_num}_agg")

    if not sel:
        st.markdown('<div class="info-box">ℹ Select at least one metric.</div>', unsafe_allow_html=True)
        return

    fig = make_chart(ctype, chart_df, sel, agg.lower(), cfilt or None)
    if fig == "need_two":
        st.markdown('<div class="info-box">ℹ Scatter plot needs at least 2 metrics. Select a second metric or switch chart type.</div>', unsafe_allow_html=True)
    elif fig is None:
        st.markdown('<div class="info-box">ℹ No numeric data for this combination.</div>', unsafe_allow_html=True)
    else:
        st.plotly_chart(fig, use_container_width=True, key=f"c{chart_num}_plotly")

# Chart 1 — default: continuous metrics on a line chart
c1_defaults = (continuous_metrics or result_metric_ids)[:3]
c1_type     = "Line chart" if continuous_metrics else "Bar chart"
c1_agg      = "Mean"       if continuous_metrics else "Sum"

@_fragment
def chart_1_section():
    _build_chart_section(1, c1_defaults, c1_type, c1_agg)

chart_1_section()

st.markdown('<hr class="rs">', unsafe_allow_html=True)

# Chart 2 — default picks the *other* metric type so the two charts
# complement rather than duplicate each other.
#   - binary metrics present           → bar chart (count of 1s per year)
#   - else 2+ continuous remaining     → scatter comparing two metrics
#   - else                             → second line chart
if binary_metrics:
    c2_defaults = binary_metrics[:3]
    c2_type     = "Bar chart"
    c2_agg      = "Sum"
else:
    remaining = [m for m in continuous_metrics if m not in c1_defaults]
    if len(remaining) >= 2:
        c2_defaults = remaining[:2]
        c2_type     = "Scatter plot"
        c2_agg      = "Mean"
    elif continuous_metrics:
        c2_defaults = continuous_metrics[:min(3, len(continuous_metrics))]
        c2_type     = "Line chart"
        c2_agg      = "Mean"
    else:
        c2_defaults = result_metric_ids[:2]
        c2_type     = "Bar chart"
        c2_agg      = "Mean"

@_fragment
def chart_2_section():
    _build_chart_section(2, c2_defaults, c2_type, c2_agg)

chart_2_section()

st.markdown('<hr class="rs">', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — SUMMARY STATISTICS
# Scoped to metrics in df only
# ══════════════════════════════════════════════════════════════════════════════
st.markdown('<div class="section-label">Summary Statistics</div>', unsafe_allow_html=True)

stat_col, drill_col = st.columns([2, 1], gap="large")

with stat_col:
    # Group by metric_id + metric_name only — unit can be None/NaN and
    # groupby silently drops NaN keys if unit is in the group
    summary = (
        df.groupby(['metric_id','metric_name'])['value_num']
        .agg(Count='count', Min='min', Mean='mean', Median='median', Max='max', Std='std')
        .reset_index()
    )
    # Attach unit separately to avoid NaN groupby issue
    unit_map = df.groupby('metric_id')['unit'].first().reset_index()
    summary  = summary.merge(unit_map, on='metric_id', how='left')

    for col in ['Min','Mean','Median','Max','Std']:
        summary[col] = summary[col].apply(lambda x: f"{x:,.4g}" if pd.notna(x) else "—")

    st.dataframe(
        summary[['metric_name','unit','Count','Min','Mean','Median','Max','Std']],
        use_container_width=True,
        hide_index=True,
    )

with drill_col:
    st.markdown("**Drill-down**")
    st.caption("Min / mean / max for one metric, grouped by country or year.")

    # Picker uses ONLY metrics present in the result
    drill_metric = st.selectbox(
        "Metric",
        options=result_metric_ids,
        format_func=lambda x: result_mid_to_name.get(x, x),
        key="drill_metric",
    )
    drill_by = st.radio("Group by", ["Country", "Year"], horizontal=True, key="drill_by")

    ddf       = df[df['metric_id'] == drill_metric].copy()
    group_col = 'country_name' if drill_by == "Country" else 'year'
    drill_summary = (
        ddf.groupby(group_col)['value_num']
        .agg(Min='min', Mean='mean', Max='max')
        .reset_index()
        .sort_values('Mean', ascending=False)
    )
    for col in ['Min','Mean','Max']:
        drill_summary[col] = drill_summary[col].apply(lambda x: f"{x:,.4g}" if pd.notna(x) else "—")
    st.dataframe(
        drill_summary.rename(columns={group_col: drill_by}),
        use_container_width=True,
        hide_index=True,
        height=320,
    )

st.markdown('<hr class="rs">', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — METRIC DEFINITIONS
# Only for metrics that actually returned data
# ══════════════════════════════════════════════════════════════════════════════
if not meta_full.empty:
    st.markdown('<div class="section-label">Metric Definitions</div>', unsafe_allow_html=True)
    st.caption("Definitions and coverage statistics for metrics that returned data.")

    for _, row in meta_full.iterrows():
        with st.expander(f"**{row['metric_name']}** — [{row['source_id'].upper()}]"):
            c1d, c2d, c3d = st.columns(3)
            c1d.markdown(f"**Unit:** {row['unit'] or '—'}")
            c2d.markdown(f"**Category:** {row['category'] or '—'}")
            c3d.markdown(f"**Frequency:** {row['frequency'] or '—'}")

            cov_parts = []
            if pd.notna(row['available_from']) and pd.notna(row['available_to']):
                cov_parts.append(f"Coverage: {int(row['available_from'])}–{int(row['available_to'])}")
            if pd.notna(row['country_count']):
                cov_parts.append(f"{int(row['country_count'])} countries")
            if pd.notna(row['observation_count']):
                cov_parts.append(f"{int(row['observation_count']):,} observations")
            if pd.notna(row['missing_value_rate']):
                cov_parts.append(f"{float(row['missing_value_rate']):.1f}% missing")
            if cov_parts:
                st.caption(" · ".join(cov_parts))

            desc = row['description']
            if pd.notna(desc) and str(desc).strip():
                st.markdown(str(desc).strip())
            else:
                st.markdown("*No description available for this metric.*")

st.markdown("<br><br>", unsafe_allow_html=True)
