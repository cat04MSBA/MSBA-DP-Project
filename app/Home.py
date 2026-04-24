"""
app/Home.py
===========
Page 1 — Landing page.
Clean, minimal entry point with product description and CTA.
"""

import streamlit as st
import sys, os
from sqlalchemy import text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from database.connection import get_engine

st.set_page_config(
    page_title="AI & Economic Data Aggregator",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Live stats ────────────────────────────────────────────────────────────────
# Single cheap query for all four numbers. Cached for 10 minutes so the
# landing page doesn't hammer Supabase on every refresh. Falls back to
# static placeholders if the DB is unreachable — we never want the
# landing page to 500 just because Supabase paused.
@st.cache_data(ttl=600)
def load_landing_stats():
    try:
        with get_engine().connect() as conn:
            row = conn.execute(text("""
                SELECT
                    (SELECT COUNT(DISTINCT source_id)   FROM standardized.observations) AS sources,
                    (SELECT COUNT(*)                    FROM standardized.observations) AS obs,
                    (SELECT COUNT(DISTINCT country_iso3) FROM standardized.observations) AS countries,
                    (SELECT MIN(year)                   FROM standardized.observations) AS min_year,
                    (SELECT MAX(year)                   FROM standardized.observations) AS max_year
            """)).fetchone()
        return {
            'sources':   int(row[0] or 0),
            'obs':       int(row[1] or 0),
            'countries': int(row[2] or 0),
            'span_yr':   int((row[4] or 0) - (row[3] or 0)) if row[3] and row[4] else 0,
        }
    except Exception:
        return None

def _fmt_obs(n):
    if n >= 1_000_000: return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:     return f"{n / 1_000:.0f}K"
    return str(n)

stats = load_landing_stats()
if stats:
    STAT_SOURCES   = str(stats['sources'])
    STAT_OBS       = _fmt_obs(stats['obs'])
    STAT_COUNTRIES = f"{stats['countries']}"
    STAT_SPAN      = f"{stats['span_yr']}yr" if stats['span_yr'] else "—"
else:
    STAT_SOURCES, STAT_OBS, STAT_COUNTRIES, STAT_SPAN = "7", "2M+", "200+", "75yr"

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500&display=swap');

  /* Global resets */
  html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
  }
  .block-container {
    padding-top: 0 !important;
    padding-bottom: 0 !important;
    max-width: 100% !important;
  }
  #MainMenu, footer, header { visibility: hidden; }
  [data-testid="collapsedControl"] { display: none; }

  /* Hero section */
  .hero {
    min-height: 100vh;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: flex-start;
    padding: 80px 10% 60px 10%;
    background: #0a0e1a;
    position: relative;
    overflow: hidden;
  }
  .hero::before {
    content: '';
    position: absolute;
    top: -30%;
    right: -10%;
    width: 700px;
    height: 700px;
    background: radial-gradient(circle, rgba(99,179,237,0.08) 0%, transparent 70%);
    pointer-events: none;
  }
  .hero::after {
    content: '';
    position: absolute;
    bottom: -20%;
    left: 20%;
    width: 500px;
    height: 500px;
    background: radial-gradient(circle, rgba(154,117,234,0.06) 0%, transparent 70%);
    pointer-events: none;
  }

  /* Tag */
  .tag {
    display: inline-block;
    font-family: 'DM Sans', sans-serif;
    font-size: 11px;
    font-weight: 500;
    letter-spacing: 3px;
    text-transform: uppercase;
    color: #63b3ed;
    border: 1px solid rgba(99,179,237,0.3);
    padding: 6px 14px;
    border-radius: 2px;
    margin-bottom: 32px;
  }

  /* Headline */
  .headline {
    font-family: 'DM Serif Display', serif;
    font-size: clamp(42px, 5.5vw, 80px);
    font-weight: 400;
    line-height: 1.08;
    color: #f0f4ff;
    margin: 0 0 12px 0;
    max-width: 800px;
  }
  .headline em {
    font-style: italic;
    color: #93c5fd;
  }

  /* Sub-headline */
  .subheadline {
    font-family: 'DM Sans', sans-serif;
    font-size: 18px;
    font-weight: 300;
    color: #8896b0;
    line-height: 1.6;
    max-width: 580px;
    margin: 24px 0 52px 0;
  }

  /* CTA button */
  .cta-btn {
    display: inline-flex;
    align-items: center;
    gap: 10px;
    background: #63b3ed;
    color: #0a0e1a;
    font-family: 'DM Sans', sans-serif;
    font-size: 15px;
    font-weight: 500;
    padding: 16px 36px;
    border-radius: 3px;
    text-decoration: none;
    cursor: pointer;
    transition: all 0.2s ease;
    border: none;
    letter-spacing: 0.3px;
  }
  .cta-btn:hover {
    background: #90cdf4;
    transform: translateY(-1px);
  }
  .cta-arrow {
    font-size: 18px;
    transition: transform 0.2s;
  }
  .cta-btn:hover .cta-arrow {
    transform: translateX(4px);
  }

  /* Source pills */
  .sources-row {
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
    margin-top: 64px;
    align-items: center;
  }
  .sources-label {
    font-size: 11px;
    font-weight: 500;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: #4a5568;
    margin-right: 6px;
  }
  .source-pill {
    font-size: 12px;
    color: #718096;
    border: 1px solid #1e2d45;
    padding: 5px 12px;
    border-radius: 20px;
    background: rgba(255,255,255,0.02);
  }

  /* Stats row */
  .stats-row {
    display: flex;
    gap: 60px;
    margin-top: 72px;
    padding-top: 48px;
    border-top: 1px solid #1a2235;
  }
  .stat-item {}
  .stat-num {
    font-family: 'DM Serif Display', serif;
    font-size: 36px;
    color: #e2e8f0;
    line-height: 1;
  }
  .stat-label {
    font-size: 13px;
    color: #4a5568;
    margin-top: 6px;
    font-weight: 400;
    letter-spacing: 0.3px;
  }

  /* Feature cards row */
  .features {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 1px;
    background: #1a2235;
    border: 1px solid #1a2235;
    margin-top: 0;
  }
  .feature-card {
    background: #0d1220;
    padding: 40px 36px;
  }
  .feature-icon {
    font-size: 24px;
    margin-bottom: 16px;
  }
  .feature-title {
    font-family: 'DM Serif Display', serif;
    font-size: 20px;
    color: #e2e8f0;
    margin-bottom: 10px;
  }
  .feature-desc {
    font-size: 14px;
    color: #4a5568;
    line-height: 1.65;
  }

  /* Divider line */
  .divider {
    height: 1px;
    background: #1a2235;
    margin: 0;
  }
</style>
""", unsafe_allow_html=True)

# ── Hero ──────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero">
  <div class="tag">MSBA 305 &nbsp;·&nbsp; AUB &nbsp;·&nbsp; 2026</div>

  <h1 class="headline">
    Global AI &amp; Economic<br>
    Data, <em>unified.</em>
  </h1>

  <p class="subheadline">
    A research platform aggregating AI readiness, economic productivity,
    and innovation metrics from 7 international sources into a single,
    queryable dataset spanning 1950–2026.
  </p>

  <div class="sources-row">
    <span class="sources-label">Sources</span>
    <span class="source-pill">World Bank</span>
    <span class="source-pill">IMF</span>
    <span class="source-pill">Oxford Insights</span>
    <span class="source-pill">Penn World Tables</span>
    <span class="source-pill">OpenAlex</span>
    <span class="source-pill">WIPO</span>
    <span class="source-pill">OECD MSTI</span>
  </div>

  <div class="stats-row">
    <div class="stat-item">
      <div class="stat-num">""" + STAT_SOURCES + """</div>
      <div class="stat-label">Data sources</div>
    </div>
    <div class="stat-item">
      <div class="stat-num">""" + STAT_OBS + """</div>
      <div class="stat-label">Observations</div>
    </div>
    <div class="stat-item">
      <div class="stat-num">""" + STAT_COUNTRIES + """</div>
      <div class="stat-label">Countries</div>
    </div>
    <div class="stat-item">
      <div class="stat-num">""" + STAT_SPAN + """</div>
      <div class="stat-label">Time coverage</div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

# ── Feature cards ─────────────────────────────────────────────────────────────
st.markdown("""
<div class="features">
  <div class="feature-card">
    <div class="feature-icon">⚡</div>
    <div class="feature-title">Cross-source queries</div>
    <div class="feature-desc">
      Combine metrics from different institutions in a single query.
      Filter by source, metric, country, region, and year range simultaneously.
    </div>
  </div>
  <div class="feature-card">
    <div class="feature-icon">📊</div>
    <div class="feature-title">Built-in visualisation</div>
    <div class="feature-desc">
      Line charts, bar charts, and scatter plots built directly into the
      results page. Choose up to 5 metrics per chart with a year range filter.
    </div>
  </div>
  <div class="feature-card">
    <div class="feature-icon">⬇</div>
    <div class="feature-title">Export-ready</div>
    <div class="feature-desc">
      Download your filtered dataset as CSV with a single click.
      Metadata export included — units, sources, and coverage statistics.
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

# ── CTA button (Streamlit native for navigation) ──────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
col1, col2, col3 = st.columns([2, 1, 2])
with col2:
    st.markdown("""
    <style>
    div[data-testid="stButton"] > button {
        background: #63b3ed;
        color: #0a0e1a;
        font-family: 'DM Sans', sans-serif;
        font-size: 15px;
        font-weight: 500;
        padding: 14px 40px;
        border-radius: 3px;
        border: none;
        width: 100%;
        cursor: pointer;
        letter-spacing: 0.3px;
        transition: background 0.2s;
    }
    div[data-testid="stButton"] > button:hover {
        background: #90cdf4;
        color: #0a0e1a;
        border: none;
    }
    </style>
    """, unsafe_allow_html=True)
    if st.button("Explore Data →", use_container_width=True):
        st.switch_page("pages/1_Query.py")

st.markdown("<br><br>", unsafe_allow_html=True)
