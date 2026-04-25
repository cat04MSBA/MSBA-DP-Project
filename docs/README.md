# Docs

Visual documentation for the project. Open the HTML files in any browser — no server required.

---

## Files

| File | Purpose |
|---|---|
| `architecture.html` | Interactive architecture explorer · click any card to expand its internals in place |

---

## Architecture explorer

Open [`architecture.html`](architecture.html) in any browser. The page renders entirely client-side (no build step, no dependencies beyond a CDN font import).

### Layout

The diagram reads left to right, with three vertical layers:

- **Top:** orchestration bar (Prefect flows · cron + manual triggers · controls Ingestion through Coverage)
- **Middle:** pipeline (Sources → Ingestion → QC ➀ → Transform → QC ➁ → Coverage → App)
- **Bottom:** storage zones (Bronze under Ingestion · Silver under Transform · Gold under Coverage · Streamlit under App)

Quality checks sit inline as gate cards between processing stages. Audit and alerting are integrated into each step's expansion (look for the 🟣 `→ logs` and 🔴 `📧 email` pills) — they're not standalone cards because they happen *during* every step, not after.

### The three storage tiers

- **🥉 Bronze** — raw, immutable JSON files on Backblaze B2. Preserves exactly what each source returned.
- **🥈 Silver** — cleaned, standardized observations on Supabase Postgres. The queryable canonical layer.
- **🥇 Gold** — usage-driven materialized views on Supabase. Built from popular query patterns surfaced in `ops.query_log`. The Streamlit app routes to these views transparently when a matching pattern exists, returning sub-100ms responses instead of scanning the 3M-row silver table.

### Interactivity

- Click any card to expand its internals in place. The grid grows to accommodate; surrounding cards adjust.
- Click again to collapse.
- "Expand all" / "Collapse all" buttons in the controls bar at the top.
- Each expanded section tells the story of that stage: numbered steps, what's logged where (with `→ ops.checkpoints` style pills), what triggers an email (with 📧 pills), what's a routine outcome (✓ pills).

### For presentations

1. **Open in fullscreen** in your browser (`F11` on most platforms, `⌃⌘F` on macOS).
2. **Start with everything collapsed** — the high-level flow is your opening slide.
3. **Walk through the pipeline left to right.** When the audience asks about a specific stage, click that card; it expands without losing the surrounding context.
4. **Closing slide:** click "Expand all" to reveal the full architecture at once. This is the "and here's everything" moment.
5. **Print-to-PDF** works cleanly if you want a static handout — the CSS includes a `@media print` block that renders all expanded content as stacked pages.

### Design notes

- **Color palette:** purple (orchestration), bronze (B2 storage), green (Supabase silver), yellow (gold materialized views), amber (quality gates), pink (logging pills), red (email pills), blue (app/Streamlit). Cold-to-warm: data arrives external (gray), warms through processing, cools into storage, brightens at the user-facing app.
- **Typography:** DM Serif Display for headings, DM Sans for body, JetBrains Mono for code references — same fonts as the Streamlit app for visual consistency.
- **Responsive:** scales down to ~1200px wide gracefully; tighter than that, the inline arrows hide but cards still wrap correctly.

### Editing

To add a new card or layer, edit `architecture.html` directly. Each card is a `<div class="card">` placed in the CSS Grid via `c-<col>` and `r-<row>` classes. The expanded content uses simple step blocks with optional pill metadata. No build step — just refresh the browser.

---

## Other documentation

For deeper docs on specific subsystems, see the per-folder READMEs:

- [`../README.md`](../README.md) — project overview, quick start, full data pipeline lifecycle
- [`../app/README.md`](../app/README.md) — Streamlit app pages, caching, customization
- [`../database/README.md`](../database/README.md) — schema overview, ops scripts, audit subsystem
- [`../ingestion/README.md`](../ingestion/README.md) — how to add a new ingestion source
- [`../transformation/README.md`](../transformation/README.md) — how to add a new transformer
- [`../orchestration/README.md`](../orchestration/README.md) — Prefect flows and scheduling
