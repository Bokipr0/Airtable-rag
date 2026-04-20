"""
Flavor Intelligence Dashboard — Streamlit Edition
====================================================
Run:  streamlit run streamlit_dashboard.py
Requires: pip install streamlit plotly pandas
"""

import json
import sqlite3
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

ROOT = Path(__file__).parent
DB_PATH = ROOT / "layer_e_store.db"
LOG_PATH = ROOT / "pipeline_run.log"
STATE_PATH = ROOT / "cursor_state.json"
OUTPUT_PATH = ROOT / "pipeline_output.json"

# Relevance tier thresholds
TIER_HIGH = 0.80    # ≥80% → Very Relevant
TIER_MID  = 0.60    # 60-80% → Mid Relevance
                     # <60% → Little Relevance

TIER_COLORS = {
    "🟢 Very Relevant": "#2ecc71",
    "🟡 Mid Relevance": "#f39c12",
    "🔴 Little Relevance": "#e74c3c",
}

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Flavor Intelligence Dashboard",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# CUSTOM CSS
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
    .stMetric .stMetricValue { font-size: 2rem; }
    .tier-high { color: #2ecc71; font-weight: 700; }
    .tier-mid  { color: #f39c12; font-weight: 700; }
    .tier-low  { color: #e74c3c; font-weight: 700; }
    div[data-testid="stSidebar"] { background: #0f1117; }
    .source-card {
        background: #1a1d27;
        border: 1px solid #2d3350;
        border-radius: 10px;
        padding: 16px;
        margin-bottom: 12px;
    }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def get_db():
    """Connect to Layer E SQLite store. Returns None if DB doesn't exist."""
    if not DB_PATH.exists():
        return None
    return sqlite3.connect(DB_PATH, timeout=10)


def relevance_tier(score: float) -> str:
    """Classify a relevance_score into a human-readable tier."""
    if score >= TIER_HIGH:
        return "🟢 Very Relevant"
    elif score >= TIER_MID:
        return "🟡 Mid Relevance"
    else:
        return "🔴 Little Relevance"


def tier_css(tier_label: str) -> str:
    if "Very" in tier_label:
        return "tier-high"
    elif "Mid" in tier_label:
        return "tier-mid"
    return "tier-low"


def load_sources_df():
    """Load sources from Layer E into a DataFrame with relevance tiers."""
    conn = get_db()
    if conn is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM sources ORDER BY composite_score DESC", conn
        )
        if df.empty:
            return df
        df["relevance_tier"] = df["relevance_score"].apply(relevance_tier)
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def load_molecules_df():
    conn = get_db()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            "SELECT * FROM molecules ORDER BY confidence DESC", conn
        )
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def load_claims_df():
    conn = get_db()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            "SELECT * FROM claims ORDER BY confidence DESC", conn
        )
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def load_review_queue():
    conn = get_db()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query("""
            SELECT rq.*, s.url, s.year, s.venue, s.authors, s.relevance_score
            FROM review_queue rq
            LEFT JOIN sources s ON s.source_uid = rq.record_uid
            WHERE rq.decision = 'pending'
            ORDER BY rq.composite_score DESC
        """, conn)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def load_run_history():
    conn = get_db()
    if conn is None:
        return pd.DataFrame()
    try:
        return pd.read_sql_query(
            "SELECT * FROM run_log ORDER BY id DESC LIMIT 20", conn
        )
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def load_stats():
    conn = get_db()
    if conn is None:
        return {}
    try:
        cur = conn.cursor()
        stats = {}
        stats["total_sources"] = cur.execute("SELECT COUNT(*) FROM sources").fetchone()[0]
        stats["total_molecules"] = cur.execute("SELECT COUNT(*) FROM molecules").fetchone()[0]
        stats["total_claims"] = cur.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
        stats["review_pending"] = cur.execute(
            "SELECT COUNT(*) FROM review_queue WHERE decision='pending'"
        ).fetchone()[0]
        stats["promoted"] = cur.execute(
            "SELECT COUNT(*) FROM sources WHERE promoted_at IS NOT NULL"
        ).fetchone()[0]
        stats["auto_promoted"] = cur.execute(
            "SELECT COUNT(*) FROM sources WHERE review_status='auto_promoted'"
        ).fetchone()[0]
        stats["held"] = cur.execute(
            "SELECT COUNT(*) FROM sources WHERE review_status='held'"
        ).fetchone()[0]

        # Relevance tier counts
        stats["tier_high"] = cur.execute(
            "SELECT COUNT(*) FROM sources WHERE relevance_score >= ?", (TIER_HIGH,)
        ).fetchone()[0]
        stats["tier_mid"] = cur.execute(
            "SELECT COUNT(*) FROM sources WHERE relevance_score >= ? AND relevance_score < ?",
            (TIER_MID, TIER_HIGH)
        ).fetchone()[0]
        stats["tier_low"] = cur.execute(
            "SELECT COUNT(*) FROM sources WHERE relevance_score < ?", (TIER_MID,)
        ).fetchone()[0]

        return stats
    except Exception:
        return {}
    finally:
        conn.close()


def read_log(n=100):
    if not LOG_PATH.exists():
        return "No pipeline log yet."
    lines = LOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-n:])


def read_cursor_state():
    if not STATE_PATH.exists():
        return {}
    try:
        with STATE_PATH.open() as f:
            s = json.load(f)
        s["seen_dois_count"] = len(s.get("seen_dois", []))
        s.pop("seen_dois", None)
        return s
    except Exception:
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/test-tube.png", width=48)
    st.title("🧪 Flavor Intel")
    st.caption("GFI — Flavor & Aroma Initiative")
    st.divider()

    # Quick stats
    stats = load_stats()
    if stats:
        st.metric("Total Sources", stats.get("total_sources", 0))
        st.metric("Total Molecules", stats.get("total_molecules", 0))
        st.metric("Review Queue", stats.get("review_pending", 0))

        st.divider()
        st.subheader("Relevance Tiers")
        col1, col2, col3 = st.columns(3)
        col1.markdown(f"<span class='tier-high'>{stats.get('tier_high', 0)}</span><br>Very",
                     unsafe_allow_html=True)
        col2.markdown(f"<span class='tier-mid'>{stats.get('tier_mid', 0)}</span><br>Mid",
                     unsafe_allow_html=True)
        col3.markdown(f"<span class='tier-low'>{stats.get('tier_low', 0)}</span><br>Low",
                     unsafe_allow_html=True)
    else:
        st.info("No Layer E database found yet. Run the pipeline first.")

    st.divider()
    if st.button("🔄 Refresh Data"):
        st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# MAIN TABS
# ─────────────────────────────────────────────────────────────────────────────

tab_overview, tab_sources, tab_molecules, tab_review, tab_runs, tab_pipeline = st.tabs([
    "📊 Overview", "📚 Sources & Relevance", "🧬 Molecules",
    "✅ Review Queue", "📈 Run History", "⚙️ Pipeline"
])


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════

with tab_overview:
    st.header("Dashboard Overview")

    if not stats:
        st.warning("Layer E database not found. Run the pipeline to populate data.")
        st.code("python pipeline.py --mock --dry-run", language="bash")
        st.stop()

    # KPI cards
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("📚 Sources", stats["total_sources"])
    c2.metric("🧬 Molecules", stats["total_molecules"])
    c3.metric("📋 Claims", stats["total_claims"])
    c4.metric("⬆️ Promoted", stats["auto_promoted"])
    c5.metric("⏳ Held", stats["held"])

    st.divider()

    # Relevance tier distribution
    col_chart, col_branch = st.columns(2)

    with col_chart:
        st.subheader("Source Relevance Distribution")
        tier_data = pd.DataFrame({
            "Tier": ["🟢 Very Relevant", "🟡 Mid Relevance", "🔴 Little Relevance"],
            "Count": [stats.get("tier_high", 0), stats.get("tier_mid", 0), stats.get("tier_low", 0)],
        })
        fig = px.pie(
            tier_data, values="Count", names="Tier",
            color="Tier",
            color_discrete_map=TIER_COLORS,
            hole=0.4,
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#e0e4f0",
            showlegend=True,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_branch:
        st.subheader("Sources by Branch")
        df = load_sources_df()
        if not df.empty:
            branch_counts = df["branch"].value_counts().reset_index()
            branch_counts.columns = ["Branch", "Count"]
            fig = px.bar(
                branch_counts, x="Count", y="Branch",
                orientation="h",
                color="Count",
                color_continuous_scale="Viridis",
            )
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#e0e4f0",
                yaxis=dict(autorange="reversed"),
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True)

    # Score distribution histogram
    if not df.empty:
        st.subheader("Composite Score Distribution")
        fig = px.histogram(
            df, x="composite_score", nbins=30,
            color="relevance_tier",
            color_discrete_map=TIER_COLORS,
            labels={"composite_score": "Composite Score", "relevance_tier": "Tier"},
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#e0e4f0",
            barmode="overlay",
        )
        fig.update_traces(opacity=0.75)
        st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — SOURCES & RELEVANCE FILTRATION
# ═══════════════════════════════════════════════════════════════════════════════

with tab_sources:
    st.header("📚 Sources — Relevance Filtration")
    st.caption(
        "Sources are classified into **3 relevance tiers** after pipeline processing: "
        "**Very Relevant** (≥80%), **Mid Relevance** (60–80%), **Little Relevance** (<60%)"
    )

    df = load_sources_df()
    if df.empty:
        st.info("No sources in the database yet.")
    else:
        # Filters
        filter_col1, filter_col2, filter_col3 = st.columns(3)

        with filter_col1:
            tier_filter = st.multiselect(
                "Filter by Relevance Tier",
                options=["🟢 Very Relevant", "🟡 Mid Relevance", "🔴 Little Relevance"],
                default=["🟢 Very Relevant", "🟡 Mid Relevance", "🔴 Little Relevance"],
            )

        with filter_col2:
            branches = sorted(df["branch"].dropna().unique().tolist())
            branch_filter = st.multiselect("Filter by Branch", branches, default=branches)

        with filter_col3:
            db_sources = sorted(df["db_source"].dropna().unique().tolist())
            db_filter = st.multiselect("Filter by Database", db_sources, default=db_sources)

        # Apply filters
        mask = (
            df["relevance_tier"].isin(tier_filter)
            & df["branch"].isin(branch_filter)
            & df["db_source"].isin(db_filter)
        )
        filtered = df[mask].copy()

        # Summary row
        sc1, sc2, sc3, sc4 = st.columns(4)
        sc1.metric("Showing", f"{len(filtered)} / {len(df)}")
        sc2.metric("Avg Relevance", f"{filtered['relevance_score'].mean():.1%}" if not filtered.empty else "—")
        sc3.metric("Avg Composite", f"{filtered['composite_score'].mean():.3f}" if not filtered.empty else "—")
        sc4.metric("Unique Branches", filtered["branch"].nunique() if not filtered.empty else 0)

        st.divider()

        # Display table
        display_cols = [
            "title", "year", "branch", "db_source",
            "relevance_score", "composite_score", "relevance_tier",
            "review_status", "venue", "url"
        ]
        available_cols = [c for c in display_cols if c in filtered.columns]

        st.dataframe(
            filtered[available_cols].rename(columns={
                "title": "Title", "year": "Year", "branch": "Branch",
                "db_source": "Database", "relevance_score": "Relevance",
                "composite_score": "Composite", "relevance_tier": "Tier",
                "review_status": "Status", "venue": "Venue", "url": "URL",
            }),
            use_container_width=True,
            height=500,
            column_config={
                "Relevance": st.column_config.ProgressColumn(
                    min_value=0, max_value=1, format="%.0%%"
                ),
                "Composite": st.column_config.ProgressColumn(
                    min_value=0, max_value=1, format="%.3f"
                ),
                "URL": st.column_config.LinkColumn("URL"),
            }
        )

        # Tier breakdown chart
        st.subheader("Tier Breakdown by Branch")
        tier_branch = filtered.groupby(["branch", "relevance_tier"]).size().reset_index(name="count")
        fig = px.bar(
            tier_branch, x="branch", y="count", color="relevance_tier",
            color_discrete_map=TIER_COLORS,
            barmode="group",
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font_color="#e0e4f0",
            xaxis_title="Branch",
            yaxis_title="Count",
        )
        st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — MOLECULES
# ═══════════════════════════════════════════════════════════════════════════════

with tab_molecules:
    st.header("🧬 Molecules")

    mol_df = load_molecules_df()
    if mol_df.empty:
        st.info("No molecules extracted yet.")
    else:
        mc1, mc2, mc3 = st.columns(3)
        mc1.metric("Total Molecules", len(mol_df))
        mc2.metric("Avg Confidence", f"{mol_df['confidence'].mean():.2f}")
        mc3.metric("Unique Types", mol_df["type"].nunique())

        # Type distribution
        col_type, col_role = st.columns(2)
        with col_type:
            st.subheader("By Chemical Type")
            type_counts = mol_df["type"].value_counts().reset_index()
            type_counts.columns = ["Type", "Count"]
            fig = px.bar(type_counts, x="Type", y="Count", color="Type")
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#e0e4f0",
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_role:
            st.subheader("By Role")
            role_counts = mol_df["role"].value_counts().reset_index()
            role_counts.columns = ["Role", "Count"]
            fig = px.pie(role_counts, values="Count", names="Role", hole=0.35)
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#e0e4f0",
            )
            st.plotly_chart(fig, use_container_width=True)

        # Molecule table
        st.subheader("All Molecules")
        mol_display = ["name", "type", "role", "sensory", "confidence", "evidence_span"]
        available = [c for c in mol_display if c in mol_df.columns]
        st.dataframe(
            mol_df[available].rename(columns={
                "name": "Compound", "type": "Type", "role": "Role",
                "sensory": "Sensory", "confidence": "Confidence",
                "evidence_span": "Evidence",
            }),
            use_container_width=True,
            height=400,
            column_config={
                "Confidence": st.column_config.ProgressColumn(
                    min_value=0, max_value=1, format="%.2f"
                ),
            }
        )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — REVIEW QUEUE
# ═══════════════════════════════════════════════════════════════════════════════

with tab_review:
    st.header("✅ Review Queue")
    st.caption("Sources in the 0.50–0.70 composite score range need human review.")

    rq_df = load_review_queue()
    if rq_df.empty:
        st.success("No items pending review!")
    else:
        st.info(f"{len(rq_df)} items awaiting review")

        for idx, row in rq_df.iterrows():
            with st.expander(
                f"**{row.get('title_preview', 'Untitled')}** — "
                f"Score: {row.get('composite_score', 0):.3f} | "
                f"Branch: {row.get('branch', '?')}",
                expanded=False
            ):
                rc1, rc2, rc3 = st.columns(3)
                rc1.write(f"**Database:** {row.get('db_source', '?')}")
                rc2.write(f"**Year:** {row.get('year', '?')}")
                rc3.write(f"**Venue:** {row.get('venue', '?')}")

                if row.get("url"):
                    st.markdown(f"[Open Paper]({row['url']})")

                bc1, bc2 = st.columns(2)
                if bc1.button("✅ Approve", key=f"approve_{row['record_uid']}"):
                    conn = get_db()
                    if conn:
                        conn.execute(
                            "UPDATE review_queue SET decision='approved', decided_at=? WHERE record_uid=? AND decision='pending'",
                            (datetime.utcnow().isoformat(), row["record_uid"])
                        )
                        conn.execute(
                            "UPDATE sources SET review_status='approved' WHERE source_uid=?",
                            (row["record_uid"],)
                        )
                        conn.commit()
                        conn.close()
                        st.success("Approved!")
                        st.rerun()

                if bc2.button("❌ Reject", key=f"reject_{row['record_uid']}"):
                    conn = get_db()
                    if conn:
                        conn.execute(
                            "UPDATE review_queue SET decision='rejected', decided_at=? WHERE record_uid=? AND decision='pending'",
                            (datetime.utcnow().isoformat(), row["record_uid"])
                        )
                        conn.execute(
                            "UPDATE sources SET review_status='rejected' WHERE source_uid=?",
                            (row["record_uid"],)
                        )
                        conn.commit()
                        conn.close()
                        st.warning("Rejected.")
                        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 — RUN HISTORY
# ═══════════════════════════════════════════════════════════════════════════════

with tab_runs:
    st.header("📈 Run History")

    runs_df = load_run_history()
    if runs_df.empty:
        st.info("No pipeline runs recorded yet.")
    else:
        st.dataframe(
            runs_df.rename(columns={
                "run_at": "Time", "sources_fetched": "Fetched",
                "sources_extracted": "Extracted", "cache_hits": "Cache Hits",
                "promoted": "Promoted", "queued_for_review": "Queued",
                "held": "Held", "run_notes": "Notes",
            }),
            use_container_width=True,
            height=400,
        )

        # Run trend chart
        if len(runs_df) > 1:
            st.subheader("Extraction Trend")
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=runs_df["run_at"], y=runs_df["sources_fetched"],
                name="Fetched", mode="lines+markers",
            ))
            fig.add_trace(go.Scatter(
                x=runs_df["run_at"], y=runs_df["promoted"],
                name="Promoted", mode="lines+markers",
            ))
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                font_color="#e0e4f0",
            )
            st.plotly_chart(fig, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 6 — PIPELINE CONTROLS
# ═══════════════════════════════════════════════════════════════════════════════

with tab_pipeline:
    st.header("⚙️ Pipeline Controls")

    pc1, pc2 = st.columns(2)

    with pc1:
        st.subheader("Pipeline State")
        cursor_state = read_cursor_state()
        if cursor_state:
            st.json(cursor_state)
        else:
            st.info("No cursor state file found.")

    with pc2:
        st.subheader("Quick Actions")

        if st.button("🚀 Run Pipeline (Mock Mode)", type="primary"):
            st.info("Starting mock pipeline run...")
            try:
                result = subprocess.run(
                    ["python", "pipeline.py", "--mock", "--dry-run"],
                    capture_output=True, text=True, timeout=300,
                    cwd=str(ROOT),
                )
                if result.returncode == 0:
                    st.success("Pipeline mock run completed!")
                else:
                    st.error(f"Pipeline error:\n{result.stderr[:500]}")
            except subprocess.TimeoutExpired:
                st.warning("Pipeline timed out after 5 minutes.")
            except FileNotFoundError:
                st.error("Python not found. Make sure pipeline.py is in the same directory.")

        if st.button("📋 View Pipeline Log"):
            log_text = read_log(150)
            st.code(log_text, language="log")

    # Layer integration status
    st.divider()
    st.subheader("Integration Status")

    status_data = {
        "Component": [
            "Layer A — Query Orchestration",
            "Layer C — Typed Extraction (PydanticAI)",
            "Layer E — SQLite Knowledge Store",
            "Relevance Filtration (3-tier)",
            "Pipeline ↔ Layer C Integration",
            "Pipeline ↔ Layer E Integration",
        ],
        "Status": [
            "✅ Built", "✅ Built", "✅ Built",
            "✅ Active", "✅ Wired", "✅ Wired",
        ],
        "File": [
            "layer_a_query_orchestration.py",
            "layer_c_schemas.py",
            "layer_e_store.py",
            "streamlit_dashboard.py",
            "pipeline.py (USE_LAYER_C=1)",
            "pipeline.py (USE_LAYER_E=1)",
        ],
    }
    st.dataframe(pd.DataFrame(status_data), use_container_width=True, hide_index=True)
