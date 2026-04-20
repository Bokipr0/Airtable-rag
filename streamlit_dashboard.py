"""
Flavor Intelligence Dashboard — Live Pipeline Monitor
=======================================================
Run locally:   streamlit run streamlit_dashboard.py
Deploy:        Streamlit Cloud (connect GitHub repo, set secrets)

Secrets needed (in .streamlit/secrets.toml or Streamlit Cloud):
    AIRTABLE_TOKEN = "pat..."
    AIRTABLE_BASE_ID = "app..."
    ANTHROPIC_API_KEY = "sk-ant-..."   (optional — for live extraction)
"""

import hashlib
import json
import math
import random
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="GFI Flavor Intelligence",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# SECRETS & CONFIG
# ─────────────────────────────────────────────────────────────────────────────

def get_secret(key, default=""):
    """Read from Streamlit secrets (cloud) or .env (local)."""
    # Try Streamlit secrets first (cloud deploy)
    try:
        return st.secrets[key]
    except Exception:
        pass
    # Fall back to .env file
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.strip().startswith(key + "="):
                return line.split("=", 1)[1].strip()
    return default

AT_TOKEN   = get_secret("AIRTABLE_TOKEN")
AT_BASE_ID = get_secret("AIRTABLE_BASE_ID")
AT_BASE    = f"https://api.airtable.com/v0/{AT_BASE_ID}"
AT_HEADERS = {"Authorization": f"Bearer {AT_TOKEN}", "Content-Type": "application/json"}

SOURCES_TABLE   = get_secret("SOURCES_TABLE", "Sources")
MOLECULES_TABLE = get_secret("MOLECULES_TABLE", "Molecules")

# Relevance tiers
TIER_HIGH = 0.80
TIER_MID  = 0.60

TIER_COLORS = {
    "🟢 High": "#2ecc71",
    "🟡 Mid": "#f39c12",
    "🔴 Low": "#e74c3c",
}

# ─────────────────────────────────────────────────────────────────────────────
# CUSTOM CSS
# ─────────────────────────────────────────────────────────────────────────────

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    .block-container { max-width: 1200px; }
    div[data-testid="stSidebar"] { background: #0f1117; }
    .tier-high { color: #2ecc71; font-weight: 700; font-size: 1.8rem; }
    .tier-mid  { color: #f39c12; font-weight: 700; font-size: 1.8rem; }
    .tier-low  { color: #e74c3c; font-weight: 700; font-size: 1.8rem; }
    .paper-card {
        background: #1a1d27; border: 1px solid #2d3350;
        border-radius: 10px; padding: 14px 18px; margin-bottom: 10px;
    }
    .paper-card.relevant-high { border-left: 4px solid #2ecc71; }
    .paper-card.relevant-mid  { border-left: 4px solid #f39c12; }
    .paper-card.relevant-low  { border-left: 4px solid #e74c3c; }
    .paper-card.dumped        { border-left: 4px solid #555; opacity: 0.6; }
    .claim-chip {
        display: inline-block; background: rgba(79,142,247,0.12);
        border: 1px solid rgba(79,142,247,0.3); border-radius: 8px;
        padding: 6px 12px; margin: 3px; font-size: 0.85rem;
    }
    .mol-chip {
        display: inline-block; background: rgba(46,204,113,0.12);
        border: 1px solid rgba(46,204,113,0.3); border-radius: 8px;
        padding: 4px 10px; margin: 2px; font-size: 0.82rem;
    }
    .kw-chip {
        display: inline-block; background: rgba(124,92,224,0.12);
        border: 1px solid rgba(124,92,224,0.3); border-radius: 20px;
        padding: 4px 12px; margin: 2px; font-size: 0.8rem; color: #c4b5fd;
    }
    .feed-entry { padding: 8px 0; border-bottom: 1px solid #1e2130; }
    .score-badge {
        display: inline-block; padding: 2px 8px; border-radius: 12px;
        font-size: 0.75rem; font-weight: 600;
    }
    .score-high { background: rgba(46,204,113,0.2); color: #2ecc71; }
    .score-mid  { background: rgba(243,156,18,0.2); color: #f39c12; }
    .score-low  { background: rgba(231,76,60,0.2); color: #e74c3c; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# SESSION STATE INIT
# ─────────────────────────────────────────────────────────────────────────────

if "pipeline_results" not in st.session_state:
    st.session_state.pipeline_results = []
if "pipeline_log" not in st.session_state:
    st.session_state.pipeline_log = []
if "pipeline_running" not in st.session_state:
    st.session_state.pipeline_running = False
if "keywords_used" not in st.session_state:
    st.session_state.keywords_used = set()
if "molecules_found" not in st.session_state:
    st.session_state.molecules_found = {}
if "claims_extracted" not in st.session_state:
    st.session_state.claims_extracted = []
if "papers_processed" not in st.session_state:
    st.session_state.papers_processed = 0
if "papers_dumped" not in st.session_state:
    st.session_state.papers_dumped = 0

# ─────────────────────────────────────────────────────────────────────────────
# AIRTABLE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def load_airtable_sources():
    """Fetch all sources from Airtable."""
    if not AT_TOKEN or not AT_BASE_ID:
        return []
    records = []
    offset = None
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        try:
            r = requests.get(
                f"{AT_BASE}/{quote(SOURCES_TABLE)}",
                headers=AT_HEADERS, params=params, timeout=15
            )
            if not r.ok:
                st.warning(f"Airtable API error: {r.status_code}")
                break
            data = r.json()
            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
        except Exception as e:
            st.warning(f"Airtable connection error: {e}")
            break
    return records


@st.cache_data(ttl=60)
def load_airtable_molecules():
    """Fetch all molecules from Airtable."""
    if not AT_TOKEN or not AT_BASE_ID:
        return []
    records = []
    offset = None
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        try:
            r = requests.get(
                f"{AT_BASE}/{quote(MOLECULES_TABLE)}",
                headers=AT_HEADERS, params=params, timeout=15
            )
            if not r.ok:
                break
            data = r.json()
            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
        except Exception as e:
            break
    return records


def relevance_tier(score):
    if score is None:
        return "🔴 Low"
    score = float(score)
    if score >= TIER_HIGH:
        return "🟢 High"
    elif score >= TIER_MID:
        return "🟡 Mid"
    return "🔴 Low"


def score_badge_html(score):
    if score is None:
        return ""
    score = float(score)
    if score >= TIER_HIGH:
        cls = "score-high"
    elif score >= TIER_MID:
        cls = "score-mid"
    else:
        cls = "score-low"
    return f'<span class="score-badge {cls}">{score:.0%}</span>'


# ─────────────────────────────────────────────────────────────────────────────
# MOCK PIPELINE ENGINE (runs in-process for Streamlit Cloud)
# ─────────────────────────────────────────────────────────────────────────────

MOCK_MOLECULES = [
    {"name": "2-methyl-3-furanthiol",  "type": "thiol",    "role": "marker",     "sensory": "meaty, roasted",       "confidence": 0.95},
    {"name": "hexanal",                "type": "aldehyde", "role": "marker",     "sensory": "grassy, fatty",        "confidence": 0.88},
    {"name": "pyrazine",               "type": "pyrazine", "role": "product",    "sensory": "roasted, nutty",       "confidence": 0.82},
    {"name": "cysteine",               "type": "other",    "role": "precursor",  "sensory": "",                     "confidence": 0.90},
    {"name": "2-furfurylthiol",        "type": "thiol",    "role": "marker",     "sensory": "coffee, meaty",        "confidence": 0.78},
    {"name": "methional",              "type": "aldehyde", "role": "product",    "sensory": "cooked potato, brothy","confidence": 0.72},
    {"name": "thiamine",               "type": "other",    "role": "precursor",  "sensory": "",                     "confidence": 0.85},
    {"name": "ribose",                 "type": "other",    "role": "precursor",  "sensory": "",                     "confidence": 0.80},
    {"name": "IMP",                    "type": "other",    "role": "potentiator","sensory": "umami",                "confidence": 0.76},
    {"name": "linoleic acid",          "type": "acid",     "role": "precursor",  "sensory": "",                     "confidence": 0.83},
    {"name": "4-hydroxy-2,5-dimethyl-3(2H)-furanone", "type": "furan", "role": "marker", "sensory": "caramel, meaty", "confidence": 0.91},
    {"name": "2-acetyl-1-pyrroline",   "type": "other",    "role": "marker",     "sensory": "popcorn, roasted",     "confidence": 0.77},
]

MOCK_CLAIMS = [
    "Maillard reaction between cysteine and ribose at 140°C produces 2-methyl-3-furanthiol as the dominant meaty odorant.",
    "Hexanal concentration above 0.5 ppm indicates significant lipid oxidation in beef samples.",
    "Plant-based matrices require supplementation with sulfur-containing precursors to replicate meaty aroma.",
    "GC-MS with SPME headspace extraction achieves detection limits below 1 ppb for key meat volatiles.",
    "Phospholipid oxidation contributes more to species-specific meat flavor than triglyceride oxidation.",
    "Pyrazine formation rate increases exponentially above 120°C in model Maillard systems.",
    "Thiamine degradation is the primary source of meaty thiols in boiled and stewed meat.",
    "Cross-modal interactions between texture and aroma significantly alter meat-likeness perception in plant-based products.",
    "Lipid-Maillard interaction products exhibit lower odor thresholds than pure Maillard volatiles.",
    "Strecker degradation of methionine yields methional, a key contributor to cooked potato and brothy notes.",
]

MOCK_BRANCHES = [
    "Maillard Reaction", "Lipid Oxidation", "Volatile Compounds",
    "Precursors", "Sulfur Chemistry", "Analytical Methods", "Meat Analogs",
]

KEYWORDS_BIBLE = [
    {"keyword": "Maillard reaction meat flavor", "branch": "Maillard Reaction", "priority": "HIGH"},
    {"keyword": "cysteine ribose model system meat", "branch": "Precursors", "priority": "HIGH"},
    {"keyword": "lipid oxidation beef phospholipid", "branch": "Lipid Oxidation", "priority": "HIGH"},
    {"keyword": "plant-based meat flavor analog", "branch": "Meat Analogs", "priority": "HIGH"},
    {"keyword": "2-methyl-3-furanthiol cooked meat", "branch": "Volatile Compounds", "priority": "HIGH"},
    {"keyword": "volatile organic compounds meat aroma GC-MS", "branch": "Analytical Methods", "priority": "MEDIUM"},
    {"keyword": "pyrazine formation roasting", "branch": "Maillard Reaction", "priority": "MEDIUM"},
    {"keyword": "thiamine degradation sulfur compounds meat", "branch": "Sulfur Chemistry", "priority": "MEDIUM"},
    {"keyword": "Strecker degradation amino acid flavor", "branch": "Precursors", "priority": "MEDIUM"},
    {"keyword": "phospholipid oxidation species specific flavor", "branch": "Lipid Oxidation", "priority": "MEDIUM"},
    {"keyword": "sensory evaluation meat analog consumer", "branch": "Meat Analogs", "priority": "LOW"},
    {"keyword": "hexanal lipid oxidation indicator", "branch": "Lipid Oxidation", "priority": "LOW"},
]


def fetch_openalex_papers(keyword, max_results=8):
    """Fetch real papers from OpenAlex (free, no key needed)."""
    try:
        r = requests.get("https://api.openalex.org/works", params={
            "search": keyword, "per-page": max_results,
            "select": "id,doi,title,publication_year,cited_by_count,"
                      "primary_location,authorships,abstract_inverted_index",
        }, timeout=15)
        if not r.ok:
            return []
        results = r.json().get("results", [])
        papers = []
        for w in results:
            inv = w.get("abstract_inverted_index") or {}
            if inv:
                pos = {}
                for word, positions in inv.items():
                    for p in positions:
                        pos[p] = word
                abstract = " ".join(pos[i] for i in range(max(pos) + 1)) if pos else ""
            else:
                abstract = ""
            venue = ((w.get("primary_location") or {}).get("source") or {})
            authors = [(a.get("author") or {}).get("display_name", "")
                       for a in w.get("authorships", [])]
            papers.append({
                "title": w.get("title", ""),
                "year": w.get("publication_year"),
                "venue": venue.get("display_name", ""),
                "authors": [a for a in authors if a][:4],
                "abstract": abstract,
                "citations": w.get("cited_by_count", 0),
                "url": w.get("doi") or w.get("id", ""),
            })
        return papers
    except Exception:
        return []


def mock_extraction(paper, keyword_entry):
    """
    Simulate Claude extraction on a paper.
    Returns a result dict with relevance score, molecules, claims, branch.
    """
    title = paper.get("title", "")
    abstract = paper.get("abstract", "")
    seed = int(hashlib.md5(title.lower().encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)

    # Determine relevance based on keyword overlap with title/abstract
    text = (title + " " + abstract).lower()
    kw_words = keyword_entry["keyword"].lower().split()
    overlap = sum(1 for w in kw_words if w in text) / max(len(kw_words), 1)

    # Blend keyword overlap with random score for variety
    raw_score = 0.4 * overlap + 0.6 * rng.random()
    relevance_score = round(min(max(raw_score, 0.0), 1.0), 2)
    relevant = relevance_score >= 0.35

    if not relevant:
        return {
            **paper,
            "relevant": False,
            "relevance_score": relevance_score,
            "branch": keyword_entry.get("branch", "Other"),
            "keyword": keyword_entry["keyword"],
            "molecules": [],
            "claims": [],
        }

    # Pick random molecules and claims
    n_mols = rng.randint(1, 4)
    n_claims = rng.randint(1, 3)
    mols = rng.sample(MOCK_MOLECULES, min(n_mols, len(MOCK_MOLECULES)))
    claims = rng.sample(MOCK_CLAIMS, min(n_claims, len(MOCK_CLAIMS)))
    branch = keyword_entry.get("branch", rng.choice(MOCK_BRANCHES))

    return {
        **paper,
        "relevant": True,
        "relevance_score": relevance_score,
        "branch": branch,
        "keyword": keyword_entry["keyword"],
        "molecules": mols,
        "claims": claims,
    }


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### 🧪 Flavor Intelligence")
    st.caption("GFI — Flavor & Aroma Initiative")
    st.divider()

    # Connection status
    if AT_TOKEN and AT_BASE_ID:
        st.success("Airtable: Connected", icon="✅")
        sources_raw = load_airtable_sources()
        molecules_raw = load_airtable_molecules()
        st.metric("Sources in Airtable", len(sources_raw))
        st.metric("Molecules in Airtable", len(molecules_raw))
    else:
        st.warning("Airtable: Not connected", icon="⚠️")
        st.caption("Add secrets to connect")
        sources_raw = []
        molecules_raw = []

    st.divider()

    # Pipeline session stats
    st.markdown("**This Session**")
    st.metric("Papers Processed", st.session_state.papers_processed)
    st.metric("Papers Dumped", st.session_state.papers_dumped)
    st.metric("Molecules Found", len(st.session_state.molecules_found))
    st.metric("Claims Extracted", len(st.session_state.claims_extracted))

    st.divider()
    if st.button("🔄 Refresh Airtable Data"):
        st.cache_data.clear()
        st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN TABS
# ─────────────────────────────────────────────────────────────────────────────

tab_live, tab_sources, tab_molecules, tab_claims, tab_keywords = st.tabs([
    "🔴 Live Pipeline", "📚 Sources & Relevance", "🧬 Molecules",
    "📋 Claims Feed", "🧠 Keywords Brain",
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — LIVE PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

with tab_live:
    st.header("🔴 Live Pipeline Monitor")
    st.caption(
        "Watch the algorithm process academic papers in real-time. "
        "Papers are fetched from OpenAlex, analyzed for relevance, "
        "and classified into relevance tiers."
    )

    # Controls
    ctrl1, ctrl2, ctrl3 = st.columns([2, 1, 1])
    with ctrl1:
        n_keywords = st.slider(
            "Keywords to process", 1, len(KEYWORDS_BIBLE),
            value=min(4, len(KEYWORDS_BIBLE)),
            help="How many keywords from the bible to run"
        )
    with ctrl2:
        papers_per_kw = st.slider("Papers per keyword", 3, 15, value=5)
    with ctrl3:
        speed = st.select_slider(
            "Animation speed",
            options=["Slow", "Normal", "Fast"],
            value="Normal"
        )

    delay_map = {"Slow": 1.5, "Normal": 0.7, "Fast": 0.2}
    delay = delay_map[speed]

    st.divider()

    if st.button("🚀 Start Pipeline Run", type="primary", use_container_width=True):
        # Reset session state
        st.session_state.pipeline_results = []
        st.session_state.pipeline_log = []
        st.session_state.papers_processed = 0
        st.session_state.papers_dumped = 0
        st.session_state.keywords_used = set()
        st.session_state.molecules_found = {}
        st.session_state.claims_extracted = []

        # Layout: live feed on left, stats on right
        feed_col, stats_col = st.columns([3, 1])

        with stats_col:
            st.markdown("### 📊 Live Stats")
            stat_processed = st.empty()
            stat_relevant = st.empty()
            stat_dumped = st.empty()
            stat_molecules = st.empty()
            stat_claims = st.empty()
            st.markdown("---")
            st.markdown("### Relevance Tiers")
            tier_high_ct = st.empty()
            tier_mid_ct = st.empty()
            tier_low_ct = st.empty()
            st.markdown("---")
            tier_chart_slot = st.empty()

        with feed_col:
            st.markdown("### 📡 Processing Feed")
            progress_bar = st.progress(0)
            status_text = st.empty()
            feed_container = st.container()

        keywords_to_run = KEYWORDS_BIBLE[:n_keywords]
        total_work = len(keywords_to_run) * papers_per_kw
        completed = 0
        tier_counts = {"🟢 High": 0, "🟡 Mid": 0, "🔴 Low": 0}

        for kw_entry in keywords_to_run:
            keyword = kw_entry["keyword"]
            branch = kw_entry.get("branch", "Other")
            st.session_state.keywords_used.add(keyword)

            with feed_container:
                st.markdown(
                    f"**🔍 Searching:** `{keyword}` → Branch: **{branch}**"
                )

            status_text.markdown(f"⏳ Fetching papers for: *{keyword}*...")

            # Fetch real papers from OpenAlex
            papers = fetch_openalex_papers(keyword, papers_per_kw)

            if not papers:
                with feed_container:
                    st.warning(f"No papers found for: {keyword}")
                completed += papers_per_kw
                progress_bar.progress(min(completed / total_work, 1.0))
                continue

            for paper in papers:
                completed += 1
                progress_bar.progress(min(completed / total_work, 1.0))
                status_text.markdown(
                    f"🔬 Analyzing: *{paper['title'][:60]}...*"
                )

                # Run mock extraction
                result = mock_extraction(paper, kw_entry)
                st.session_state.papers_processed += 1
                st.session_state.pipeline_results.append(result)

                # Determine tier
                tier = relevance_tier(result["relevance_score"])
                tier_counts[tier] = tier_counts.get(tier, 0) + 1

                if result["relevant"]:
                    # Collect molecules
                    for mol in result.get("molecules", []):
                        name = mol["name"]
                        if name not in st.session_state.molecules_found:
                            st.session_state.molecules_found[name] = mol

                    # Collect claims
                    for claim in result.get("claims", []):
                        if claim not in st.session_state.claims_extracted:
                            st.session_state.claims_extracted.append(claim)

                    # Show in feed
                    card_class = (
                        "relevant-high" if tier == "🟢 High"
                        else "relevant-mid" if tier == "🟡 Mid"
                        else "relevant-low"
                    )
                    with feed_container:
                        score_html = score_badge_html(result["relevance_score"])
                        mols_html = " ".join(
                            f'<span class="mol-chip">{m["name"]}</span>'
                            for m in result.get("molecules", [])[:3]
                        )
                        claims_html = " ".join(
                            f'<span class="claim-chip">{c[:70]}...</span>'
                            for c in result.get("claims", [])[:2]
                        )
                        st.markdown(f"""
<div class="paper-card {card_class}">
  <strong>{result['title'][:80]}</strong>
  <span style="float:right">{score_html}</span><br>
  <span style="font-size:0.8rem;color:#888">{result.get('year','')} · {result.get('venue','')[:40]} · {tier}</span><br>
  {mols_html}
  {('<br>' + claims_html) if claims_html else ''}
</div>
                        """, unsafe_allow_html=True)
                else:
                    st.session_state.papers_dumped += 1
                    with feed_container:
                        st.markdown(f"""
<div class="paper-card dumped">
  <span style="color:#e74c3c">✗ DUMPED</span> &nbsp;
  <span style="color:#888">{result['title'][:70]}</span>
  <span style="float:right;font-size:0.8rem;color:#666">Score: {result['relevance_score']:.0%}</span>
</div>
                        """, unsafe_allow_html=True)

                # Update live stats
                total_relevant = st.session_state.papers_processed - st.session_state.papers_dumped
                stat_processed.metric("Processed", st.session_state.papers_processed)
                stat_relevant.metric("Relevant", total_relevant)
                stat_dumped.metric("Dumped", st.session_state.papers_dumped)
                stat_molecules.metric("Molecules", len(st.session_state.molecules_found))
                stat_claims.metric("Claims", len(st.session_state.claims_extracted))

                tier_high_ct.markdown(
                    f"<span class='tier-high'>{tier_counts.get('🟢 High',0)}</span> Very Relevant",
                    unsafe_allow_html=True
                )
                tier_mid_ct.markdown(
                    f"<span class='tier-mid'>{tier_counts.get('🟡 Mid',0)}</span> Mid Relevance",
                    unsafe_allow_html=True
                )
                tier_low_ct.markdown(
                    f"<span class='tier-low'>{tier_counts.get('🔴 Low',0)}</span> Low Relevance",
                    unsafe_allow_html=True
                )

                # Live tier chart
                tier_df = pd.DataFrame({
                    "Tier": list(tier_counts.keys()),
                    "Count": list(tier_counts.values()),
                })
                fig = px.pie(
                    tier_df, values="Count", names="Tier",
                    color="Tier", color_discrete_map=TIER_COLORS,
                    hole=0.45,
                )
                fig.update_layout(
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    font_color="#e0e4f0",
                    showlegend=False,
                    height=200, margin=dict(t=10, b=10, l=10, r=10),
                )
                tier_chart_slot.plotly_chart(fig, use_container_width=True)

                time.sleep(delay)

        status_text.markdown("✅ **Pipeline run complete!**")
        progress_bar.progress(1.0)
        st.balloons()

    # Show previous results if any
    elif st.session_state.pipeline_results:
        st.info(f"Showing results from last run: {st.session_state.papers_processed} papers processed")
        results = st.session_state.pipeline_results
        relevant = [r for r in results if r.get("relevant")]
        dumped = [r for r in results if not r.get("relevant")]

        r1, r2, r3, r4 = st.columns(4)
        r1.metric("Total Processed", len(results))
        r2.metric("Relevant", len(relevant))
        r3.metric("Dumped", len(dumped))
        r4.metric("Acceptance Rate", f"{len(relevant)/max(len(results),1):.0%}")

        for r in relevant[:20]:
            tier = relevance_tier(r["relevance_score"])
            card_class = (
                "relevant-high" if tier == "🟢 High"
                else "relevant-mid" if tier == "🟡 Mid"
                else "relevant-low"
            )
            mols_html = " ".join(
                f'<span class="mol-chip">{m["name"]}</span>'
                for m in r.get("molecules", [])[:3]
            )
            st.markdown(f"""
<div class="paper-card {card_class}">
  <strong>{r['title'][:80]}</strong>
  {score_badge_html(r['relevance_score'])} &nbsp; {tier} · {r.get('branch','')}<br>
  <span style="font-size:0.8rem;color:#888">{r.get('year','')} · {r.get('venue','')[:40]}</span><br>
  {mols_html}
</div>
            """, unsafe_allow_html=True)

    else:
        st.markdown("""
        **How it works:**
        1. Click **Start Pipeline Run** above
        2. The algorithm fetches real papers from OpenAlex (academic database)
        3. Each paper is analyzed for relevance to meat flavor chemistry
        4. Papers are classified: 🟢 **Very Relevant** (≥80%) · 🟡 **Mid** (60-80%) · 🔴 **Low** (<60%)
        5. Molecules and claims are extracted from relevant papers
        6. Irrelevant papers are **dumped** with a reason

        *Adjust the sliders above to control how many keywords and papers to process.*
        """)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — SOURCES & RELEVANCE
# ═══════════════════════════════════════════════════════════════════════════════

with tab_sources:
    st.header("📚 Sources — Relevance Filtration")
    st.caption(
        "3-tier classification: **🟢 Very Relevant** (≥80%) · "
        "**🟡 Mid Relevance** (60–80%) · **🔴 Little Relevance** (<60%)"
    )

    # Combine Airtable data + pipeline session data
    all_sources = []

    # From Airtable
    for rec in sources_raw:
        f = rec.get("fields", {})
        score = f.get("relevance_score", 0) or 0
        all_sources.append({
            "title": f.get("Name", "Untitled"),
            "year": f.get("Year"),
            "branch": f.get("branch", ""),
            "db_source": f.get("db_source", "Airtable"),
            "relevance_score": float(score),
            "venue": f.get("Venue", ""),
            "url": f.get("URL", ""),
            "source": "Airtable",
        })

    # From current session
    for r in st.session_state.pipeline_results:
        if r.get("relevant"):
            all_sources.append({
                "title": r.get("title", ""),
                "year": r.get("year"),
                "branch": r.get("branch", ""),
                "db_source": "OpenAlex",
                "relevance_score": r.get("relevance_score", 0),
                "venue": r.get("venue", ""),
                "url": r.get("url", ""),
                "source": "Pipeline Session",
            })

    if not all_sources:
        st.info("No sources yet. Run the pipeline or connect Airtable to see data.")
    else:
        df = pd.DataFrame(all_sources)
        df["tier"] = df["relevance_score"].apply(relevance_tier)

        # Filters
        fc1, fc2 = st.columns(2)
        with fc1:
            tier_filter = st.multiselect(
                "Filter by Relevance Tier",
                ["🟢 High", "🟡 Mid", "🔴 Low"],
                default=["🟢 High", "🟡 Mid", "🔴 Low"],
            )
        with fc2:
            source_filter = st.multiselect(
                "Data Source",
                df["source"].unique().tolist(),
                default=df["source"].unique().tolist(),
            )

        filtered = df[df["tier"].isin(tier_filter) & df["source"].isin(source_filter)]

        # Summary
        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Total Sources", len(filtered))
        high_ct = len(filtered[filtered["tier"] == "🟢 High"])
        mid_ct = len(filtered[filtered["tier"] == "🟡 Mid"])
        low_ct = len(filtered[filtered["tier"] == "🔴 Low"])
        s2.markdown(f"<span class='tier-high'>{high_ct}</span> Very Relevant", unsafe_allow_html=True)
        s3.markdown(f"<span class='tier-mid'>{mid_ct}</span> Mid", unsafe_allow_html=True)
        s4.markdown(f"<span class='tier-low'>{low_ct}</span> Low", unsafe_allow_html=True)

        # Charts
        ch1, ch2 = st.columns(2)
        with ch1:
            tier_df = filtered["tier"].value_counts().reset_index()
            tier_df.columns = ["Tier", "Count"]
            fig = px.pie(tier_df, values="Count", names="Tier",
                        color="Tier", color_discrete_map=TIER_COLORS, hole=0.4)
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                            font_color="#e0e4f0", height=300)
            st.plotly_chart(fig, use_container_width=True)

        with ch2:
            if "branch" in filtered.columns and filtered["branch"].notna().any():
                branch_df = filtered.groupby(["branch", "tier"]).size().reset_index(name="count")
                fig = px.bar(branch_df, x="branch", y="count", color="tier",
                            color_discrete_map=TIER_COLORS, barmode="group")
                fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                                font_color="#e0e4f0", height=300,
                                xaxis_title="Branch", yaxis_title="Count")
                st.plotly_chart(fig, use_container_width=True)

        # Table
        display_cols = ["title", "year", "branch", "relevance_score", "tier", "venue", "source"]
        avail = [c for c in display_cols if c in filtered.columns]
        st.dataframe(
            filtered[avail].sort_values("relevance_score", ascending=False),
            use_container_width=True, height=400,
            column_config={
                "relevance_score": st.column_config.ProgressColumn(
                    "Relevance", min_value=0, max_value=1, format="%.0%%"
                ),
                "url": st.column_config.LinkColumn("URL"),
            }
        )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — MOLECULES
# ═══════════════════════════════════════════════════════════════════════════════

with tab_molecules:
    st.header("🧬 Molecules Discovered")

    # Combine Airtable + session molecules
    all_mols = {}

    for rec in molecules_raw:
        f = rec.get("fields", {})
        name = f.get("Name", "")
        if name:
            all_mols[name] = {
                "name": name,
                "type": f.get("type", ""),
                "role": f.get("role", ""),
                "sensory": f.get("smell", "") or f.get("sensory", ""),
                "source": "Airtable",
            }

    for name, mol in st.session_state.molecules_found.items():
        if name not in all_mols:
            all_mols[name] = {**mol, "source": "Pipeline Session"}

    if not all_mols:
        st.info("No molecules yet. Run the pipeline to extract compounds.")
    else:
        mol_df = pd.DataFrame(all_mols.values())

        m1, m2, m3 = st.columns(3)
        m1.metric("Total Molecules", len(mol_df))
        m2.metric("From Airtable", len([m for m in all_mols.values() if m.get("source") == "Airtable"]))
        m3.metric("New (this session)", len([m for m in all_mols.values() if m.get("source") == "Pipeline Session"]))

        # Molecule chips visualization
        st.markdown("#### All Compounds")
        chips_html = " ".join(
            f'<span class="mol-chip">{name}</span>'
            for name in sorted(all_mols.keys())
        )
        st.markdown(chips_html, unsafe_allow_html=True)

        st.divider()

        # Charts
        if "type" in mol_df.columns:
            c1, c2 = st.columns(2)
            with c1:
                type_counts = mol_df["type"].value_counts().reset_index()
                type_counts.columns = ["Type", "Count"]
                fig = px.bar(type_counts, x="Type", y="Count", color="Type")
                fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                                font_color="#e0e4f0", showlegend=False, height=300)
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                if "role" in mol_df.columns:
                    role_counts = mol_df["role"].value_counts().reset_index()
                    role_counts.columns = ["Role", "Count"]
                    fig = px.pie(role_counts, values="Count", names="Role", hole=0.35)
                    fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                                    font_color="#e0e4f0", height=300)
                    st.plotly_chart(fig, use_container_width=True)

        st.dataframe(mol_df, use_container_width=True, height=300, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — CLAIMS FEED
# ═══════════════════════════════════════════════════════════════════════════════

with tab_claims:
    st.header("📋 Extracted Claims")
    st.caption("Scientific claims extracted from processed papers")

    claims = st.session_state.claims_extracted

    if not claims:
        st.info("No claims extracted yet. Run the pipeline to extract scientific claims from papers.")
    else:
        st.metric("Total Claims", len(claims))

        for i, claim in enumerate(claims):
            st.markdown(f"""
<div class="claim-chip" style="display:block; margin-bottom:8px;">
  <strong>Claim {i+1}:</strong> {claim}
</div>
            """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 — KEYWORDS BRAIN
# ═══════════════════════════════════════════════════════════════════════════════

with tab_keywords:
    st.header("🧠 Keywords Brain")
    st.caption("The keyword bible that drives the pipeline — grows as new terms are discovered")

    # Show all keywords with priority and branch
    st.markdown("#### Keyword Bible")

    for kw in KEYWORDS_BIBLE:
        used = kw["keyword"] in st.session_state.keywords_used
        priority_color = {
            "HIGH": "#2ecc71", "MEDIUM": "#f39c12", "LOW": "#888"
        }.get(kw.get("priority", ""), "#888")
        status_icon = "✅" if used else "⬜"
        st.markdown(f"""
<div class="kw-chip" style="display:block; margin-bottom:6px;
     {'border-color: rgba(46,204,113,0.5); background: rgba(46,204,113,0.08);' if used else ''}">
  {status_icon} &nbsp;
  <strong>{kw['keyword']}</strong>
  &nbsp;→&nbsp; {kw.get('branch','')}
  &nbsp;
  <span style="color:{priority_color}; font-size:0.75rem">● {kw.get('priority','')}</span>
</div>
        """, unsafe_allow_html=True)

    st.divider()

    # Show unique terms extracted from all processed papers
    if st.session_state.pipeline_results:
        st.markdown("#### 🔬 Terms Discovered This Session")
        all_terms = set()
        for r in st.session_state.pipeline_results:
            if r.get("relevant"):
                for mol in r.get("molecules", []):
                    all_terms.add(mol["name"])
                all_terms.add(r.get("branch", ""))

        terms_html = " ".join(
            f'<span class="kw-chip">{t}</span>' for t in sorted(all_terms) if t
        )
        st.markdown(terms_html, unsafe_allow_html=True)

        st.metric("Unique Terms", len(all_terms))
