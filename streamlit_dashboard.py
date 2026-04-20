"""
Flavor Intelligence Dashboard — Live Pipeline Monitor v2
==========================================================
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

# ---------------------------------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="GFI Flavor Intelligence",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# SECRETS & CONFIG
# ---------------------------------------------------------------------------

def get_secret(key, default=""):
    """Read from Streamlit secrets (cloud) or .env (local)."""
    try:
        return st.secrets[key]
    except Exception:
        pass
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

# Relevance tiers — 1st / 2nd / 3rd level
TIER_1ST = 0.80
TIER_2ND = 0.60

TIER_COLORS = {
    "1st Level — High Relevance": "#2ecc71",
    "2nd Level — Mid Relevance": "#f39c12",
    "3rd Level — Somewhat Relevant": "#e74c3c",
}

# ---------------------------------------------------------------------------
# CUSTOM CSS
# ---------------------------------------------------------------------------

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    .block-container { max-width: 1200px; }

    /* Tier labels */
    .tier-1st { color: #1a8a4a; font-weight: 700; font-size: 1.8rem; }
    .tier-2nd { color: #c47f0a; font-weight: 700; font-size: 1.8rem; }
    .tier-3rd { color: #c0392b; font-weight: 700; font-size: 1.8rem; }

    /* Paper cards */
    .paper-card {
        background: #f8f9fb; border: 1px solid #dde1e8;
        border-radius: 10px; padding: 14px 18px; margin-bottom: 10px;
    }
    .paper-card.relevant-1st { border-left: 4px solid #1a8a4a; }
    .paper-card.relevant-2nd { border-left: 4px solid #c47f0a; }
    .paper-card.relevant-3rd { border-left: 4px solid #c0392b; }
    .paper-card.dumped        { border-left: 4px solid #999; opacity: 0.6; }

    /* Paper card text */
    .paper-card strong { color: #1a1a2e; }
    .paper-card .meta-line { font-size: 0.82rem; color: #555; margin-top: 4px; }

    /* Chips — claims */
    .claim-chip {
        display: inline-block; background: rgba(37,99,186,0.08);
        border: 1px solid rgba(37,99,186,0.25); border-radius: 8px;
        padding: 6px 12px; margin: 3px; font-size: 0.85rem; color: #1a5096;
    }
    .claim-chip span, .claim-chip .claim-text { color: #1a1a2e; }
    .mol-chip {
        display: inline-block; background: rgba(26,138,74,0.08);
        border: 1px solid rgba(26,138,74,0.25); border-radius: 8px;
        padding: 4px 10px; margin: 2px; font-size: 0.82rem; color: #15713e;
    }
    .kw-chip {
        display: inline-block; background: rgba(100,70,180,0.08);
        border: 1px solid rgba(100,70,180,0.25); border-radius: 20px;
        padding: 4px 12px; margin: 2px; font-size: 0.8rem; color: #5b3db3;
    }

    .feed-entry { padding: 8px 0; border-bottom: 1px solid #e0e4ea; }

    /* Score badges */
    .score-badge {
        display: inline-block; padding: 2px 8px; border-radius: 12px;
        font-size: 0.75rem; font-weight: 600;
    }
    .score-1st { background: rgba(26,138,74,0.12); color: #1a8a4a; }
    .score-2nd { background: rgba(196,127,10,0.12); color: #c47f0a; }
    .score-3rd { background: rgba(192,57,43,0.12); color: #c0392b; }

    /* Session history cards */
    .session-card {
        background: #f8f9fb; border: 1px solid #dde1e8;
        border-radius: 10px; padding: 14px 18px; margin-bottom: 8px;
        color: #1a1a2e;
    }
    .session-card .session-date { color: #666; font-size: 0.8rem; }
    .session-card .session-stat { font-size: 1.2rem; font-weight: 600; color: #1a1a2e; }

    /* Review queue cards */
    .review-card {
        background: #f8f9fb; border: 1px solid #dde1e8;
        border-radius: 10px; padding: 16px; margin-bottom: 12px;
    }
    .review-card strong { color: #1a1a2e; }
    .review-card .review-meta { color: #555; font-size: 0.82rem; }

    /* Molecule category header */
    .mol-category-header {
        color: #1a5096; font-size: 1rem; font-weight: 600;
        margin-top: 16px; margin-bottom: 8px;
        border-bottom: 1px solid #dde1e8; padding-bottom: 6px;
    }

    /* Completion banner */
    .completion-banner {
        background: linear-gradient(135deg, #e8f5ec 0%, #f0faf3 100%);
        border: 2px solid #1a8a4a;
        border-radius: 12px;
        padding: 24px;
        text-align: center;
        margin: 16px 0;
    }
    .completion-banner h2 { color: #1a8a4a; margin: 0 0 8px 0; }
    .completion-banner p { color: #333; margin: 0; font-size: 1rem; }

    /* Keyword submission */
    .kw-attributed {
        display: inline-block; background: rgba(100,70,180,0.08);
        border: 1px solid rgba(100,70,180,0.25); border-radius: 20px;
        padding: 4px 12px; margin: 2px; font-size: 0.8rem; color: #5b3db3;
    }
    .kw-attributed .kw-author { font-size: 0.7rem; color: #666; }

    /* Toggle buttons for selection grids */
    .toggle-btn {
        display: inline-block; border-radius: 8px; padding: 8px 14px;
        margin: 3px; font-size: 0.85rem; cursor: pointer;
        text-align: center; transition: all 0.15s ease;
    }
    .toggle-btn-selected-field {
        border: 3px solid #1a8a4a; background: rgba(26,138,74,0.08); color: #1a1a2e;
    }
    .toggle-btn-unselected {
        border: 1px solid #ccc; background: rgba(0,0,0,0.02); color: #555;
    }
    .toggle-btn-selected-ptype {
        border: 3px solid #c47f0a; background: rgba(196,127,10,0.08); color: #1a1a2e;
    }
    .toggle-btn-selected-kw {
        border: 3px solid #6c46b4; background: rgba(100,70,180,0.08); color: #1a1a2e;
    }

    /* Override Streamlit primary button — softer, not eye-burning */
    div[data-testid="stButton"] button[kind="primary"] {
        background-color: #2563a8 !important;
        border-color: #2563a8 !important;
    }
    div[data-testid="stButton"] button[kind="primary"]:hover {
        background-color: #1d4f8a !important;
        border-color: #1d4f8a !important;
    }
    div[data-testid="stButton"] button[kind="secondary"] {
        border: 2px solid #ccc !important;
        color: #333 !important;
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# SESSION STATE INIT
# ---------------------------------------------------------------------------

defaults = {
    "pipeline_results": [],
    "pipeline_log": [],
    "pipeline_running": False,
    "keywords_used": set(),
    "molecules_found": {},
    "claims_extracted": [],
    "papers_processed": 0,
    "papers_dumped": 0,
    "session_history": [],         # list of past session summaries
    "review_queue": [],            # items awaiting Daniel's approval
    "approved_items": [],          # items Daniel approved
    "rejected_items": [],          # items Daniel rejected
    "user_keywords": [],           # keywords submitted by public users
    "run_complete": False,         # flag for completion popup
    "user_name": "",               # login gate — visitor name
    "user_email": "",              # login gate — visitor email
    "logged_in": False,            # login gate — has user identified
    "flagged_items": [],           # items flagged for Daniel's attention
    "run_feedback": [],            # post-run feedback entries
    "contributor_stats": {},       # {email: {keywords: N, flags: N, runs: N, name: str}}
    "dumped_log": [],              # log of dumped articles
    "run_started": False,          # hide choices during/after run
    "feedback_submitted": False,   # reset choices after feedback
    # Toggle button selections (sets)
    "selected_fields": set(),
    "selected_ptypes": set(),
    "selected_kws": set(),
    "fields_initialized": False,
    "ptypes_initialized": False,
    "kws_initialized": False,
}

for key, val in defaults.items():
    if key not in st.session_state:
        st.session_state[key] = val

# ---------------------------------------------------------------------------
# LOGIN GATE — every visitor must identify before using the dashboard
# ---------------------------------------------------------------------------

if not st.session_state.logged_in:
    st.markdown("""
    <div style="text-align:center; padding: 60px 20px;
         background: linear-gradient(145deg, rgba(37,99,186,0.06) 0%, rgba(26,138,74,0.06) 100%);
         border-radius: 18px; margin: 40px auto; max-width: 700px;
         border: 1px solid #dde1e8;">
        <h1 style="color:#1a1a2e; font-weight:700;">🧪 Flavor Intelligence Dashboard</h1>
        <p style="color:#444; font-size:1.1rem;">GFI — Flavor & Aroma Initiative</p>
        <p style="color:#666; margin-top:20px;">Welcome! Please identify yourself to enter the dashboard.</p>
    </div>
    """, unsafe_allow_html=True)

    with st.form("login_gate"):
        login_col1, login_col2 = st.columns(2)
        with login_col1:
            gate_name = st.text_input("Your name", placeholder="e.g., Sarah K.")
        with login_col2:
            gate_email = st.text_input("Your email", placeholder="e.g., sarah@gfi.org")

        login_submitted = st.form_submit_button("🚀 Enter Dashboard", type="primary", use_container_width=True)

        if login_submitted:
            if gate_name.strip() and gate_email.strip():
                st.session_state.user_name = gate_name.strip()
                st.session_state.user_email = gate_email.strip()
                st.session_state.logged_in = True
                st.rerun()
            else:
                st.warning("Please fill in both your name and email to continue.")

    st.stop()

# ---------------------------------------------------------------------------
# AIRTABLE HELPERS
# ---------------------------------------------------------------------------

@st.cache_data(ttl=60)
def load_airtable_sources():
    if not AT_TOKEN or not AT_BASE_ID:
        return []
    records, offset = [], None
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        try:
            r = requests.get(f"{AT_BASE}/{quote(SOURCES_TABLE)}",
                             headers=AT_HEADERS, params=params, timeout=15)
            if not r.ok:
                break
            data = r.json()
            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
        except Exception:
            break
    return records


@st.cache_data(ttl=60)
def load_airtable_molecules():
    if not AT_TOKEN or not AT_BASE_ID:
        return []
    records, offset = [], None
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        try:
            r = requests.get(f"{AT_BASE}/{quote(MOLECULES_TABLE)}",
                             headers=AT_HEADERS, params=params, timeout=15)
            if not r.ok:
                break
            data = r.json()
            records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
        except Exception:
            break
    return records


def push_to_airtable(table, fields_list):
    """Push a batch of records to Airtable. Returns number of successes."""
    successes = 0
    last_error = ""
    # Airtable batch limit is 10
    for i in range(0, len(fields_list), 10):
        batch = fields_list[i:i+10]
        payload = {"records": [{"fields": f} for f in batch]}
        try:
            r = requests.post(f"{AT_BASE}/{quote(table)}",
                              headers=AT_HEADERS, json=payload, timeout=15)
            if r.ok:
                successes += len(batch)
            else:
                last_error = r.text[:300]
                st.warning(f"Airtable API error: {r.status_code} — {last_error}")
        except Exception as e:
            last_error = str(e)
            st.warning(f"Network error pushing to Airtable: {last_error}")
    return successes


def relevance_tier(score):
    if score is None:
        return "3rd Level — Somewhat Relevant"
    score = float(score)
    if score >= TIER_1ST:
        return "1st Level — High Relevance"
    elif score >= TIER_2ND:
        return "2nd Level — Mid Relevance"
    return "3rd Level — Somewhat Relevant"


def tier_css_class(tier):
    if "1st" in tier:
        return "relevant-1st"
    elif "2nd" in tier:
        return "relevant-2nd"
    return "relevant-3rd"


def score_badge_html(score):
    if score is None:
        return ""
    score = float(score)
    if score >= TIER_1ST:
        cls = "score-1st"
    elif score >= TIER_2ND:
        cls = "score-2nd"
    else:
        cls = "score-3rd"
    return f'<span class="score-badge {cls}">{score:.0%}</span>'


# ---------------------------------------------------------------------------
# KEYWORDS FILE MANAGEMENT (for public keyword submissions)
# ---------------------------------------------------------------------------

KEYWORDS_FILE = Path(__file__).parent / "keywords_bible.json"

def load_keywords_from_file():
    """Load keywords from the JSON file on disk."""
    if KEYWORDS_FILE.exists():
        try:
            data = json.loads(KEYWORDS_FILE.read_text())
            if isinstance(data, list):
                return data
        except Exception:
            pass
    return DEFAULT_KEYWORDS[:]


def save_keywords_to_file(keywords):
    """Save keywords back to the JSON file."""
    try:
        KEYWORDS_FILE.write_text(json.dumps(keywords, indent=2, ensure_ascii=False))
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# DUMPED ARTICLES FILE MANAGEMENT
# ---------------------------------------------------------------------------

DUMPED_FILE = Path(__file__).parent / "dumped_articles.json"

def load_dumped_articles():
    """Load dumped articles log from the JSON file on disk."""
    if DUMPED_FILE.exists():
        try:
            data = json.loads(DUMPED_FILE.read_text())
            if isinstance(data, list):
                return data
        except Exception:
            pass
    return []


def save_dumped_articles(dumped):
    """Save dumped articles log back to the JSON file."""
    try:
        DUMPED_FILE.write_text(json.dumps(dumped, indent=2, ensure_ascii=False))
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# MOCK PIPELINE ENGINE
# ---------------------------------------------------------------------------

# ── Chemistry category hierarchy ──
CATEGORY_HIERARCHY = {
    "Amino Acids":  ["amino acid", "sulfur amino acid", "thiol"],
    "Fats":         ["fatty acid", "lipid", "phospholipid"],
    "Proteins":     ["peptide", "enzyme"],
    "Sugars":       ["sugar", "reducing sugar"],
    "Alcohols":     ["alcohol", "enol"],
    "Aldehydes":    ["aldehyde"],
    "Ketones":      ["ketone"],
    "Pyrazines":    ["pyrazine"],
    "Furanones":    ["furanone"],
    "Nucleotides":  ["nucleotide"],
    "Vitamins":     ["vitamin"],
    "Other":        ["other"],
}

_TYPE_TO_PRIMARY = {}
for _primary, _subtypes in CATEGORY_HIERARCHY.items():
    for _st in _subtypes:
        _TYPE_TO_PRIMARY[_st] = _primary

CATEGORY_DISPLAY_ORDER = [
    "Amino Acids", "Fats", "Sugars", "Proteins", "Alcohols",
    "Aldehydes", "Ketones", "Furanones", "Pyrazines",
    "Nucleotides", "Vitamins", "Other",
]

def get_primary_category(mol_type):
    """Map a specific molecule type to its primary display category."""
    return _TYPE_TO_PRIMARY.get(mol_type, "Other")


MOCK_MOLECULES = [
    {"name": "cysteine",               "type": "sulfur amino acid", "primary": "Amino Acids", "role": "precursor",  "sensory": "",                      "confidence": 0.90},
    {"name": "methionine",             "type": "sulfur amino acid", "primary": "Amino Acids", "role": "precursor",  "sensory": "",                      "confidence": 0.87},
    {"name": "2-methyl-3-furanthiol",  "type": "thiol",             "primary": "Amino Acids", "role": "marker",     "sensory": "meaty, roasted",        "confidence": 0.95},
    {"name": "2-furfurylthiol",        "type": "thiol",             "primary": "Amino Acids", "role": "marker",     "sensory": "coffee, meaty",         "confidence": 0.78},
    {"name": "linoleic acid",          "type": "fatty acid",        "primary": "Fats",        "role": "precursor",  "sensory": "",                      "confidence": 0.83},
    {"name": "phosphatidylcholine",    "type": "phospholipid",      "primary": "Fats",        "role": "precursor",  "sensory": "",                      "confidence": 0.79},
    {"name": "ribose",                 "type": "reducing sugar",    "primary": "Sugars",      "role": "precursor",  "sensory": "",                      "confidence": 0.80},
    {"name": "glucose",                "type": "reducing sugar",    "primary": "Sugars",      "role": "precursor",  "sensory": "",                      "confidence": 0.82},
    {"name": "hexanal",                "type": "aldehyde",          "primary": "Aldehydes",   "role": "marker",     "sensory": "grassy, fatty",         "confidence": 0.88},
    {"name": "methional",              "type": "aldehyde",          "primary": "Aldehydes",   "role": "product",    "sensory": "cooked potato, brothy", "confidence": 0.72},
    {"name": "pyrazine",               "type": "pyrazine",          "primary": "Pyrazines",   "role": "product",    "sensory": "roasted, nutty",        "confidence": 0.82},
    {"name": "2-acetyl-1-pyrroline",   "type": "pyrazine",          "primary": "Pyrazines",   "role": "marker",     "sensory": "popcorn, roasted",      "confidence": 0.77},
    {"name": "4-hydroxy-2,5-dimethyl-3(2H)-furanone", "type": "furanone", "primary": "Furanones", "role": "marker", "sensory": "caramel, meaty",       "confidence": 0.91},
    {"name": "IMP",                    "type": "nucleotide",        "primary": "Nucleotides", "role": "potentiator","sensory": "umami",                 "confidence": 0.76},
    {"name": "thiamine",               "type": "vitamin",           "primary": "Vitamins",    "role": "precursor",  "sensory": "",                      "confidence": 0.85},
]

MOCK_CLAIMS = [
    "Maillard reaction between cysteine and ribose at 140\u00b0C produces 2-methyl-3-furanthiol as the dominant meaty odorant.",
    "Hexanal concentration above 0.5 ppm indicates significant lipid oxidation in beef samples.",
    "Plant-based matrices require supplementation with sulfur-containing precursors to replicate meaty aroma.",
    "GC-MS with SPME headspace extraction achieves detection limits below 1 ppb for key meat volatiles.",
    "Phospholipid oxidation contributes more to species-specific meat flavor than triglyceride oxidation.",
    "Pyrazine formation rate increases exponentially above 120\u00b0C in model Maillard systems.",
    "Thiamine degradation is the primary source of meaty thiols in boiled and stewed meat.",
    "Cross-modal interactions between texture and aroma significantly alter meat-likeness perception in plant-based products.",
    "Lipid-Maillard interaction products exhibit lower odor thresholds than pure Maillard volatiles.",
    "Strecker degradation of methionine yields methional, a key contributor to cooked potato and brothy notes.",
]

MOCK_BRANCHES = [
    "Maillard Reaction", "Lipid Oxidation", "Volatile Compounds",
    "Precursors", "Sulfur Chemistry", "Analytical Methods", "Meat Analogs",
]

DEFAULT_KEYWORDS = [
    {"keyword": "Maillard reaction meat flavor", "branch": "Maillard Reaction", "priority": "HIGH", "added_by": "system"},
    {"keyword": "cysteine ribose model system meat", "branch": "Precursors", "priority": "HIGH", "added_by": "system"},
    {"keyword": "lipid oxidation beef phospholipid", "branch": "Lipid Oxidation", "priority": "HIGH", "added_by": "system"},
    {"keyword": "plant-based meat flavor analog", "branch": "Meat Analogs", "priority": "HIGH", "added_by": "system"},
    {"keyword": "2-methyl-3-furanthiol cooked meat", "branch": "Volatile Compounds", "priority": "HIGH", "added_by": "system"},
    {"keyword": "volatile organic compounds meat aroma GC-MS", "branch": "Analytical Methods", "priority": "MEDIUM", "added_by": "system"},
    {"keyword": "pyrazine formation roasting", "branch": "Maillard Reaction", "priority": "MEDIUM", "added_by": "system"},
    {"keyword": "thiamine degradation sulfur compounds meat", "branch": "Sulfur Chemistry", "priority": "MEDIUM", "added_by": "system"},
    {"keyword": "Strecker degradation amino acid flavor", "branch": "Precursors", "priority": "MEDIUM", "added_by": "system"},
    {"keyword": "phospholipid oxidation species specific flavor", "branch": "Lipid Oxidation", "priority": "MEDIUM", "added_by": "system"},
    {"keyword": "sensory evaluation meat analog consumer", "branch": "Meat Analogs", "priority": "LOW", "added_by": "system"},
    {"keyword": "hexanal lipid oxidation indicator", "branch": "Lipid Oxidation", "priority": "LOW", "added_by": "system"},
]

# Load keywords — from file if it exists, otherwise defaults
KEYWORDS_BIBLE = load_keywords_from_file()


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
    """Simulate Claude extraction on a paper."""
    title = paper.get("title", "")
    abstract = paper.get("abstract", "")
    seed = int(hashlib.md5(title.lower().encode()).hexdigest()[:8], 16)
    rng = random.Random(seed)

    text = (title + " " + abstract).lower()
    kw_words = keyword_entry["keyword"].lower().split()
    overlap = sum(1 for w in kw_words if w in text) / max(len(kw_words), 1)

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
        "timestamp": datetime.now().isoformat(),
    }


# ---------------------------------------------------------------------------
# RESEARCH FIELDS & PAPER TYPES (for pipeline focus grids)
# ---------------------------------------------------------------------------

RESEARCH_FIELDS = [
    {"name": "Maillard Chemistry",    "icon": "🔥", "desc": "Browning reactions, sugar-amino acid interactions"},
    {"name": "Lipid Oxidation",       "icon": "🫧", "desc": "Fat breakdown, rancidity, flavor formation"},
    {"name": "Sensory Science",       "icon": "👃", "desc": "Human perception, taste panels, psychophysics"},
    {"name": "Analytical Methods",    "icon": "🔬", "desc": "GC-MS, SPME, headspace analysis techniques"},
    {"name": "Plant-Based Analogs",   "icon": "🌱", "desc": "Meat alternatives, flavor replication"},
    {"name": "Fermentation",          "icon": "🧫", "desc": "Microbial flavor generation, fermented foods"},
    {"name": "Enzymatic Processes",   "icon": "⚗️", "desc": "Enzyme-catalyzed flavor development"},
    {"name": "Sulfur Chemistry",      "icon": "💛", "desc": "Thiols, disulfides, meaty aroma precursors"},
]

PAPER_TYPES = [
    {"name": "Experimental Studies", "icon": "🧪", "desc": "Original lab research with data"},
    {"name": "Review Articles",      "icon": "📖", "desc": "Comprehensive literature surveys"},
    {"name": "Meta-analyses",        "icon": "📊", "desc": "Statistical synthesis across studies"},
    {"name": "Patents",              "icon": "📜", "desc": "Industrial applications and IP"},
]


def get_smart_keyword_recommendations(keywords_bible, used_keywords, n=8):
    """Pick smart keyword recommendations: prioritize HIGH, then unused, then variety across branches."""
    unused = [kw for kw in keywords_bible if kw["keyword"] not in used_keywords]
    used = [kw for kw in keywords_bible if kw["keyword"] in used_keywords]

    priority_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    unused.sort(key=lambda x: priority_order.get(x.get("priority", "LOW"), 2))

    recommended = []
    seen_branches = set()
    for kw in unused:
        branch = kw.get("branch", "Other")
        if branch not in seen_branches or len(recommended) < n:
            recommended.append(kw)
            seen_branches.add(branch)
        if len(recommended) >= n:
            break

    if len(recommended) < n:
        for kw in used:
            if kw not in recommended:
                recommended.append(kw)
            if len(recommended) >= n:
                break

    return recommended


# ---------------------------------------------------------------------------
# SIDEBAR
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("### 🧪 Flavor Intelligence")
    st.caption("GFI — Flavor & Aroma Initiative")
    st.markdown(f"**👤 {st.session_state.user_name}**")
    st.caption(st.session_state.user_email)
    st.divider()

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

    if AT_TOKEN and AT_BASE_ID:
        st.link_button("📊 Open Airtable Database",
                        "https://airtable.com/appcS9K0FZK2DIPbZ",
                        use_container_width=True)
    st.divider()

    st.markdown("**This Session**")
    st.metric("Papers Processed", st.session_state.papers_processed)
    st.metric("Papers Dumped", st.session_state.papers_dumped)
    st.metric("Molecules Found", len(st.session_state.molecules_found))
    st.metric("Claims Extracted", len(st.session_state.claims_extracted))
    st.metric("Pending Review", len(st.session_state.review_queue))

    st.divider()
    if st.button("🔄 Refresh Airtable Data"):
        st.cache_data.clear()
        st.rerun()


# ---------------------------------------------------------------------------
# MAIN TABS
# ---------------------------------------------------------------------------

tab_live, tab_review, tab_sources, tab_molecules, tab_claims, tab_keywords, tab_community = st.tabs([
    "🔴 Live Pipeline",
    "✅ Review Gate",
    "📚 Sources & Relevance",
    "🧬 Last Molecules Added",
    "📋 Claims Feed",
    "🧠 Keywords Brain",
    "🏆 Community",
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — LIVE PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

with tab_live:
    st.header("🔴 Live Pipeline Monitor")
    st.caption(
        "Watch the algorithm process academic papers in real-time. "
        "Papers are fetched from OpenAlex, analyzed for relevance, "
        "and classified into 1st / 2nd / 3rd level tiers."
    )

    # ---- COMPLETION POP-UP ----
    if st.session_state.run_complete:
        total = st.session_state.papers_processed
        relevant = total - st.session_state.papers_dumped
        mols = len(st.session_state.molecules_found)
        claims = len(st.session_state.claims_extracted)
        st.markdown(f"""
<div class="completion-banner">
    <h2>🎉 Pipeline Run Complete!</h2>
    <p><strong>{total}</strong> papers processed &nbsp;&middot;&nbsp;
       <strong>{relevant}</strong> relevant &nbsp;&middot;&nbsp;
       <strong>{mols}</strong> molecules &nbsp;&middot;&nbsp;
       <strong>{claims}</strong> claims</p>
    <p style="margin-top:10px; font-size:0.9rem;">
       Head to the <strong>Review Gate</strong> tab to approve items before they reach Airtable.
    </p>
</div>
        """, unsafe_allow_html=True)
        st.session_state.run_complete = False

    # ---- SESSION HISTORY ----
    if st.session_state.session_history:
        with st.expander("📋 Recent Runs", expanded=False):
            for sess in reversed(st.session_state.session_history[-5:]):
                kw_text = ', '.join(sess.get('keywords', [])[:3])
                st.markdown(
                    f"**{sess['date']}** — "
                    f"{sess['processed']} processed · "
                    f"{sess['relevant']} relevant · "
                    f"{sess['dumped']} dumped · "
                    f"{sess['molecules']} molecules · "
                    f"{sess['claims']} claims"
                )
                if kw_text:
                    st.caption(f"Keywords: {kw_text}")
        st.divider()

    # ================================================================
    # CHOICE SECTION — only show when run has not started (or after feedback)
    # ================================================================

    show_choices = not st.session_state.run_started or (
        not st.session_state.pipeline_results and not st.session_state.run_started
    )

    if show_choices:
        # ================================================================
        # STEP 1: CHOOSE YOUR FOCUS — Research Fields (toggle buttons)
        # ================================================================
        st.markdown("#### 🔬 Focus Your Search — Research Fields")
        st.caption("Click to toggle fields on/off for the pipeline")

        # Initialize defaults (first 3 selected)
        if not st.session_state.fields_initialized:
            st.session_state.selected_fields = {f["name"] for f in RESEARCH_FIELDS[:3]}
            st.session_state.fields_initialized = True

        field_cols = st.columns(4)
        for i, field in enumerate(RESEARCH_FIELDS):
            with field_cols[i % 4]:
                is_sel = field["name"] in st.session_state.selected_fields
                label = f"{field['icon']} {field['name']}"
                btn_type = "primary" if is_sel else "secondary"
                if st.button(label, key=f"field_toggle_{i}",
                             help=field["desc"], use_container_width=True,
                             type=btn_type):
                    if field["name"] in st.session_state.selected_fields:
                        st.session_state.selected_fields.discard(field["name"])
                    else:
                        st.session_state.selected_fields.add(field["name"])
                    st.rerun()

        selected_fields = list(st.session_state.selected_fields)

        # ================================================================
        # STEP 2: CHOOSE YOUR FOCUS — Paper Types (toggle buttons)
        # ================================================================
        st.markdown("#### 📄 Paper Types")
        st.caption("Click to toggle paper types on/off")

        if not st.session_state.ptypes_initialized:
            st.session_state.selected_ptypes = {p["name"] for p in PAPER_TYPES[:2]}
            st.session_state.ptypes_initialized = True

        type_cols = st.columns(4)
        for i, ptype in enumerate(PAPER_TYPES):
            with type_cols[i % 4]:
                is_sel = ptype["name"] in st.session_state.selected_ptypes
                label = f"{ptype['icon']} {ptype['name']}"
                btn_type = "primary" if is_sel else "secondary"
                if st.button(label, key=f"ptype_toggle_{i}",
                             help=ptype["desc"], use_container_width=True,
                             type=btn_type):
                    if ptype["name"] in st.session_state.selected_ptypes:
                        st.session_state.selected_ptypes.discard(ptype["name"])
                    else:
                        st.session_state.selected_ptypes.add(ptype["name"])
                    st.rerun()

        selected_paper_types = list(st.session_state.selected_ptypes)

        st.divider()

        # ================================================================
        # STEP 3: KEYWORD FOCUS — Smart Recommendations + Custom
        # ================================================================
        st.markdown("#### 🎯 Keyword Focus")
        st.caption("Click to toggle recommended keywords on/off, and add your own")

        recommended_kws = get_smart_keyword_recommendations(
            KEYWORDS_BIBLE, st.session_state.keywords_used, n=8
        )

        # Initialize keyword defaults (HIGH priority selected)
        if not st.session_state.kws_initialized:
            st.session_state.selected_kws = {
                kw["keyword"] for kw in recommended_kws if kw.get("priority") == "HIGH"
            }
            st.session_state.kws_initialized = True

        kw_cols = st.columns(4)
        for i, kw in enumerate(recommended_kws):
            priority_icon = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "⚪"}.get(kw.get("priority", ""), "⚪")
            with kw_cols[i % 4]:
                is_sel = kw["keyword"] in st.session_state.selected_kws
                label = f"{priority_icon} {kw['keyword'][:35]}"
                btn_type = "primary" if is_sel else "secondary"
                if st.button(label, key=f"kw_toggle_{i}",
                             help=f"Branch: {kw.get('branch', '')} · Priority: {kw.get('priority', '')}",
                             use_container_width=True,
                             type=btn_type):
                    if kw["keyword"] in st.session_state.selected_kws:
                        st.session_state.selected_kws.discard(kw["keyword"])
                    else:
                        st.session_state.selected_kws.add(kw["keyword"])
                    st.rerun()

        # Build selected keyword entries from the set
        selected_keywords = [kw for kw in recommended_kws if kw["keyword"] in st.session_state.selected_kws]

        # Custom keyword input
        custom_kw_text = st.text_input(
            "➕ Extra keywords (comma-separated)",
            placeholder="e.g., vanillin lignin, umami peptides, fermented soy",
            help="Add any keywords you want the pipeline to search for"
        )

        # Parse custom keywords into keyword entries
        custom_entries = []
        if custom_kw_text.strip():
            for raw_kw in custom_kw_text.split(","):
                kw = raw_kw.strip()
                if kw:
                    custom_entries.append({
                        "keyword": kw,
                        "branch": "Custom",
                        "priority": "MEDIUM",
                        "added_by": st.session_state.user_name,
                    })

        # Papers per keyword control
        papers_per_kw = st.slider("Papers per keyword", 3, 15, value=5)

        all_keywords_to_run = selected_keywords + custom_entries

        st.divider()

        # ================================================================
        # RUN PIPELINE
        # ================================================================

        if not all_keywords_to_run:
            st.warning("Select at least one keyword to run the pipeline.")
        elif not selected_fields:
            st.warning("Select at least one research field.")

        run_disabled = not all_keywords_to_run or not selected_fields

        if st.button("🚀 Start Pipeline Run", type="primary", use_container_width=True, disabled=run_disabled):
            # Mark run as started — hides choices
            st.session_state.run_started = True
            st.session_state.feedback_submitted = False

            # Reset session state for new run
            st.session_state.pipeline_results = []
            st.session_state.pipeline_log = []
            st.session_state.papers_processed = 0
            st.session_state.papers_dumped = 0
            st.session_state.keywords_used = set()
            st.session_state.molecules_found = {}
            st.session_state.claims_extracted = []
            st.session_state.review_queue = []
            st.session_state.run_complete = False
            st.session_state.dumped_log = []

            # Track contributor stats
            user_email = st.session_state.user_email
            if user_email not in st.session_state.contributor_stats:
                st.session_state.contributor_stats[user_email] = {
                    "name": st.session_state.user_name, "keywords": 0, "flags": 0, "runs": 0,
                }
            st.session_state.contributor_stats[user_email]["runs"] += 1

            # Add custom keywords to the bible for future users
            for entry in custom_entries:
                existing_kws = [kw["keyword"].lower() for kw in KEYWORDS_BIBLE]
                if entry["keyword"].lower() not in existing_kws:
                    full_entry = {
                        **entry,
                        "added_email": user_email,
                        "added_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    }
                    KEYWORDS_BIBLE.append(full_entry)
                    save_keywords_to_file(KEYWORDS_BIBLE)
                    st.session_state.contributor_stats[user_email]["keywords"] += 1

            # Append field/type context to search queries for better results
            field_terms = " ".join(f.lower().replace("-", " ") for f in selected_fields[:2])
            type_terms = " ".join(t.lower() for t in selected_paper_types[:1])

            feed_col, stats_col = st.columns([3, 1])

            with stats_col:
                st.markdown("### 📊 Live Stats")
                stat_processed = st.empty()
                stat_relevant = st.empty()
                stat_dumped = st.empty()
                stat_molecules = st.empty()
                stat_claims = st.empty()
                st.markdown("---")
                st.markdown("### Relevance Levels")
                tier_1st_ct = st.empty()
                tier_2nd_ct = st.empty()
                tier_3rd_ct = st.empty()
                st.markdown("---")
                tier_chart_slot = st.empty()

            with feed_col:
                st.markdown("### 📡 Processing Feed")
                st.caption(
                    f"Fields: {', '.join(selected_fields)} · "
                    f"Types: {', '.join(selected_paper_types) or 'All'}"
                )
                progress_bar = st.progress(0)
                status_text = st.empty()
                feed_container = st.container()

            total_work = len(all_keywords_to_run) * papers_per_kw
            completed = 0
            tier_counts = {"1st Level — High Relevance": 0, "2nd Level — Mid Relevance": 0, "3rd Level — Somewhat Relevant": 0}
            kw_names_used = []
            delay = 0.5

            for kw_entry in all_keywords_to_run:
                keyword = kw_entry["keyword"]
                branch = kw_entry.get("branch", "Other")
                st.session_state.keywords_used.add(keyword)
                kw_names_used.append(keyword)

                # Enhance search with field context
                search_query = f"{keyword} {field_terms}"

                with feed_container:
                    st.markdown(
                        f"**🔍 Searching:** `{keyword}` → Branch: **{branch}**"
                    )

                status_text.markdown(f"⏳ Fetching papers for: *{keyword}*...")
                papers = fetch_openalex_papers(search_query, papers_per_kw)

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

                    result = mock_extraction(paper, kw_entry)
                    st.session_state.papers_processed += 1
                    st.session_state.pipeline_results.append(result)

                    tier = relevance_tier(result["relevance_score"])
                    tier_counts[tier] = tier_counts.get(tier, 0) + 1

                    if result["relevant"]:
                        for mol in result.get("molecules", []):
                            name = mol["name"]
                            if name not in st.session_state.molecules_found:
                                st.session_state.molecules_found[name] = {
                                    **mol,
                                    "discovered_at": datetime.now().isoformat(),
                                    "from_keyword": keyword,
                                }

                        for claim in result.get("claims", []):
                            if claim not in st.session_state.claims_extracted:
                                st.session_state.claims_extracted.append(claim)

                        st.session_state.review_queue.append({
                            "type": "source",
                            "title": result["title"],
                            "year": result.get("year"),
                            "venue": result.get("venue", ""),
                            "relevance_score": result["relevance_score"],
                            "tier": tier,
                            "branch": result.get("branch", ""),
                            "molecules": result.get("molecules", []),
                            "claims": result.get("claims", []),
                            "url": result.get("url", ""),
                            "keyword": keyword,
                        })

                        card_class = tier_css_class(tier)
                        with feed_container:
                            score_html = score_badge_html(result["relevance_score"])
                            mols_html = " ".join(
                                f'<span class="mol-chip">{m["name"]}</span>'
                                for m in result.get("molecules", [])[:3]
                            )
                            claims_html = " ".join(
                                f'<span class="claim-chip"><span class="claim-text">{c[:70]}...</span></span>'
                                for c in result.get("claims", [])[:2]
                            )
                            st.markdown(f"""
<div class="paper-card {card_class}">
  <strong>{result['title'][:80]}</strong>
  {f'<a href="{result.get("url","")}" target="_blank" style="float:right;font-size:0.85rem;text-decoration:none;">🔗</a>' if result.get('url') else ''}
  <span style="float:right;margin-right:8px">{score_html}</span>
  <div class="meta-line">{result.get('authors','')[:60]} · {result.get('year','')} · {result.get('venue','')[:40]} · {tier}</div>
  {mols_html}
  {('<br>' + claims_html) if claims_html else ''}
</div>
                            """, unsafe_allow_html=True)
                    else:
                        st.session_state.papers_dumped += 1
                        # Log dumped article
                        dumped_entry = {
                            "title": result.get("title", ""),
                            "year": result.get("year"),
                            "score": result["relevance_score"],
                            "keyword": keyword,
                            "dumped_at": datetime.now().isoformat(),
                        }
                        st.session_state.dumped_log.append(dumped_entry)

                        with feed_container:
                            st.markdown(f"""
<div class="paper-card dumped">
  <span style="color:#e74c3c; font-weight:600">✗ DUMPED</span> &nbsp;
  <span style="color:#555">{result['title'][:70]}</span>
  <span style="float:right;font-size:0.85rem;color:#666">Score: {result['relevance_score']:.0%}</span>
  <div class="meta-line">{result.get('authors','')[:60]}</div>
</div>
                            """, unsafe_allow_html=True)

                    # Update live stats
                    total_relevant = st.session_state.papers_processed - st.session_state.papers_dumped
                    stat_processed.metric("Processed", st.session_state.papers_processed)
                    stat_relevant.metric("Relevant", total_relevant)
                    stat_dumped.metric("Dumped", st.session_state.papers_dumped)
                    stat_molecules.metric("Molecules", len(st.session_state.molecules_found))
                    stat_claims.metric("Claims", len(st.session_state.claims_extracted))

                    tier_1st_ct.markdown(
                        f"<span class='tier-1st'>{tier_counts.get('1st Level — High Relevance',0)}</span> 1st Level",
                        unsafe_allow_html=True
                    )
                    tier_2nd_ct.markdown(
                        f"<span class='tier-2nd'>{tier_counts.get('2nd Level — Mid Relevance',0)}</span> 2nd Level",
                        unsafe_allow_html=True
                    )
                    tier_3rd_ct.markdown(
                        f"<span class='tier-3rd'>{tier_counts.get('3rd Level — Somewhat Relevant',0)}</span> 3rd Level",
                        unsafe_allow_html=True
                    )

                    tier_df = pd.DataFrame({
                        "Tier": list(tier_counts.keys()),
                        "Count": list(tier_counts.values()),
                    })
                    fig = px.pie(tier_df, values="Count", names="Tier",
                                 color="Tier", color_discrete_map=TIER_COLORS, hole=0.45)
                    fig.update_layout(
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                        font_color="#1a1a2e", showlegend=False,
                        height=200, margin=dict(t=10, b=10, l=10, r=10),
                    )
                    tier_chart_slot.plotly_chart(fig, use_container_width=True)

                    time.sleep(delay)

            # ---- SAVE DUMPED ARTICLES TO JSON ----
            if st.session_state.dumped_log:
                existing_dumped = load_dumped_articles()
                existing_dumped.extend(st.session_state.dumped_log)
                save_dumped_articles(existing_dumped)

            # ---- RUN COMPLETE ----
            status_text.markdown("✅ **Pipeline run complete!**")
            progress_bar.progress(1.0)

            st.session_state.session_history.append({
                "date": datetime.now().strftime("%Y-%m-%d"),
                "time": datetime.now().strftime("%H:%M"),
                "processed": st.session_state.papers_processed,
                "relevant": st.session_state.papers_processed - st.session_state.papers_dumped,
                "dumped": st.session_state.papers_dumped,
                "molecules": len(st.session_state.molecules_found),
                "claims": len(st.session_state.claims_extracted),
                "keywords": kw_names_used,
                "fields": selected_fields,
                "paper_types": selected_paper_types,
                "user": st.session_state.user_name,
            })
            st.session_state.session_history = st.session_state.session_history[-5:]

            total = st.session_state.papers_processed
            relevant = total - st.session_state.papers_dumped
            mols = len(st.session_state.molecules_found)
            claims_ct = len(st.session_state.claims_extracted)
            review_ct = len(st.session_state.review_queue)

            with feed_container:
                st.markdown(f"""
<div class="completion-banner">
    <h2>🎉 Pipeline Run Complete!</h2>
    <p><strong>{total}</strong> papers &nbsp;&middot;&nbsp;
       <strong>{relevant}</strong> relevant &nbsp;&middot;&nbsp;
       <strong>{mols}</strong> molecules &nbsp;&middot;&nbsp;
       <strong>{claims_ct}</strong> claims</p>
    <p style="margin-top:10px; font-size:0.9rem;">
        <strong>{review_ct}</strong> items waiting in the <strong>Review Gate</strong> for Daniel's approval.
    </p>
</div>
                """, unsafe_allow_html=True)

                # ---- DUMPED ARTICLES SECTION ----
                if st.session_state.dumped_log:
                    with st.expander(f"📦 Dumped Articles ({len(st.session_state.dumped_log)})", expanded=False):
                        for d_idx, d_item in enumerate(st.session_state.dumped_log):
                            dc1, dc2 = st.columns([8, 1])
                            with dc1:
                                st.markdown(
                                    f"**{d_item['title'][:70]}** — "
                                    f"Score: {d_item['score']:.0%} · "
                                    f"Keyword: _{d_item['keyword'][:30]}_ · "
                                    f"{d_item.get('year', 'N/A')}"
                                )
                            with dc2:
                                if st.button("🔄", key=f"rescue_run_{d_idx}", help="Rescue — move to review queue"):
                                    rescued = st.session_state.dumped_log.pop(d_idx)
                                    st.session_state.review_queue.append({
                                        "type": "source",
                                        "title": rescued["title"],
                                        "year": rescued.get("year"),
                                        "venue": "",
                                        "relevance_score": rescued["score"],
                                        "tier": relevance_tier(rescued["score"]),
                                        "branch": "",
                                        "molecules": [],
                                        "claims": [],
                                        "url": "",
                                        "keyword": rescued["keyword"],
                                    })
                                    st.success(f"Rescued! Moved to Review Gate.")
                                    st.rerun()

            st.balloons()
            st.session_state.run_complete = True

            # ---- POST-RUN FEEDBACK ----
            with feed_container:
                st.markdown("---")
                st.markdown("#### 💬 How was this run?")
                with st.form("run_feedback", clear_on_submit=True):
                    fb_rating = st.select_slider(
                        "Was this run useful?",
                        options=["Not useful", "Somewhat", "Useful", "Very useful", "Excellent"],
                        value="Useful"
                    )
                    fb_learned = st.text_area(
                        "What did you learn or notice?",
                        placeholder="e.g., Found interesting papers on thiamine degradation, "
                                    "the Maillard keywords gave the best results...",
                        height=80,
                    )
                    fb_submit = st.form_submit_button("📨 Submit Feedback")
                    if fb_submit:
                        st.session_state.run_feedback.append({
                            "user": st.session_state.user_name,
                            "email": st.session_state.user_email,
                            "rating": fb_rating,
                            "learned": fb_learned,
                            "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
                            "papers_processed": total,
                            "keywords": kw_names_used,
                        })
                        st.session_state.feedback_submitted = True
                        st.session_state.run_started = False
                        st.success("Thanks for the feedback! Daniel will see this.")

    # Show previous results if any (after rerun)
    elif st.session_state.pipeline_results:
        st.info(f"Showing results from last run: {st.session_state.papers_processed} papers processed")
        results = st.session_state.pipeline_results
        relevant_results = [r for r in results if r.get("relevant")]
        dumped_results = [r for r in results if not r.get("relevant")]

        r1, r2, r3, r4 = st.columns(4)
        r1.metric("Total Processed", len(results))
        r2.metric("Relevant", len(relevant_results))
        r3.metric("Dumped", len(dumped_results))
        r4.metric("Acceptance Rate", f"{len(relevant_results)/max(len(results),1):.0%}")

        # Progress bar through feed
        total_to_show = len(relevant_results[:20])
        if total_to_show > 0:
            review_progress = st.progress(0)

        for idx, r in enumerate(relevant_results[:20]):
            tier = relevance_tier(r["relevance_score"])
            card_class = tier_css_class(tier)
            mols_html = " ".join(
                f'<span class="mol-chip">{m["name"]}</span>'
                for m in r.get("molecules", [])[:3]
            )

            # 2-column layout: card content (col 9) + flag button (col 1)
            card_col, flag_col = st.columns([9, 1])

            with card_col:
                st.markdown(f"""
<div class="paper-card {card_class}">
  <strong>{r['title'][:80]}</strong>
  {f'<a href="{r.get("url","")}" target="_blank" style="float:right;font-size:0.85rem;text-decoration:none;">🔗</a>' if r.get('url') else ''}
  {score_badge_html(r['relevance_score'])} &nbsp; {tier}<br>
  <div class="meta-line">{r.get('authors','')[:60]} · {r.get('year','')} · {r.get('venue','')[:40]}</div>
  {mols_html}
</div>
                """, unsafe_allow_html=True)

            with flag_col:
                if st.button("🚩", key=f"flag_result_{idx}", help="Flag for Daniel"):
                    st.session_state.flagged_items.append({
                        **r,
                        "flagged_by": st.session_state.user_name,
                        "flagged_email": st.session_state.user_email,
                        "flagged_at": datetime.now().isoformat(),
                        "flag_reason": "Interesting finding",
                    })
                    ue = st.session_state.user_email
                    if ue not in st.session_state.contributor_stats:
                        st.session_state.contributor_stats[ue] = {
                            "name": st.session_state.user_name, "keywords": 0, "flags": 0, "runs": 0,
                        }
                    st.session_state.contributor_stats[ue]["flags"] += 1
                    st.success("🚩 Flagged!")

            # Update progress bar
            if total_to_show > 0:
                progress_val = (idx + 1) / total_to_show
                review_progress.progress(progress_val)

        if total_to_show > 0:
            remaining = total_to_show - total_to_show  # all shown
            st.markdown(f"📊 **{total_to_show} of {len(relevant_results)} papers reviewed** — scroll down to submit feedback")

        # Dumped articles section for post-run view
        if st.session_state.dumped_log:
            with st.expander(f"📦 Dumped Articles ({len(st.session_state.dumped_log)})", expanded=False):
                for d_idx2, d_item in enumerate(st.session_state.dumped_log):
                    dc1, dc2 = st.columns([8, 1])
                    with dc1:
                        st.markdown(
                            f"**{d_item['title'][:70]}** — "
                            f"Score: {d_item['score']:.0%} · "
                            f"Keyword: _{d_item['keyword'][:30]}_ · "
                            f"{d_item.get('year', 'N/A')}"
                        )
                    with dc2:
                        if st.button("🔄", key=f"rescue_post_{d_idx2}", help="Rescue — move to review queue"):
                            rescued = st.session_state.dumped_log.pop(d_idx2)
                            st.session_state.review_queue.append({
                                "type": "source",
                                "title": rescued["title"],
                                "year": rescued.get("year"),
                                "venue": "",
                                "relevance_score": rescued["score"],
                                "tier": relevance_tier(rescued["score"]),
                                "branch": "",
                                "molecules": [],
                                "claims": [],
                                "url": "",
                                "keyword": rescued["keyword"],
                            })
                            st.success(f"Rescued! Moved to Review Gate.")
                            st.rerun()

    else:
        st.markdown("""
        **How it works:**
        1. **Choose your focus** — pick research fields and paper types above
        2. **Select keywords** — toggle recommended keywords or add your own
        3. Click **Start Pipeline Run**
        4. Papers are fetched from OpenAlex and analyzed for relevance
        5. Results are classified: **1st Level** (High Relevance) · **2nd Level** (Mid Relevance) · **3rd Level** (Somewhat Relevant)
        6. Relevant items go to the **Review Gate** for approval before reaching Airtable
        7. Your custom keywords are saved for future users

        *Every run you do makes the database smarter for everyone.*
        """)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — REVIEW GATE (Daniel's approval flow)
# ═══════════════════════════════════════════════════════════════════════════════

with tab_review:
    st.header("✅ Review Gate — Daniel's Approval")
    st.caption(
        "Items scoring 70%+ are auto-approved. "
        "Only items below 70% relevance appear here for manual review. "
        "Approved items can then be pushed to Airtable."
    )

    queue = st.session_state.review_queue

    # Auto-approve items with score >= 0.70 and filter queue to < 0.70
    auto_approved = [item for item in queue if item.get("relevance_score", 0) >= 0.70]
    manual_queue = [item for item in queue if item.get("relevance_score", 0) < 0.70]

    if auto_approved:
        st.session_state.approved_items.extend(auto_approved)
        st.session_state.review_queue = manual_queue
        queue = manual_queue

    if not queue and not st.session_state.approved_items:
        st.info("No items to review. Run the pipeline first to populate the queue.")
    else:
        # Summary metrics
        q1, q2, q3 = st.columns(3)
        q1.metric("⏳ Pending Review (< 70%)", len(queue))
        q2.metric("✅ Approved", len(st.session_state.approved_items))
        q3.metric("❌ Rejected", len(st.session_state.rejected_items))

        if auto_approved:
            st.success(f"Auto-approved {len(auto_approved)} items scoring 70%+ relevance.")

        st.divider()

        # Batch actions
        if queue:
            st.markdown("#### Pending Items (Below 70% Relevance)")
            batch_col1, batch_col2, batch_col3 = st.columns([1, 1, 2])
            with batch_col1:
                if st.button("✅ Approve All", type="primary"):
                    st.session_state.approved_items.extend(queue)
                    st.session_state.review_queue = []
                    st.rerun()
            with batch_col2:
                if st.button("❌ Reject All"):
                    st.session_state.rejected_items.extend(queue)
                    st.session_state.review_queue = []
                    st.rerun()

            # ---- CSV EXPORT / RE-UPLOAD ----
            st.markdown("##### 📥 Export & Re-upload for AI Review")
            st.caption(
                "Export pending items as CSV → send to your AI chatbot for review → "
                "re-upload the CSV with the 'decision' column filled (approve / reject)."
            )

            export_col, upload_col = st.columns(2)

            with export_col:
                # Build CSV for download
                csv_rows = []
                for i, item in enumerate(queue):
                    claims_text = " | ".join(
                        c[:120] for c in item.get("claims", [])[:5]
                    )
                    csv_rows.append({
                        "id": i,
                        "title": item.get("title", ""),
                        "authors": item.get("authors", ""),
                        "relevance_score": f"{item.get('relevance_score', 0):.0%}",
                        "claims": claims_text,
                        "decision": "",  # Empty — to be filled by Daniel / AI
                    })
                export_df = pd.DataFrame(csv_rows)
                csv_bytes = export_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "📤 Export Pending to CSV",
                    data=csv_bytes,
                    file_name="review_queue_for_approval.csv",
                    mime="text/csv",
                    use_container_width=True,
                )

            with upload_col:
                uploaded_csv = st.file_uploader(
                    "📥 Upload reviewed CSV",
                    type=["csv"],
                    key="review_csv_upload",
                    help="Upload the CSV with the 'decision' column filled: approve / reject",
                )

            if uploaded_csv is not None:
                try:
                    decisions_df = pd.read_csv(uploaded_csv)
                    if "decision" not in decisions_df.columns:
                        st.error("CSV must have a 'decision' column with 'approve' or 'reject' values.")
                    else:
                        approved_count = 0
                        rejected_count = 0
                        remaining = []

                        for _, row in decisions_df.iterrows():
                            decision = str(row.get("decision", "")).strip().lower()
                            idx = int(row.get("id", -1))
                            if 0 <= idx < len(queue):
                                if decision == "approve":
                                    st.session_state.approved_items.append(queue[idx])
                                    approved_count += 1
                                elif decision == "reject":
                                    st.session_state.rejected_items.append(queue[idx])
                                    rejected_count += 1
                                else:
                                    remaining.append(queue[idx])
                            else:
                                if idx < len(queue):
                                    remaining.append(queue[idx])

                        # Keep only items without decisions
                        st.session_state.review_queue = remaining
                        st.success(
                            f"CSV processed! ✅ {approved_count} approved · "
                            f"❌ {rejected_count} rejected · "
                            f"⏳ {len(remaining)} still pending"
                        )
                        st.rerun()
                except Exception as e:
                    st.error(f"Error reading CSV: {e}")

            st.divider()

            # Individual items
            for idx, item in enumerate(queue):
                tier = item.get("tier", "3rd Level — Somewhat Relevant")
                card_class = tier_css_class(tier)
                mols_html = " ".join(
                    f'<span class="mol-chip">{m["name"]}</span>'
                    for m in item.get("molecules", [])[:4]
                )
                claims_html = " ".join(
                    f'<span class="claim-chip"><span class="claim-text">{c[:60]}...</span></span>'
                    for c in item.get("claims", [])[:2]
                )

                st.markdown(f"""
<div class="review-card" style="border-left: 4px solid {TIER_COLORS.get(tier, '#555')};">
  <strong>{item['title'][:90]}</strong>
  {f'<a href="{item.get("url","")}" target="_blank" style="float:right;font-size:0.85rem;text-decoration:none;">🔗</a>' if item.get('url') else ''}
  {score_badge_html(item.get('relevance_score'))} &nbsp; {tier}
  <div class="review-meta">{item.get('authors','')[:60]} · {item.get('year','')} · {item.get('venue','')[:40]}</div>
  <div style="margin-top:6px">{mols_html}</div>
  <div style="margin-top:4px">{claims_html}</div>
</div>
                """, unsafe_allow_html=True)

                btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 4])
                with btn_col1:
                    if st.button("✅ Approve", key=f"approve_{idx}"):
                        approved = st.session_state.review_queue.pop(idx)
                        st.session_state.approved_items.append(approved)
                        st.rerun()
                with btn_col2:
                    if st.button("❌ Reject", key=f"reject_{idx}"):
                        rejected = st.session_state.review_queue.pop(idx)
                        st.session_state.rejected_items.append(rejected)
                        st.rerun()

        # Push approved to Airtable
        if st.session_state.approved_items:
            st.divider()
            st.markdown("#### ✅ Approved Items")
            st.markdown(f"**{len(st.session_state.approved_items)}** items approved and ready to push to Airtable.")

            for item in st.session_state.approved_items[:10]:
                tier = item.get("tier", "")
                st.markdown(f"""
<div class="session-card">
  ✅ <strong>{item['title'][:80]}</strong> — {tier} ({item.get('relevance_score',0):.0%})
</div>
                """, unsafe_allow_html=True)

            if len(st.session_state.approved_items) > 10:
                st.caption(f"...and {len(st.session_state.approved_items) - 10} more")

            if AT_TOKEN and AT_BASE_ID:
                if st.button("📤 Push Approved to Airtable", type="primary", use_container_width=True):
                    fields_list = []
                    for item in st.session_state.approved_items:
                        year_val = item.get("year")
                        if year_val is not None:
                            try:
                                year_val = int(year_val)
                            except (ValueError, TypeError):
                                year_val = None
                        record = {
                            "Name": item.get("title", "")[:200],
                            "URL": item.get("url", ""),
                            "Venue": item.get("venue", ""),
                            "query": item.get("branch", ""),
                        }
                        if year_val is not None:
                            record["Year"] = year_val
                        fields_list.append(record)
                    successes = push_to_airtable(SOURCES_TABLE, fields_list)
                    if successes > 0:
                        st.success(f"✅ Successfully pushed {successes} sources to Airtable!")
                        st.session_state.approved_items = []
                        st.cache_data.clear()
                    else:
                        st.error("Failed to push to Airtable. Check your API token and base ID.")
            else:
                st.warning("Connect Airtable (add secrets) to push approved items.")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — SOURCES & RELEVANCE
# ═══════════════════════════════════════════════════════════════════════════════

with tab_sources:
    st.header("📚 Sources — Relevance Filtration")
    st.caption(
        "3-tier classification: **1st Level** (High Relevance, ≥80%) · "
        "**2nd Level** (Mid Relevance, 60-80%) · **3rd Level** (Somewhat Relevant, <60%)"
    )

    all_sources = []

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

        fc1, fc2 = st.columns(2)
        with fc1:
            tier_filter = st.multiselect(
                "Filter by Relevance Level",
                ["1st Level — High Relevance", "2nd Level — Mid Relevance", "3rd Level — Somewhat Relevant"],
                default=["1st Level — High Relevance", "2nd Level — Mid Relevance", "3rd Level — Somewhat Relevant"],
            )
        with fc2:
            source_filter = st.multiselect(
                "Data Source",
                df["source"].unique().tolist(),
                default=df["source"].unique().tolist(),
            )

        filtered = df[df["tier"].isin(tier_filter) & df["source"].isin(source_filter)]

        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Total Sources", len(filtered))
        ct_1st = len(filtered[filtered["tier"] == "1st Level — High Relevance"])
        ct_2nd = len(filtered[filtered["tier"] == "2nd Level — Mid Relevance"])
        ct_3rd = len(filtered[filtered["tier"] == "3rd Level — Somewhat Relevant"])
        s2.markdown(f"<span class='tier-1st'>{ct_1st}</span> 1st Level", unsafe_allow_html=True)
        s3.markdown(f"<span class='tier-2nd'>{ct_2nd}</span> 2nd Level", unsafe_allow_html=True)
        s4.markdown(f"<span class='tier-3rd'>{ct_3rd}</span> 3rd Level", unsafe_allow_html=True)

        ch1, ch2 = st.columns(2)
        with ch1:
            tier_df = filtered["tier"].value_counts().reset_index()
            tier_df.columns = ["Tier", "Count"]
            fig = px.pie(tier_df, values="Count", names="Tier",
                         color="Tier", color_discrete_map=TIER_COLORS, hole=0.4)
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              font_color="#1a1a2e", height=300)
            st.plotly_chart(fig, use_container_width=True)

        with ch2:
            if "branch" in filtered.columns and filtered["branch"].notna().any():
                branch_df = filtered.groupby(["branch", "tier"]).size().reset_index(name="count")
                fig = px.bar(branch_df, x="branch", y="count", color="tier",
                             color_discrete_map=TIER_COLORS, barmode="group")
                fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                                  font_color="#1a1a2e", height=300,
                                  xaxis_title="Branch", yaxis_title="Count")
                st.plotly_chart(fig, use_container_width=True)

        display_cols = ["title", "year", "branch", "relevance_score", "tier", "venue", "source"]
        avail = [c for c in display_cols if c in filtered.columns]
        st.dataframe(
            filtered[avail].sort_values("relevance_score", ascending=False),
            use_container_width=True, height=400,
            column_config={
                "relevance_score": st.column_config.ProgressColumn(
                    "Relevance", min_value=0, max_value=1, format="%.0%%"
                ),
            }
        )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — LAST MOLECULES ADDED (by category)
# ═══════════════════════════════════════════════════════════════════════════════

with tab_molecules:
    st.header("🧬 Last Molecules Added")
    st.caption("Recently discovered molecules grouped by chemical category")

    # Gather session molecules (most recent first)
    session_mols = list(st.session_state.molecules_found.values())

    if not session_mols and not molecules_raw:
        st.info("No molecules yet. Run the pipeline to extract compounds.")
    else:
        # Show session molecules grouped by primary chemistry category
        if session_mols:
            st.markdown("### 🆕 Discovered This Session")

            m1, m2 = st.columns(2)
            m1.metric("New Molecules", len(session_mols))
            unique_cats = set(mol.get("primary", get_primary_category(mol.get("type", "other"))) for mol in session_mols)
            m2.metric("Categories Found", len(unique_cats))

            # Group by primary category
            by_primary = {}
            for mol in session_mols:
                cat = mol.get("primary", get_primary_category(mol.get("type", "other")))
                if cat not in by_primary:
                    by_primary[cat] = []
                by_primary[cat].append(mol)

            cat_icons = {
                "Amino Acids": "🧬", "Fats": "🫧", "Sugars": "🍬",
                "Proteins": "🥩", "Alcohols": "🧪",
                "Aldehydes": "⚗️", "Ketones": "🔬", "Furanones": "🍮",
                "Pyrazines": "🔥", "Nucleotides": "🧫", "Vitamins": "💊",
                "Other": "📦",
            }

            for cat in CATEGORY_DISPLAY_ORDER:
                if cat not in by_primary:
                    continue
                mols = by_primary[cat]
                icon = cat_icons.get(cat, "⚗️")
                st.markdown(f'<div class="mol-category-header">{icon} {cat.upper()} ({len(mols)})</div>',
                            unsafe_allow_html=True)
                for mol in mols:
                    sensory = mol.get("sensory", "")
                    role = mol.get("role", "")
                    conf = mol.get("confidence", 0)
                    kw = mol.get("from_keyword", "")
                    subtype = mol.get("type", "")

                    sensory_text = f" — {sensory}" if sensory else ""
                    subtype_text = f" <span style='color:#888; font-size:0.75rem'>({subtype})</span>" if subtype else ""
                    st.markdown(f"""
<div style="background:#f8f9fb; border:1px solid #dde1e8; border-radius:8px;
            padding:10px 14px; margin-bottom:6px; border-left:3px solid #1a8a4a;">
  <strong style="color:#15713e">{mol['name']}</strong>{subtype_text}
  <span style="color:#555; font-size:0.85rem;">{sensory_text}</span>
  <div style="color:#666; font-size:0.8rem; margin-top:3px;">
    Role: <strong style="color:#555">{role}</strong> &nbsp;&middot;&nbsp;
    Confidence: <strong style="color:#555">{conf:.0%}</strong>
    {f' &nbsp;&middot;&nbsp; From: <em>{kw[:30]}</em>' if kw else ''}
  </div>
</div>
                    """, unsafe_allow_html=True)

        # Show Airtable molecules summary
        if molecules_raw:
            st.divider()
            st.markdown("### 📦 In Airtable Database")
            st.metric("Total in Airtable", len(molecules_raw))


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 — CLAIMS FEED
# ═══════════════════════════════════════════════════════════════════════════════

with tab_claims:
    st.header("📋 Extracted Claims")
    st.caption("Scientific claims extracted from processed papers")

    claims = st.session_state.claims_extracted

    if claims:
        st.markdown("### 🆕 This Session")
        st.metric("Claims This Run", len(claims))
        for i, claim in enumerate(claims):
            st.markdown(f"""
<div class="claim-chip" style="display:block; margin-bottom:8px;">
  <strong style="color:#1a5096">Claim {i+1}:</strong>
  <span class="claim-text" style="color:#1a1a2e">{claim}</span>
</div>
            """, unsafe_allow_html=True)

    # Show previous claims from Airtable
    if AT_TOKEN and AT_BASE_ID:
        try:
            r = requests.get(f"{AT_BASE}/{quote('Claims')}",
                             headers=AT_HEADERS, params={"maxRecords": 50}, timeout=10)
            if r.ok:
                at_claims = r.json().get("records", [])
                if at_claims:
                    st.divider()
                    st.markdown("### 📦 Previous Claims (from Airtable)")
                    st.metric("Claims in Database", len(at_claims))
                    for j, rec in enumerate(at_claims):
                        f = rec.get("fields", {})
                        claim_text = f.get("claim_text", "")
                        stance = f.get("stance", "")
                        conf = f.get("confidence", "")
                        stance_color = {"supports": "#1a8a4a", "contradicts": "#c0392b", "neutral": "#666"}.get(
                            str(stance).lower(), "#666")
                        conf_text = f" · Confidence: {conf}" if conf else ""
                        st.markdown(f"""
<div class="claim-chip" style="display:block; margin-bottom:8px;">
  <strong style="color:#1a5096">{j+1}.</strong>
  <span class="claim-text" style="color:#1a1a2e">{claim_text[:200]}</span>
  <span style="color:{stance_color}; font-size:0.8rem; font-weight:600"> {stance}{conf_text}</span>
</div>
                        """, unsafe_allow_html=True)
        except Exception:
            pass

    if not claims and not (AT_TOKEN and AT_BASE_ID):
        st.info("No claims extracted yet. Run the pipeline to extract scientific claims from papers.")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 6 — KEYWORDS BRAIN (with public keyword submission)
# ═══════════════════════════════════════════════════════════════════════════════

with tab_keywords:
    st.header("🧠 Keywords Brain")
    st.caption("The keyword bible that drives the pipeline — anyone can contribute new keywords")

    # ---- PUBLIC KEYWORD SUBMISSION ----
    st.markdown("#### ➕ Suggest a New Keyword")
    st.markdown(f"Submitting as **{st.session_state.user_name}** ({st.session_state.user_email})")

    with st.form("keyword_submission", clear_on_submit=True):
        kw_col1, kw_col2 = st.columns([3, 1])
        with kw_col1:
            new_keyword = st.text_input("Keyword phrase", placeholder="e.g., vanillin formation lignin degradation")
        with kw_col2:
            new_branch = st.selectbox("Branch", MOCK_BRANCHES + ["Other"])

        new_priority = st.selectbox("Priority suggestion", ["MEDIUM", "HIGH", "LOW"])

        submitted = st.form_submit_button("🚀 Submit Keyword", type="primary", use_container_width=True)

        if submitted and new_keyword.strip():
            new_entry = {
                "keyword": new_keyword.strip(),
                "branch": new_branch,
                "priority": new_priority,
                "added_by": st.session_state.user_name,
                "added_email": st.session_state.user_email,
                "added_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
            }

            # Check for duplicates
            existing_kws = [kw["keyword"].lower() for kw in KEYWORDS_BIBLE]
            if new_keyword.strip().lower() in existing_kws:
                st.warning("This keyword already exists in the bible.")
            else:
                KEYWORDS_BIBLE.append(new_entry)
                save_keywords_to_file(KEYWORDS_BIBLE)
                st.session_state.user_keywords.append(new_entry)
                st.success(f"✅ Keyword added by {st.session_state.user_name}! It will be used in the next pipeline run.")
                st.rerun()
        elif submitted:
            st.warning("Please enter a keyword phrase.")

    st.divider()

    # ---- KEYWORD BIBLE ----
    st.markdown("#### 📖 Keyword Bible")

    # Separate system vs user-added
    system_kws = [kw for kw in KEYWORDS_BIBLE if kw.get("added_by", "system") == "system"]
    user_kws = [kw for kw in KEYWORDS_BIBLE if kw.get("added_by", "system") != "system"]

    if user_kws:
        st.markdown(f"**👥 Community Contributed ({len(user_kws)})**")
        for kw in user_kws:
            used = kw["keyword"] in st.session_state.keywords_used
            priority_color = {"HIGH": "#2ecc71", "MEDIUM": "#f39c12", "LOW": "#a0a4b0"}.get(kw.get("priority", ""), "#a0a4b0")
            status_icon = "✅" if used else "⬜"
            author = kw.get("added_by", "unknown")
            added_date = kw.get("added_date", "")
            st.markdown(f"""
<div class="kw-attributed" style="display:block; margin-bottom:6px;
     {'border-color: rgba(46,204,113,0.5); background: rgba(46,204,113,0.08);' if used else ''}">
  {status_icon} &nbsp;
  <strong>{kw['keyword']}</strong>
  &nbsp;→&nbsp; {kw.get('branch','')}
  &nbsp;
  <span style="color:{priority_color}; font-size:0.75rem">● {kw.get('priority','')}</span>
  <span class="kw-author">&nbsp;— added by <strong>{author}</strong> {added_date}</span>
</div>
            """, unsafe_allow_html=True)
        st.divider()

    st.markdown(f"**⚙️ System Keywords ({len(system_kws)})**")
    for kw in system_kws:
        used = kw["keyword"] in st.session_state.keywords_used
        priority_color = {"HIGH": "#2ecc71", "MEDIUM": "#f39c12", "LOW": "#a0a4b0"}.get(kw.get("priority", ""), "#a0a4b0")
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

    # Terms discovered this session
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


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 7 — COMMUNITY (Leaderboard, Flags, Feedback)
# ═══════════════════════════════════════════════════════════════════════════════

with tab_community:
    st.header("🏆 Community Hub")
    st.caption(
        "Every user makes the database smarter. "
        "See who's contributing the most and what the community is discovering."
    )

    # ---- LEADERBOARD ----
    st.markdown("#### Contributor Leaderboard")

    stats = st.session_state.contributor_stats
    if not stats:
        st.info("No contributions yet this session. Run the pipeline, flag interesting papers, or add keywords to appear here!")
    else:
        # Build leaderboard data
        leaderboard = []
        for email, data in stats.items():
            score = data.get("runs", 0) * 3 + data.get("flags", 0) * 2 + data.get("keywords", 0) * 1
            leaderboard.append({
                "name": data.get("name", "Anonymous"),
                "runs": data.get("runs", 0),
                "flags": data.get("flags", 0),
                "keywords": data.get("keywords", 0),
                "score": score,
            })
        leaderboard.sort(key=lambda x: -x["score"])

        position_labels = ["#1", "#2", "#3"]
        for i, entry in enumerate(leaderboard[:10]):
            pos_label = position_labels[i] if i < 3 else f"#{i+1}"
            st.markdown(f"""
<div class="session-card" style="display:flex; align-items:center; justify-content:space-between;">
  <div>
    <span style="font-size:1.4rem">{pos_label}</span> &nbsp;
    <strong style="color:#1a1a2e; font-size:1.1rem">{entry['name']}</strong>
  </div>
  <div style="text-align:right; color:#555; font-size:0.85rem;">
    🚀 {entry['runs']} runs &nbsp;&middot;&nbsp;
    🚩 {entry['flags']} flags &nbsp;&middot;&nbsp;
    🔑 {entry['keywords']} keywords &nbsp;&middot;&nbsp;
    <strong style="color:#f39c12">Score: {entry['score']}</strong>
  </div>
</div>
            """, unsafe_allow_html=True)

    st.divider()

    # ---- FLAGGED ITEMS ----
    st.markdown("#### 🚩 Flagged for Daniel's Attention")

    flagged = st.session_state.flagged_items
    if not flagged:
        st.info("No items flagged yet. Use the 🚩 button on pipeline results to flag interesting findings for Daniel.")
    else:
        st.metric("Total Flagged", len(flagged))
        for item in flagged[:15]:
            tier = relevance_tier(item.get("relevance_score", 0))
            st.markdown(f"""
<div class="review-card" style="border-left: 4px solid #f39c12;">
  🚩 <strong>{item.get('title','')[:80]}</strong>
  {score_badge_html(item.get('relevance_score'))} &nbsp; {tier}
  <div class="review-meta">
    Flagged by <strong>{item.get('flagged_by','')}</strong> &middot;
    {item.get('flagged_at','')[:16]}
  </div>
</div>
            """, unsafe_allow_html=True)

    st.divider()

    # ---- RUN FEEDBACK ----
    st.markdown("#### 💬 Run Feedback from the Team")

    feedback = st.session_state.run_feedback
    if not feedback:
        st.info("No feedback submitted yet. After a pipeline run, users can share what they learned.")
    else:
        for fb in reversed(feedback[:10]):
            rating_color = {
                "Excellent": "#2ecc71", "Very useful": "#27ae60",
                "Useful": "#f39c12", "Somewhat": "#e67e22",
                "Not useful": "#e74c3c",
            }.get(fb.get("rating", ""), "#f39c12")
            st.markdown(f"""
<div class="session-card">
  <div style="display:flex; justify-content:space-between; align-items:center;">
    <strong style="color:#1a1a2e">{fb.get('user','')}</strong>
    <span style="color:{rating_color}; font-weight:600">{fb.get('rating','')}</span>
  </div>
  <div style="color:#555; margin-top:6px;">{fb.get('learned','')}</div>
  <div style="color:#888; font-size:0.75rem; margin-top:4px;">
    {fb.get('date','')} &middot; {fb.get('papers_processed',0)} papers &middot;
    Keywords: {', '.join(fb.get('keywords',[])[:3])}
  </div>
</div>
            """, unsafe_allow_html=True)
