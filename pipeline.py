"""
FLAVOR INTELLIGENCE PIPELINE  v2
=============================================================
Databases:
  openalex   — cursor-based pagination (opaque token), free
  s2         — Semantic Scholar, offset-based, free
  pubmed     — NCBI E-utilities, offset-based, free
  crossref   — CrossRef REST API, 130M+ records, free
  serpapi    — Google Scholar via SerpAPI (requires SERPAPI_KEY)

Run:
    python pipeline.py                           # all 5 databases
    python pipeline.py --dry-run                 # no Airtable writes
    python pipeline.py --databases openalex s2   # subset
    python pipeline.py --workers 6
"""

import os, json, time, re, hashlib, logging, argparse, threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote
from collections import defaultdict

import requests
import anthropic
from dotenv import load_dotenv

# Always resolve .env relative to this file, not the caller's CWD.
# override=True ensures the file values win even if the shell has stale/empty env vars.
load_dotenv(Path(__file__).parent / ".env", override=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline")

# ── file logger so the dashboard can read live progress ──────────────────────
_fh = logging.FileHandler(Path(__file__).with_name("pipeline_run.log"), encoding="utf-8")
_fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S"))
logging.getLogger().addHandler(_fh)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
AIRTABLE_TOKEN    = os.environ["AIRTABLE_TOKEN"].strip()
BASE_ID           = os.environ["AIRTABLE_BASE_ID"].strip()
ANTHROPIC_KEY     = os.environ["ANTHROPIC_API_KEY"].strip()
SERPAPI_KEY       = os.environ.get("SERPAPI_KEY", "").strip()

SOURCES_TABLE     = os.environ.get("SOURCES_TABLE",     "Sources")
MOLECULES_TABLE   = os.environ.get("MOLECULES_TABLE",   "Molecules")
CONNECTIONS_TABLE = os.environ.get("CONNECTIONS_TABLE", "Connections")

CLAUDE_MODEL   = "claude-haiku-4-5-20251001"
MAX_TOKENS     = 1024
WORKERS        = int(os.environ.get("PIPELINE_WORKERS",  "2"))
PER_PAGE       = int(os.environ.get("PER_PAGE",          "25"))
MAX_PAGES      = int(os.environ.get("MAX_PAGES",          "8"))
MIN_JACCARD    = float(os.environ.get("MIN_JACCARD",    "0.12"))
MAX_CONNS      = int(os.environ.get("MAX_CONNECTIONS",  "300"))

STATE_FILE = Path(__file__).with_name("cursor_state.json")

AT_BASE    = f"https://api.airtable.com/v0/{BASE_ID}"
AT_HEADERS = {"Authorization": f"Bearer {AIRTABLE_TOKEN}",
              "Content-Type":  "application/json"}

_claude_client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)

# Limit concurrent Claude calls to avoid 429s across workers
_claude_sem   = threading.Semaphore(2)
# Guard shared state["seen_dois"] against concurrent writes
_seen_lock    = threading.Lock()

# ─────────────────────────────────────────────────────────────────────────────
# STATE
# ─────────────────────────────────────────────────────────────────────────────

def load_state():
    if STATE_FILE.exists():
        with STATE_FILE.open() as f:
            s = json.load(f)
        s["seen_dois"] = set(s.get("seen_dois", []))
        s.setdefault("stats", {})
        return s
    return {"seen_dois": set(), "cursors": {}, "stats": {}}

def save_state(state):
    out = dict(state)
    out["seen_dois"] = list(state["seen_dois"])
    out["last_run"]  = time.strftime("%Y-%m-%dT%H:%M:%S")
    with STATE_FILE.open("w") as f:
        json.dump(out, f, indent=2)
    log.info("State saved — %d seen DOIs", len(state["seen_dois"]))

def ck(keyword, db):
    return f"{keyword}::{db}"

# ─────────────────────────────────────────────────────────────────────────────
# KEYWORDS BIBLE
# ─────────────────────────────────────────────────────────────────────────────

def load_keywords(path="keywords_bible.json"):
    p = Path(path)
    if not p.exists():
        p = Path(__file__).with_name("keywords_bible.json")
    if p.exists():
        with p.open() as f:
            data = json.load(f)
        log.info("Loaded %d keywords from %s", len(data), p)
        return data
    log.warning("keywords_bible.json not found — using minimal fallback")
    return [
        {"keyword": "Maillard reaction meat flavor",       "branch": "Maillard Reaction", "priority": "HIGH"},
        {"keyword": "cysteine ribose model system meat",   "branch": "Precursors",        "priority": "HIGH"},
        {"keyword": "lipid oxidation beef phospholipid",   "branch": "Lipid Oxidation",   "priority": "HIGH"},
        {"keyword": "plant-based meat flavor analog",      "branch": "Meat Analogs",      "priority": "HIGH"},
        {"keyword": "2-methyl-3-furanthiol cooked meat",   "branch": "Volatile Compounds","priority": "HIGH"},
    ]

# ─────────────────────────────────────────────────────────────────────────────
# SHARED HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def reconstruct_abstract(inv):
    if not inv:
        return ""
    pos = {}
    for word, positions in inv.items():
        for p in positions:
            pos[p] = word
    return " ".join(pos[i] for i in range(max(pos) + 1)) if pos else ""

def sleep_db(db):
    time.sleep({"openalex":0.1,"s2":0.35,"pubmed":0.34,
                "crossref":0.2,"serpapi":1.0}.get(db, 0.2))

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE 1 — OpenAlex  (cursor-based)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_openalex(keyword, state, branch=""):
    db     = "openalex"
    key    = ck(keyword, db)
    cursor = state["cursors"].get(key, "*")
    papers = []
    pages  = 0

    while pages < MAX_PAGES:
        r = requests.get("https://api.openalex.org/works", params={
            "search":   keyword, "per-page": PER_PAGE, "cursor": cursor,
            "select":   "id,doi,title,publication_year,cited_by_count,"
                        "primary_location,authorships,abstract_inverted_index,concepts",
        }, timeout=30)
        if not r.ok:
            log.warning("OpenAlex HTTP %s", r.status_code); break

        data    = r.json()
        results = data.get("results", [])
        if not results:
            break

        for w in results:
            uid = (w.get("doi") or w.get("id") or "").strip()
            if "doi.org/" in uid:
                uid = uid.split("doi.org/")[-1]
            if uid in state["seen_dois"]:
                continue
            venue   = ((w.get("primary_location") or {}).get("source") or {})
            authors = [(a.get("author") or {}).get("display_name","")
                       for a in w.get("authorships", [])]
            papers.append({
                "id":       uid, "title": w.get("title",""),
                "year":     w.get("publication_year"),
                "venue":    venue.get("display_name",""),
                "authors":  [a for a in authors if a],
                "url":      w.get("doi") or w.get("id",""),
                "abstract": reconstruct_abstract(w.get("abstract_inverted_index")),
                "citations":w.get("cited_by_count", 0),
                "concepts": [c.get("display_name","") for c in w.get("concepts",[])[:5]],
                "branch": branch, "keyword": keyword, "db": db,
            })

        pages += 1
        cursor = data.get("meta",{}).get("next_cursor")
        if not cursor:
            state["cursors"].pop(key, None); break
        state["cursors"][key] = cursor
        sleep_db(db)

    log.info("OpenAlex [%s]: +%d", keyword[:40], len(papers))
    return papers

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE 2 — Semantic Scholar  (offset-based)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_semantic_scholar(keyword, state, branch=""):
    db     = "s2"
    key    = ck(keyword, db)
    offset = state["cursors"].get(key, 0)
    papers = []
    pages  = 0
    fields = "paperId,externalIds,title,year,citationCount,venue,authors,abstract,fieldsOfStudy"

    while pages < MAX_PAGES:
        r = requests.get(
            "https://api.semanticscholar.org/graph/v1/paper/search",
            params={"query": keyword, "fields": fields,
                    "limit": PER_PAGE, "offset": offset},
            timeout=30)
        if r.status_code == 429:
            time.sleep(10); continue
        if not r.ok:
            log.warning("S2 HTTP %s", r.status_code); break

        results = r.json().get("data", [])
        if not results:
            state["cursors"].pop(key, None); break

        for p in results:
            ext = p.get("externalIds") or {}
            uid = ext.get("DOI") or ext.get("ArXiv") or p.get("paperId","")
            if uid in state["seen_dois"]:
                continue
            papers.append({
                "id":       uid, "title": p.get("title",""),
                "year":     p.get("year"),
                "venue":    p.get("venue",""),
                "authors":  [a.get("name","") for a in (p.get("authors") or [])],
                "url":      (f"https://doi.org/{uid}" if str(uid).startswith("10.")
                             else f"https://www.semanticscholar.org/paper/{p.get('paperId','')}"),
                "abstract": p.get("abstract","") or "",
                "citations":p.get("citationCount", 0),
                "concepts": p.get("fieldsOfStudy") or [],
                "branch": branch, "keyword": keyword, "db": db,
            })

        offset += len(results)
        state["cursors"][key] = offset
        pages += 1
        sleep_db(db)

    log.info("S2 [%s]: +%d", keyword[:40], len(papers))
    return papers

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE 3 — PubMed  (NCBI E-utilities, offset-based)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_pubmed(keyword, state, branch=""):
    db    = "pubmed"
    key   = ck(keyword, db)
    start = state["cursors"].get(key, 0)

    r = requests.get(
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
        params={"db":"pubmed","term":keyword,"retmax":PER_PAGE*MAX_PAGES,
                "retstart":start,"retmode":"json"},
        timeout=30)
    if not r.ok:
        return []

    ids = r.json().get("esearchresult",{}).get("idlist",[])
    if not ids:
        state["cursors"].pop(key, None); return []

    r2 = requests.get(
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi",
        params={"db":"pubmed","id":",".join(ids),"retmode":"json"},
        timeout=30)
    summaries = r2.json().get("result",{}) if r2.ok else {}

    papers = []
    for pmid in ids:
        uid = f"pmid:{pmid}"
        if uid in state["seen_dois"]:
            continue
        s = summaries.get(pmid, {})
        yr = s.get("pubdate","")[:4]
        papers.append({
            "id": uid, "title": s.get("title",""),
            "year": int(yr) if yr.isdigit() else None,
            "venue": s.get("source",""),
            "authors": [a.get("name","") for a in s.get("authors",[])],
            "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
            "abstract": "", "citations": 0, "concepts": [],
            "branch": branch, "keyword": keyword, "db": db,
        })

    state["cursors"][key] = start + len(ids)
    log.info("PubMed [%s]: +%d", keyword[:40], len(papers))
    return papers

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE 4 — CrossRef  (free, 130M+ records, polite pool)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_crossref(keyword, state, branch=""):
    """
    CrossRef REST API — completely free, no key needed.
    Add CROSSREF_MAILTO=your@email.com to .env to join the polite pool
    (higher rate limits, faster responses).

    Excellent coverage of:
      - Food Chemistry (Elsevier)
      - Meat Science (Elsevier)
      - Journal of Agricultural and Food Chemistry (ACS)
      - Food Research International
      - LWT - Food Science and Technology
      - Journal of Food Science

    Pagination: offset + rows, max 10,000 results per query.
    """
    db     = "crossref"
    key    = ck(keyword, db)
    offset = state["cursors"].get(key, 0)
    papers = []
    pages  = 0

    mailto = os.environ.get("CROSSREF_MAILTO", "")
    ua     = f"FlavorPipeline/2.0 (mailto:{mailto})" if mailto else "FlavorPipeline/2.0"

    while pages < MAX_PAGES:
        r = requests.get(
            "https://api.crossref.org/works",
            params={
                "query":   keyword,
                "rows":    PER_PAGE,
                "offset":  offset,
                "select":  "DOI,title,published,container-title,author,"
                           "abstract,is-referenced-by-count,subject,type",
                "filter":  "type:journal-article",
                "sort":    "relevance",
                "order":   "desc",
            },
            headers={"User-Agent": ua},
            timeout=30)

        if r.status_code == 429:
            retry = int(r.headers.get("Retry-After", 10))
            log.warning("CrossRef rate-limit — sleeping %ds", retry)
            time.sleep(retry); continue
        if not r.ok:
            log.warning("CrossRef HTTP %s", r.status_code); break

        items = r.json().get("message",{}).get("items",[])
        if not items:
            state["cursors"].pop(key, None); break

        for item in items:
            doi = item.get("DOI","").strip()
            uid = doi or hashlib.md5(str(item).encode()).hexdigest()[:16]
            if uid in state["seen_dois"]:
                continue

            titles   = item.get("title") or []
            title    = titles[0] if titles else ""
            authors  = []
            for a in (item.get("author") or []):
                name = f"{a.get('given','')} {a.get('family','')}".strip()
                if name:
                    authors.append(name)

            pub  = item.get("published") or item.get("published-print") or {}
            dp   = pub.get("date-parts",[[None]])[0]
            year = dp[0] if dp else None

            jl    = item.get("container-title") or []
            venue = jl[0] if jl else ""

            # Strip JATS XML tags from CrossRef abstracts
            raw_abs  = item.get("abstract","") or ""
            abstract = re.sub(r"<[^>]+>", " ", raw_abs).strip()

            papers.append({
                "id":       doi or uid,
                "title":    title,
                "year":     year,
                "venue":    venue,
                "authors":  authors,
                "url":      f"https://doi.org/{doi}" if doi else "",
                "abstract": abstract,
                "citations":item.get("is-referenced-by-count", 0),
                "concepts": item.get("subject") or [],
                "branch":   branch, "keyword": keyword, "db": db,
            })

        offset += len(items)
        state["cursors"][key] = offset
        pages += 1
        sleep_db(db)

    log.info("CrossRef [%s]: +%d", keyword[:40], len(papers))
    return papers

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE 5 — SerpAPI / Google Scholar  (requires SERPAPI_KEY)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_serpapi(keyword, state, branch=""):
    """
    Google Scholar via SerpAPI.
    Requires SERPAPI_KEY in .env — free tier: 100 searches/month.
    Paid plans from $50/mo for 5,000 searches.

    Why worth it:
      - Covers conference papers, theses, preprints
      - Indexes 6-12 months ahead of OpenAlex for new papers
      - Better Asian food-science journal coverage
      - Grey literature and industry reports

    Google Scholar caps at ~100 results per query.
    SerpAPI uses start offset: 0, 10, 20...
    """
    if not SERPAPI_KEY:
        return []

    db    = "serpapi"
    key   = ck(keyword, db)
    start = state["cursors"].get(key, 0)

    if start >= 100:   # Google Scholar hard limit
        return []

    papers = []
    pages  = 0

    while pages < MAX_PAGES and start < 100:
        r = requests.get("https://serpapi.com/search", params={
            "engine":  "google_scholar",
            "q":       keyword,
            "start":   start,
            "num":     min(PER_PAGE, 10),   # Scholar max 10/page
            "api_key": SERPAPI_KEY,
            "as_ylo":  "2010",
        }, timeout=30)

        if r.status_code == 401:
            log.error("SerpAPI: invalid API key"); break
        if r.status_code == 429:
            time.sleep(30); continue
        if not r.ok:
            log.warning("SerpAPI HTTP %s", r.status_code); break

        results = r.json().get("organic_results", [])
        if not results:
            state["cursors"].pop(key, None); break

        for item in results:
            title = item.get("title","")
            uid   = hashlib.md5(title.lower().encode()).hexdigest()[:16]
            if uid in state["seen_dois"]:
                continue

            pub_info = item.get("publication_info", {})
            authors  = [a.get("name","") for a in (pub_info.get("authors") or []) if a.get("name")]
            summary  = pub_info.get("summary","")
            ym       = re.search(r"\b(20\d{2}|19\d{2})\b", summary)
            year     = int(ym.group(1)) if ym else None
            vm       = re.search(r"-\s*(.+?),?\s*\d{4}", summary)
            venue    = vm.group(1).strip() if vm else ""

            cited_by = ((item.get("inline_links") or {})
                        .get("cited_by",{}).get("total", 0) or 0)

            papers.append({
                "id":       uid,
                "title":    title,
                "year":     year,
                "venue":    venue,
                "authors":  authors,
                "url":      item.get("link",""),
                "abstract": item.get("snippet",""),
                "citations":cited_by,
                "concepts": [],
                "branch": branch, "keyword": keyword, "db": db,
            })

        start += len(results)
        state["cursors"][key] = start
        pages += 1
        sleep_db(db)

    log.info("SerpAPI [%s]: +%d", keyword[:40], len(papers))
    return papers

# ─────────────────────────────────────────────────────────────────────────────
# FETCHER REGISTRY
# ─────────────────────────────────────────────────────────────────────────────

FETCHERS = {
    "openalex": fetch_openalex,
    "s2":       fetch_semantic_scholar,
    "pubmed":   fetch_pubmed,
    "crossref": fetch_crossref,
    "serpapi":  fetch_serpapi,
}

ALL_DBS = list(FETCHERS.keys())

# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

EXTRACT_SYSTEM = """\
You are a precision flavor-chemistry extraction engine for a meat and Maillard
reaction research database. Extract ONLY information relevant to:
meat flavor, aroma compounds, Maillard reaction, lipid oxidation,
volatile organic compounds (VOCs), flavor precursors, amino acids in flavor
context, reducing sugars in flavor context, plant-based meat analogs,
and analytical methods for flavor.

Ignore: general nutrition, genetics unrelated to flavor, tenderness/texture
unless directly linked to flavor, dairy, fruit/vegetable flavor unless
compared to meat.

Return ONLY valid JSON, no markdown, no preamble, exactly this schema:
{
  "relevant": true,
  "relevance_score": 0.0-1.0,
  "branch": "Maillard Reaction|Lipid Oxidation|Volatile Compounds|Precursors|Sulfur Chemistry|Heterocyclic Formation|Analytical Methods|Meat Analogs|Other",
  "molecules": [
    {
      "name": "compound name",
      "type": "aldehyde|ketone|furan|thiazole|thiophene|pyrazine|thiol|acid|alcohol|peptide|other",
      "role": "precursor|product|marker|potentiator",
      "sensory": "brief aroma/taste description or empty string",
      "threshold_ppb": null
    }
  ],
  "biological_processes": ["list of processes mentioned"],
  "key_claims": ["max 3 one-sentence claims from abstract"],
  "connections": ["entity_a::entity_b pairs that meaningfully co-occur"]
}
If not relevant: {"relevant": false, "relevance_score": 0.0}
"""

def claude_extract(paper):
    title    = (paper.get("title","")    or "").strip()
    abstract = (paper.get("abstract","") or "").strip()
    if not title and not abstract:
        return {**paper, "relevant": False, "relevance_score": 0.0}

    content = f"Title: {title}\n\nAbstract: {abstract}" if abstract else f"Title: {title}"

    for attempt in range(3):
        try:
            with _claude_sem:
                msg = _claude_client.messages.create(
                    model=CLAUDE_MODEL,
                    max_tokens=MAX_TOKENS,
                    system=EXTRACT_SYSTEM,
                    messages=[{"role": "user", "content": content}],
                )
            raw = msg.content[0].text.strip()
            # Extract the outermost JSON object robustly
            start = raw.find("{")
            end   = raw.rfind("}")
            if start != -1 and end != -1 and end > start:
                raw = raw[start:end + 1]
            return {**paper, **json.loads(raw)}
        except anthropic.APIStatusError as e:
            log.warning("Claude attempt %d failed '%s': %s", attempt + 1, title[:40], e)
            if e.status_code in (400, 401, 403):
                break   # non-retryable
            time.sleep(2)
        except Exception as e:
            log.warning("Claude attempt %d failed '%s': %s", attempt + 1, title[:40], e)
            time.sleep(2)

    return {**paper, "relevant": False, "relevance_score": 0.0}


def test_claude_connection():
    """Quick sanity-check called at startup to verify API connectivity."""
    try:
        msg = _claude_client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=10,
            messages=[{"role": "user", "content": "ping"}],
        )
        log.info("Claude API connected — model=%s stop=%s", CLAUDE_MODEL, msg.stop_reason)
        return True
    except anthropic.AuthenticationError as e:
        log.error("Claude API auth failed — check ANTHROPIC_API_KEY: %s", e)
    except anthropic.BadRequestError as e:
        # Typically 'credit balance too low' — key is valid but account needs top-up
        log.error("Claude API rejected request: %s", e)
    except Exception as e:
        log.error("Claude API connectivity test failed: %s", e)
    return False

# ─────────────────────────────────────────────────────────────────────────────
# SIMILARITY GRAPH
# ─────────────────────────────────────────────────────────────────────────────

def build_similarity_graph(records):
    entity_papers = defaultdict(set)
    for rec in records:
        pid = rec.get("id","")
        for mol in (rec.get("molecules") or []):
            name = mol.get("name","").lower().strip()
            if len(name) > 2:
                entity_papers[name].add(pid)
        for conn in (rec.get("connections") or []):
            for part in conn.split("::"):
                part = part.lower().strip()
                if len(part) > 2:
                    entity_papers[part].add(pid)
        for proc in (rec.get("biological_processes") or []):
            entity_papers[proc.lower().strip()].add(pid)

    entities = [e for e,pids in entity_papers.items() if len(pids) >= 2]
    log.info("Graph: %d eligible entities", len(entities))

    connections = []
    for i, a in enumerate(entities):
        for b in entities[i+1:]:
            pa,pb  = entity_papers[a], entity_papers[b]
            inter  = len(pa & pb)
            if inter < 2: continue
            union  = len(pa | pb)
            j      = inter/union if union else 0
            if j < MIN_JACCARD: continue
            connections.append({
                "entity_a":     a, "entity_b": b,
                "cooccurrence": inter, "jaccard": round(j,4),
                "paper_ids":    list(pa & pb),
                "strength":     "strong" if j>=0.4 else "medium" if j>=0.25 else "weak",
            })

    connections.sort(key=lambda x: x["jaccard"], reverse=True)
    log.info("Graph: %d connections", len(connections))
    return connections

# ─────────────────────────────────────────────────────────────────────────────
# AIRTABLE WRITES
# ─────────────────────────────────────────────────────────────────────────────

def _at_find(table, formula):
    r = requests.get(f"{AT_BASE}/{quote(table)}", headers=AT_HEADERS,
                     params={"filterByFormula": formula, "maxRecords": 1},
                     timeout=20)
    return r.json().get("records",[]) if r.ok else []

def at_upsert_source(rec, dry_run):
    url_val = (rec.get("url") or "").strip()
    if url_val:
        safe     = url_val.replace("'","''")
        existing = _at_find(SOURCES_TABLE, f"{{URL}}='{safe}'")
        if existing:
            return existing[0]["id"]

    fields = {
        "Name":            (rec.get("title") or "Untitled")[:250],
        "Year":            rec.get("year"),
        "Venue":           (rec.get("venue") or "")[:250],
        "URL":             url_val,
        "Authors":         ", ".join(rec.get("authors",[]))[:500],
        "citation_count":  rec.get("citations",0),
        "query":           rec.get("keyword",""),
        "branch":          rec.get("branch",""),
        "relevance_score": rec.get("relevance_score",0.0),
        "db_source":       rec.get("db",""),
        "key_claims":      "\n".join(rec.get("key_claims") or [])[:1000],
    }
    fields = {k:v for k,v in fields.items() if v not in (None,"")}

    if dry_run:
        log.info("[DRY] Source: %s", fields.get("Name","")[:60])
        return f"dry_{hashlib.md5(url_val.encode()).hexdigest()[:8]}"

    r = requests.post(f"{AT_BASE}/{quote(SOURCES_TABLE)}", headers=AT_HEADERS,
                      json={"records":[{"fields":fields}]}, timeout=20)
    if not r.ok:
        log.warning("Source write failed: %s", r.text[:200]); return None
    return r.json()["records"][0]["id"]

def at_upsert_molecule(mol, source_id, dry_run):
    name = mol.get("name","").strip()
    if not name or len(name) < 3: return

    mol_key = "mol:" + re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")[:60]
    safe    = mol_key.replace("'","''")
    existing = _at_find(MOLECULES_TABLE, f"{{molecule_key}}='{safe}'")

    fields = {
        "molecule_key": mol_key, "Name": name[:200],
        "type": mol.get("type",""), "role": mol.get("role",""),
        "smell": (mol.get("sensory","") or "")[:500],
    }
    thr = mol.get("threshold_ppb")
    if thr is not None:
        try: fields["threshold_ppb"] = float(thr)
        except (ValueError, TypeError): pass
    fields = {k:v for k,v in fields.items() if v not in (None,"")}

    if dry_run:
        log.info("[DRY] Molecule: %s", name); return

    if existing:
        mol_id = existing[0]["id"]
        requests.patch(f"{AT_BASE}/{quote(MOLECULES_TABLE)}/{mol_id}",
                       headers=AT_HEADERS, json={"fields":fields}, timeout=20)
    else:
        r = requests.post(f"{AT_BASE}/{quote(MOLECULES_TABLE)}", headers=AT_HEADERS,
                          json={"records":[{"fields":fields}]}, timeout=20)
        if not r.ok:
            log.warning("Molecule write failed: %s", r.text[:200]); return
        mol_id = r.json()["records"][0]["id"]

    if source_id:
        rec = requests.get(f"{AT_BASE}/{quote(MOLECULES_TABLE)}/{mol_id}",
                           headers=AT_HEADERS, timeout=20)
        if rec.ok:
            links = rec.json().get("fields",{}).get("Sources",[]) or []
            if source_id not in links:
                links.append(source_id)
                requests.patch(f"{AT_BASE}/{quote(MOLECULES_TABLE)}/{mol_id}",
                               headers=AT_HEADERS, json={"fields":{"Sources":links}}, timeout=20)

def at_push_connections(connections, dry_run):
    if dry_run:
        log.info("[DRY] Would push %d connections", len(connections)); return

    records = [{"fields":{
        "entity_a": c["entity_a"][:200], "entity_b": c["entity_b"][:200],
        "cooccurrence": c["cooccurrence"], "jaccard": c["jaccard"],
        "strength": c["strength"],
    }} for c in connections]

    for i in range(0, len(records), 10):
        r = requests.post(f"{AT_BASE}/{quote(CONNECTIONS_TABLE)}", headers=AT_HEADERS,
                          json={"records": records[i:i+10]}, timeout=30)
        if not r.ok:
            log.warning("Connections batch failed: %s", r.text[:200])
        time.sleep(0.2)
    log.info("Pushed %d connections", len(records))

# ─────────────────────────────────────────────────────────────────────────────
# WORKER
# ─────────────────────────────────────────────────────────────────────────────

def worker(kw_entry, db, state, dry_run):
    keyword = kw_entry["keyword"]
    branch  = kw_entry.get("branch","")
    label   = f"{db}::{keyword[:35]}"
    log.info("▶ %s", label)

    raw      = FETCHERS[db](keyword, state, branch)
    enriched = []
    for paper in raw:
        extracted = claude_extract(paper)
        with _seen_lock:
            state["seen_dois"].add(paper["id"])
        if extracted.get("relevant"):
            enriched.append(extracted)
            log.info("  ✓ [%.2f] %s", extracted.get("relevance_score",0),
                     paper.get("title","")[:55])

    log.info("◀ %s → %d/%d relevant", label, len(enriched), len(raw))
    return enriched

# ─────────────────────────────────────────────────────────────────────────────
# MAIN ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

def run(keywords_path="keywords_bible.json", dry_run=False,
        workers=WORKERS, databases=None, output_json="pipeline_output.json"):

    if databases is None:
        databases = ALL_DBS
    if "serpapi" in databases and not SERPAPI_KEY:
        log.warning("SERPAPI_KEY not set — skipping Google Scholar")
        databases = [d for d in databases if d != "serpapi"]

    log.info("Testing Claude API connection...")
    test_claude_connection()

    state    = load_state()
    keywords = load_keywords(keywords_path)

    # LOW priority → OpenAlex only to save quota
    work = [(kw, db) for kw in keywords for db in databases
            if not (kw.get("priority") == "LOW" and db != "openalex")]

    log.info("="*60)
    log.info("PIPELINE START — %d work items | workers=%d | dbs=%s | dry=%s",
             len(work), workers, databases, dry_run)
    log.info("="*60)

    all_records = []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(worker, kw, db, state, dry_run): (kw["keyword"], db)
                   for kw, db in work}
        for fut in as_completed(futures):
            kw_name, db_name = futures[fut]
            try:
                all_records.extend(fut.result())
            except Exception as e:
                log.error("Worker FAILED %s::%s — %s", db_name, kw_name[:30], e)

    log.info("Step 1 complete — %d relevant records", len(all_records))

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(all_records, f, indent=2, default=str)
    log.info("JSON dump → %s", output_json)

    connections = build_similarity_graph(all_records)

    log.info("Pushing to Airtable (dry=%s)...", dry_run)

    def push_one(rec):
        src_id = at_upsert_source(rec, dry_run)
        for mol in (rec.get("molecules") or []):
            at_upsert_molecule(mol, src_id, dry_run)
            time.sleep(0.05)

    with ThreadPoolExecutor(max_workers=min(workers, 3)) as pool:
        list(pool.map(push_one, all_records))

    at_push_connections(connections[:MAX_CONNS], dry_run)

    state["stats"].update({"last_records": len(all_records),
                           "last_connections": len(connections)})
    save_state(state)

    log.info("="*60)
    log.info("PIPELINE COMPLETE — %d records | %d connections pushed",
             len(all_records), min(len(connections), MAX_CONNS))
    log.info("="*60)
    return all_records, connections


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flavor Intelligence Pipeline v2")
    parser.add_argument("--keywords",  default="keywords_bible.json")
    parser.add_argument("--dry-run",   action="store_true")
    parser.add_argument("--workers",   type=int, default=WORKERS)
    parser.add_argument("--databases", nargs="+", choices=ALL_DBS, default=ALL_DBS)
    parser.add_argument("--output",    default="pipeline_output.json")
    args = parser.parse_args()
    run(keywords_path=args.keywords, dry_run=args.dry_run,
        workers=args.workers, databases=args.databases, output_json=args.output)
