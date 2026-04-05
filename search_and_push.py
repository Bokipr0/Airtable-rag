import os
import re
import math
import requests
from urllib.parse import quote
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.environ["AIRTABLE_TOKEN"]
BASE_ID = os.environ["AIRTABLE_BASE_ID"]
SOURCES_TABLE = os.environ.get("SOURCES_TABLE", "Sources")

AIRTABLE_API_BASE = f"https://api.airtable.com/v0/{BASE_ID}"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}

# ── Field names (match your Airtable exactly) ──────────────────────────────
SRC_F_NAME     = os.environ.get("SRC_F_NAME", "Name")
SRC_F_URL      = os.environ.get("SRC_F_URL", "URL")
SRC_F_QUERY    = os.environ.get("SRC_F_QUERY", "query")
SRC_F_YEAR     = os.environ.get("SRC_F_YEAR", "Year")
SRC_F_VENUE    = os.environ.get("SRC_F_VENUE", "Venue")
SRC_F_AUTHORS  = os.environ.get("SRC_F_AUTHORS", "Authors")
SRC_F_CITES    = os.environ.get("SRC_F_CITES", "citation_count")
SRC_F_KEYWORDS = os.environ.get("SRC_F_KEYWORDS", "top_keywords")


# ── Airtable helpers ────────────────────────────────────────────────────────

def airtable_find_by_url(url_value: str) -> str | None:
    """
    Returns the record_id of an existing Source row whose URL matches url_value.
    WHY: Prevents duplicate Sources for the same DOI/paper.
    """
    if not url_value:
        return None
    safe = url_value.replace("'", "''")
    formula = f"{{{SRC_F_URL}}}='{safe}'"
    params = {"filterByFormula": formula, "maxRecords": 1}
    r = requests.get(
        f"{AIRTABLE_API_BASE}/{quote(SOURCES_TABLE)}",
        headers=HEADERS,
        params=params,
        timeout=30,
    )
    if not r.ok:
        print(f"  [WARN] airtable_find_by_url failed: {r.status_code} {r.text[:200]}")
        return None
    recs = r.json().get("records", [])
    return recs[0]["id"] if recs else None


def airtable_push_batch(records: list[dict]) -> int:
    """
    Pushes up to 10 records per request (Airtable limit).
    Returns the number of rows actually inserted.
    """
    url = f"{AIRTABLE_API_BASE}/{quote(SOURCES_TABLE)}"
    total = 0
    for i in range(0, len(records), 10):
        batch = records[i : i + 10]
        resp = requests.post(url, headers=HEADERS, json={"records": batch}, timeout=30)
        print(f"  AIRTABLE STATUS: {resp.status_code}")
        if not resp.ok:
            print(f"  AIRTABLE ERROR: {resp.text[:400]}")
            continue
        total += len(resp.json().get("records", []))
    return total


# ── OpenAlex ────────────────────────────────────────────────────────────────

def openalex_search(query: str, limit: int = 20) -> list[dict]:
    """
    Searches OpenAlex and returns a list of paper dicts.
    """
    params = {"search": query, "per-page": limit}
    r = requests.get("https://api.openalex.org/works", params=params, timeout=30)
    print(f"OPENALEX STATUS: {r.status_code}")
    if not r.ok:
        print(f"OPENALEX ERROR: {r.text[:300]}")
        return []

    papers = []
    for w in r.json().get("results", []):
        doi = w.get("doi")
        landing_url = doi if doi else w.get("id", "")

        authorships = w.get("authorships", [])
        authors = [
            (a.get("author") or {}).get("display_name", "")
            for a in authorships
        ]

        venue = ((w.get("primary_location") or {}).get("source") or {})

        papers.append({
            "title":         w.get("title") or "Untitled",
            "year":          w.get("publication_year"),
            "venue":         venue.get("display_name"),
            "authors":       [{"name": n} for n in authors if n],
            "url":           landing_url,
            "abstract":      "",   # fetched separately in mvp_molecules script
            "citationCount": w.get("cited_by_count", 0),
        })
    return papers


# ── Utility ─────────────────────────────────────────────────────────────────

def extract_keywords(text: str, max_keywords: int = 12) -> str:
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9\s\-]", " ", text.lower())
    stop = {
        "with", "that", "from", "this", "were", "have", "their", "into", "also",
        "using", "used", "such", "between", "based", "study", "paper", "results",
        "method", "methods", "analysis", "engine", "engines",
    }
    words = [w for w in text.split() if len(w) >= 4 and w not in stop]
    freq: dict[str, int] = {}
    for w in words:
        freq[w] = freq.get(w, 0) + 1
    top = sorted(freq.items(), key=lambda kv: kv[1], reverse=True)[:max_keywords]
    return ", ".join(w for w, _ in top)


def compute_relevance_score(paper: dict, query: str) -> float:
    title    = (paper.get("title") or "").lower()
    abstract = (paper.get("abstract") or "").lower()
    citations = paper.get("citationCount") or 0
    q_terms = [t for t in re.findall(r"[a-z0-9]+", query.lower()) if len(t) >= 3]
    hits = sum((t in title) + 0.5 * (t in abstract) for t in q_terms)
    return round(hits + math.log10(citations + 1), 3)


# ── Main entry point ─────────────────────────────────────────────────────────

def push_sources_to_airtable(papers: list[dict], query: str) -> int:
    """
    For each paper:
      1. Check if URL already exists in Airtable  →  skip if yes (prevents duplicates)
      2. Build the record fields
      3. Batch-insert new records only
    """
    new_records = []
    skipped = 0

    for p in papers:
        url_val = p.get("url") or ""

        # ── Step 1: dedup check ──────────────────────────────────────────
        existing_id = airtable_find_by_url(url_val)
        if existing_id:
            print(f"  SKIP (already exists): {p.get('title', '')[:60]}")
            skipped += 1
            continue

        # ── Step 2: build fields ─────────────────────────────────────────
        authors_str = ", ".join(
            a.get("name", "") for a in (p.get("authors") or []) if a.get("name")
        )
        keywords = extract_keywords(
            (p.get("title") or "") + " " + (p.get("abstract") or "")
        )

        fields: dict = {}
        if p.get("title"):          fields[SRC_F_NAME]     = p["title"]
        if p.get("year"):           fields[SRC_F_YEAR]     = p["year"]
        if p.get("venue"):          fields[SRC_F_VENUE]    = p["venue"]
        if url_val:                 fields[SRC_F_URL]      = url_val
        if authors_str:             fields[SRC_F_AUTHORS]  = authors_str
        if p.get("citationCount"):  fields[SRC_F_CITES]    = p["citationCount"]
        if query:                   fields[SRC_F_QUERY]    = query
        if keywords:                fields[SRC_F_KEYWORDS] = keywords

        new_records.append({"fields": fields})

    print(f"\nSummary: {len(new_records)} new, {skipped} skipped (already in Airtable)")

    if not new_records:
        return 0
    return airtable_push_batch(new_records)


def search_papers(query: str, limit: int = 20) -> list[dict]:
    return openalex_search(query, limit=limit)


# ── Run ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    query = os.environ.get("MVP_QUERY", "Flavor and Metabolite Profiles of Meat,")
    limit = int(os.environ.get("SEARCH_LIMIT", "20"))

    papers = search_papers(query, limit=limit)
    print(f"Found papers from OpenAlex: {len(papers)}")
    if papers:
        print(f"First title: {papers[0]['title']}")

    saved = push_sources_to_airtable(papers, query=query)
    print(f"Saved NEW sources to Airtable: {saved}")