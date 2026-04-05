# Flavor Intelligence Pipeline

A parallel, cursor-aware research pipeline that crawls multiple academic
databases, extracts flavor-chemistry knowledge via Claude API, computes
statistical co-occurrence connections, and pushes everything to Airtable.

---

## Files

```
pipeline.py          — main orchestrator (run this)
keywords_bible.json  — the "brain": all search keywords + branches + priorities
cursor_state.json    — auto-generated: tracks which pages/DOIs were already seen
pipeline_output.json — auto-generated: full extraction dump after each run
```

---

## Setup

### 1. Install dependencies
```bash
pip install requests python-dotenv
```

### 2. Create .env file (same folder as pipeline.py)
```env
AIRTABLE_TOKEN=pat_...
AIRTABLE_BASE_ID=app...
ANTHROPIC_API_KEY=sk-ant-...

# Optional overrides
SOURCES_TABLE=Sources
MOLECULES_TABLE=Molecules
CONNECTIONS_TABLE=Connections
PIPELINE_WORKERS=4
PER_PAGE=25
MAX_PAGES=8
```

### 3. Airtable tables needed

**Sources** table fields:
- Name (text)
- Year (number)
- Venue (text)
- URL (text)
- Authors (text)
- citation_count (number)
- query (text)
- branch (text)
- relevance_score (number)
- db_source (text)

**Molecules** table fields:
- molecule_key (text) ← unique identifier
- Name (text)
- type (text)
- role (text)
- smell (text)
- threshold_ppb (number)
- Sources (link to Sources table)

**Connections** table fields:
- entity_a (text)
- entity_b (text)
- cooccurrence (number)
- jaccard (number)
- strength (text: strong/medium/weak)

---

## Usage

```bash
# Dry run — no writes to Airtable, shows what would happen
python pipeline.py --dry-run

# Full run — all 3 databases, 4 parallel workers
python pipeline.py

# Only OpenAlex, 6 workers
python pipeline.py --databases openalex --workers 6

# Only HIGH priority keywords, OpenAlex only (fastest)
# Edit keywords_bible.json and set unwanted keywords to "priority": "LOW"
python pipeline.py --databases openalex

# Custom keywords file
python pipeline.py --keywords my_custom_keywords.json

# Save output to specific file
python pipeline.py --output run_2026_03.json
```

---

## How the cursor system works

`cursor_state.json` stores two things:

1. **`seen_dois`** — a set of every DOI/paper ID ever processed.
   On the next run, any paper whose ID is in this set is immediately skipped
   — no API call, no extraction, no Airtable check. This is O(1) per paper.

2. **`cursors`** — a dict mapping `"keyword::database"` → the next-page token.
   - OpenAlex uses cursor-based pagination (opaque token, not page numbers).
     Each run resumes exactly where the last run ended.
   - Semantic Scholar uses offset integers.
   - PubMed uses retstart offset integers.

This means:
- Run 1: pages 1–8 of "Maillard reaction meat" on OpenAlex → cursor saved at page 9
- Run 2: starts at page 9 — never re-checks pages 1–8
- If you add a new keyword: starts from page 1 for that keyword only

To reset a specific keyword (re-scan from scratch):
```python
import json
with open("cursor_state.json") as f: s = json.load(f)
del s["cursors"]["Maillard reaction meat flavor::openalex"]
with open("cursor_state.json","w") as f: json.dump(s, f)
```

To reset everything:
```bash
rm cursor_state.json
```

---

## How the similarity graph works

After all extractions, the pipeline builds a co-occurrence graph:

1. For every extracted record, it collects:
   - molecule names
   - biological processes mentioned
   - entity pairs from the `connections` field Claude returns

2. Builds an inverted index: `entity → set of paper IDs where it appears`

3. For every pair (A, B): computes **Jaccard similarity** = |A∩B| / |A∪B|
   - Strong: Jaccard ≥ 0.40
   - Medium: Jaccard ≥ 0.25
   - Weak:   Jaccard ≥ 0.12 (minimum threshold)

4. Only pairs appearing in ≥ 2 papers and with Jaccard ≥ 0.12 are kept.

5. Top 200 connections are pushed to the Connections table in Airtable.

---

## The keywords_bible.json format

```json
[
  {
    "keyword":       "exact search string sent to databases",
    "branch":        "Maillard Reaction | Lipid Oxidation | Volatile Compounds | ...",
    "priority":      "HIGH | MEDIUM | LOW",
    "query_variant": "optional alternative query (not currently used but useful for logging)"
  }
]
```

**Priority rules:**
- HIGH → crawled on all 3 databases
- MEDIUM → crawled on all 3 databases
- LOW → crawled on OpenAlex only (saves API quota)

To add keywords: just add entries to keywords_bible.json.
The pipeline picks them up automatically on the next run.

---

## Recommendations implemented from your request

| Your requirement                                      | Implementation                                              |
|-------------------------------------------------------|-------------------------------------------------------------|
| Never re-visit same pages                             | cursor_state.json with cursor/offset per (keyword, db)      |
| Never re-process same paper                           | seen_dois set — O(1) skip before any API call               |
| Multiple databases simultaneously                     | ThreadPoolExecutor — one worker per (keyword × db) pair     |
| Keyword file as "bible and brain"                     | keywords_bible.json — single source of truth                |
| Claude API extracts structured data                   | claude_extract() → JSON schema with molecules, processes    |
| Statistical similarity/connections                    | Jaccard co-occurrence graph across all records              |
| Dump to JSON                                          | pipeline_output.json written after every run                |
| Push to Airtable simultaneously                       | Sources + Molecules in parallel; Connections batched        |
| Dedup before writing                                  | Check by URL before inserting Sources; by molecule_key for Molecules |

---

## Suggested next steps

1. **Add Google Scholar** via SerpAPI (paid, ~$50/mo) for broader coverage
2. **Add CrossRef** API (free) for citation graph data
3. **Schedule** with cron: `0 3 * * * python /path/to/pipeline.py`
4. **Connect MCP**: since Airtable MCP is already configured in Claude.ai,
   you can ask Claude to query the populated tables directly in chat
5. **Extend keywords_bible.json** with the full Search Keywords tab from the Excel
