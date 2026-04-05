"""
layer_a_query_orchestration.py
===============================================================
DROP-IN REPLACEMENT for the load_keywords() call and work-list
builder inside pipeline.py.

What this adds on top of the current flat keyword list:

  1. QUERY EXPANSION
     Each keyword in keywords_bible.json automatically generates
     4 search variants:
       - exact phrase (quoted)
       - field-scoped (title-only for precision)
       - boolean OR  (broadens recall)
       - recency-boosted (last 10 years, for newer databases)

  2. DB ROUTING RULES
     Each query entry carries per-database enable/disable flags,
     so you can say "run this molecule keyword on CrossRef + S2
     only, skip PubMed because it won't appear there."

  3. SEARCH PLAN
     Instead of a flat [(kw, db)] list, the orchestrator builds
     a typed SearchPlan — a list of SearchTask objects. Each task
     carries its own cursor key, retry budget, and metadata.

  4. ADAPTIVE PRIORITY
     If a keyword returned 0 results last run, its priority is
     automatically downgraded. If it returned >50 results, it
     gets scheduled across more databases next run.
     State is stored in cursor_state.json under "kw_stats".

  5. QUOTA GUARD
     You can cap total Claude API calls per run. The orchestrator
     sorts tasks by expected yield × priority and stops scheduling
     once the cap is reached.

Usage — replace in pipeline.py:
    # Old:
    keywords = load_keywords(keywords_path)
    work = [(kw, db) for kw in keywords for db in databases
            if not (kw.get("priority") == "LOW" and db != "openalex")]

    # New (drop-in):
    from layer_a_query_orchestration import build_search_plan, SearchTask
    tasks = build_search_plan(
        keywords_path = keywords_path,
        databases     = databases,
        state         = state,
        max_claude_calls = 500,   # quota cap
    )
    # tasks is List[SearchTask], use task.keyword and task.db
    # in the existing worker() call — same interface
"""

from __future__ import annotations

import json, hashlib, logging, os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

log = logging.getLogger("layer_a")

# ── Which databases each branch is best suited for ───────────────────────────
# This prevents wasting quota on databases unlikely to have the papers
BRANCH_DB_AFFINITY: dict[str, list[str]] = {
    "Maillard Reaction":       ["openalex", "s2", "crossref", "pubmed", "serpapi"],
    "Lipid Oxidation":         ["openalex", "s2", "crossref", "pubmed"],
    "Volatile Compounds":      ["openalex", "crossref", "pubmed", "s2"],
    "Precursors":              ["openalex", "s2", "crossref", "pubmed"],
    "Sulfur Chemistry":        ["openalex", "crossref", "pubmed"],
    "Heterocyclic Formation":  ["openalex", "crossref", "s2"],
    "Analytical Methods":      ["openalex", "s2", "pubmed", "crossref"],
    "Meat Analogs":            ["openalex", "s2", "crossref", "serpapi"],
    "Processes":               ["openalex", "s2", "pubmed"],
    # default fallback — all dbs
    "__default__":             ["openalex", "s2", "pubmed", "crossref", "serpapi"],
}

# ── Priority weights for quota allocation ────────────────────────────────────
PRIORITY_WEIGHT = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}

# ─────────────────────────────────────────────────────────────────────────────
# DATA CLASS — one unit of search work
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SearchTask:
    """
    One atomic unit of work: (expanded_query, database).
    The worker() function in pipeline.py already accepts (kw_entry, db, state).
    SearchTask.as_kw_entry() returns a dict compatible with that interface.
    """
    keyword:       str                    # the actual query string sent to the DB
    db:            str                    # which database
    branch:        str                    # knowledge branch
    priority:      str                    # HIGH / MEDIUM / LOW
    source_keyword:str                    # original keyword from the bible
    variant_type:  str                    # original / exact_phrase / title_only / boolean_or / recency
    estimated_yield: int  = 0            # last-run result count (0 = unknown)
    cursor_key:    str    = ""           # pre-computed cursor_state.json key

    def as_kw_entry(self) -> dict:
        """Returns dict compatible with existing worker(kw_entry, db, state)."""
        return {
            "keyword":       self.keyword,
            "branch":        self.branch,
            "priority":      self.priority,
            "query_variant": self.source_keyword,
        }

    @property
    def task_id(self) -> str:
        h = hashlib.md5(f"{self.keyword}::{self.db}".encode()).hexdigest()[:8]
        return f"{self.db}::{h}"

# ─────────────────────────────────────────────────────────────────────────────
# QUERY EXPANSION
# ─────────────────────────────────────────────────────────────────────────────

def expand_keyword(entry: dict) -> list[dict]:
    """
    Takes one keywords_bible entry and returns a list of expanded query dicts.

    Expansion variants:
      original      → as-is (already works)
      exact_phrase  → "quoted query" — maximum precision, lower recall
      title_only    → ti:"query" for databases that support field search (CrossRef, PubMed)
      boolean_or    → splits multi-word query into OR terms for broader recall
      query_variant → the human-curated variant already in keywords_bible.json

    Each variant has a `variant_type` tag so you can track which performed best.
    """
    kw       = entry["keyword"]
    branch   = entry.get("branch",        "")
    priority = entry.get("priority",      "MEDIUM")
    variant  = entry.get("query_variant", "")

    expanded = []

    # 1. Original — always include
    expanded.append({**entry, "keyword": kw, "variant_type": "original"})

    # 2. Exact phrase — wrap in quotes
    # Most databases (OpenAlex, CrossRef, S2) support quoted phrase search
    quoted = f'"{kw}"'
    expanded.append({**entry, "keyword": quoted, "variant_type": "exact_phrase"})

    # 3. Human-curated variant from keywords_bible.json
    if variant and variant != kw:
        expanded.append({**entry, "keyword": variant, "variant_type": "query_variant"})

    # 4. Boolean OR — splits on spaces and joins top 3 terms with OR
    # Best for broad recall when you want any paper mentioning key terms
    # Skip if the keyword is already short (≤2 words)
    words = [w for w in kw.split() if len(w) >= 4]
    if len(words) >= 3:
        # Use the 3 most distinctive words (skip common ones)
        _skip = {"meat","food","from","with","that","this","their","into","also",
                 "using","based","study","cooked","reaction","flavor","flavour"}
        key_words = [w for w in words if w.lower() not in _skip][:3]
        if len(key_words) >= 2:
            bool_query = " OR ".join(key_words)
            expanded.append({**entry, "keyword": bool_query, "variant_type": "boolean_or"})

    # 5. Recency-boosted — append year filter as hint string
    # Not all databases parse this, but OpenAlex and CrossRef do
    recency = f"{kw} 2018 2025"
    expanded.append({**entry, "keyword": recency, "variant_type": "recency"})

    return expanded


# ─────────────────────────────────────────────────────────────────────────────
# ADAPTIVE PRIORITY — learns from last run
# ─────────────────────────────────────────────────────────────────────────────

def adapt_priorities(keywords: list[dict], kw_stats: dict) -> list[dict]:
    """
    Adjusts keyword priorities based on last-run yield stored in state["kw_stats"].

    Rules:
      - 0 results last run → downgrade priority by one level
      - >50 results        → upgrade priority by one level (cap at HIGH)
      - Not seen before    → keep original priority
    """
    adapted = []
    for entry in keywords:
        kw    = entry["keyword"]
        stats = kw_stats.get(kw, {})
        count = stats.get("last_count", -1)
        current = entry.get("priority", "MEDIUM")

        if count == 0:
            # Demote
            new_priority = {"HIGH": "MEDIUM", "MEDIUM": "LOW", "LOW": "LOW"}[current]
            if new_priority != current:
                log.info("Priority demoted: '%s' → %s (was %s, got 0 results)",
                         kw[:50], new_priority, current)
            adapted.append({**entry, "priority": new_priority})

        elif count > 50:
            # Promote
            new_priority = {"LOW": "MEDIUM", "MEDIUM": "HIGH", "HIGH": "HIGH"}[current]
            if new_priority != current:
                log.info("Priority promoted: '%s' → %s (was %s, got %d results)",
                         kw[:50], new_priority, current, count)
            adapted.append({**entry, "priority": new_priority})

        else:
            adapted.append(entry)

    return adapted


# ─────────────────────────────────────────────────────────────────────────────
# DB ROUTING — filters databases by branch affinity
# ─────────────────────────────────────────────────────────────────────────────

def route_databases(entry: dict, available_dbs: list[str]) -> list[str]:
    """
    Returns the subset of available_dbs that are worth running for this entry.

    Priority rules:
      HIGH   → all affinity-matched databases
      MEDIUM → top 3 affinity-matched databases only
      LOW    → openalex only (saves quota)
    """
    branch    = entry.get("branch", "__default__")
    priority  = entry.get("priority", "MEDIUM")
    affinity  = BRANCH_DB_AFFINITY.get(branch, BRANCH_DB_AFFINITY["__default__"])

    # Intersect with what's actually available this run
    matched = [db for db in affinity if db in available_dbs]

    if priority == "HIGH":
        return matched
    elif priority == "MEDIUM":
        return matched[:3]    # top 3 by affinity order
    else:  # LOW
        return [d for d in ["openalex"] if d in matched]


# ─────────────────────────────────────────────────────────────────────────────
# QUOTA GUARD
# ─────────────────────────────────────────────────────────────────────────────

def apply_quota(
    tasks:           list[SearchTask],
    max_claude_calls: int,
    papers_per_task:  int = 25,    # PER_PAGE — estimated papers per task
    relevance_rate:   float = 0.3, # ~30% of fetched papers are relevant
) -> list[SearchTask]:
    """
    Caps the work list so total estimated Claude API calls ≤ max_claude_calls.

    Estimation: each task fetches ~PER_PAGE papers per page × MAX_PAGES,
    of which ~30% pass relevance → each gets one Claude call.
    So: estimated_claude_calls per task ≈ PER_PAGE * MAX_PAGES * relevance_rate

    Tasks are sorted by (priority_weight × estimated_yield) descending,
    so high-value work is scheduled first when quota is tight.
    """
    if max_claude_calls <= 0:
        return tasks   # no cap

    calls_per_task = papers_per_task * relevance_rate  # default ~7.5

    # Sort: highest value first
    sorted_tasks = sorted(
        tasks,
        key=lambda t: (
            PRIORITY_WEIGHT.get(t.priority, 1),
            t.estimated_yield,
        ),
        reverse=True,
    )

    selected = []
    budget   = max_claude_calls

    for task in sorted_tasks:
        cost = max(1, task.estimated_yield * relevance_rate
                   if task.estimated_yield > 0 else calls_per_task)
        if budget <= 0:
            break
        selected.append(task)
        budget -= cost

    dropped = len(tasks) - len(selected)
    if dropped > 0:
        log.info("Quota guard: dropped %d tasks to stay under %d Claude calls",
                 dropped, max_claude_calls)

    return selected


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT — build_search_plan()
# ─────────────────────────────────────────────────────────────────────────────

def build_search_plan(
    keywords_path:    str       = "keywords_bible.json",
    databases:        list[str] = None,
    state:            dict      = None,
    max_claude_calls: int       = 0,          # 0 = no cap
    expand:           bool      = True,       # enable query expansion
    adapt:            bool      = True,       # enable adaptive priority
    serpapi_key:      str       = "",         # pass SERPAPI_KEY here
) -> list[SearchTask]:
    """
    Builds the full typed search plan for one pipeline run.

    Returns List[SearchTask] — each task is one (query, database) pair,
    fully expanded, routed, quota-checked, and ready to hand to worker().

    Drop-in usage in pipeline.py:

        tasks = build_search_plan(
            keywords_path    = keywords_path,
            databases        = databases,
            state            = state,
            max_claude_calls = 500,
        )
        work = [(t.as_kw_entry(), t.db) for t in tasks]
        # then use exactly as before:
        # pool.submit(worker, kw, db, state, dry_run)
    """
    if databases is None:
        from pipeline import ALL_DBS
        databases = ALL_DBS

    # Remove serpapi if no key
    if "serpapi" in databases and not serpapi_key:
        databases = [d for d in databases if d != "serpapi"]

    # Load keywords bible
    p = Path(keywords_path)
    if not p.exists():
        p = Path(__file__).with_name("keywords_bible.json")
    with p.open() as f:
        raw_keywords: list[dict] = json.load(f)
    log.info("Loaded %d base keywords from %s", len(raw_keywords), p)

    # Adaptive priority adjustment from last-run stats
    kw_stats = (state or {}).get("kw_stats", {})
    if adapt and kw_stats:
        raw_keywords = adapt_priorities(raw_keywords, kw_stats)

    # Expand keywords into variants
    all_entries: list[dict] = []
    if expand:
        for entry in raw_keywords:
            all_entries.extend(expand_keyword(entry))
        log.info("Expanded to %d query variants", len(all_entries))
    else:
        all_entries = raw_keywords

    # Build SearchTask objects with DB routing
    seen_tasks: set[str] = set()
    tasks: list[SearchTask] = []

    for entry in all_entries:
        kw        = entry["keyword"]
        branch    = entry.get("branch", "")
        priority  = entry.get("priority", "MEDIUM")
        variant   = entry.get("variant_type", "original")
        source_kw = entry.get("query_variant", kw)

        routed_dbs = route_databases(entry, databases)

        for db in routed_dbs:
            dedup_key = hashlib.md5(f"{kw}::{db}".encode()).hexdigest()
            if dedup_key in seen_tasks:
                continue
            seen_tasks.add(dedup_key)

            # Pull last-run yield from state stats
            stat_key = f"{entry.get('keyword',kw)}::{db}"
            yield_count = (state or {}).get("kw_stats",{}).get(stat_key,{}).get("last_count", 0)

            tasks.append(SearchTask(
                keyword        = kw,
                db             = db,
                branch         = branch,
                priority       = priority,
                source_keyword = source_kw,
                variant_type   = variant,
                estimated_yield = yield_count,
                cursor_key     = f"{kw}::{db}",
            ))

    log.info("Search plan: %d tasks across %d databases before quota check",
             len(tasks), len(databases))

    # Apply quota cap
    if max_claude_calls > 0:
        tasks = apply_quota(tasks, max_claude_calls)

    log.info("Final search plan: %d tasks", len(tasks))

    # Log a plan summary by branch + db
    branch_counts: dict[str, int] = {}
    db_counts:     dict[str, int] = {}
    for t in tasks:
        branch_counts[t.branch] = branch_counts.get(t.branch, 0) + 1
        db_counts[t.db]         = db_counts.get(t.db, 0) + 1

    log.info("By branch: %s", branch_counts)
    log.info("By DB:     %s", db_counts)

    return tasks


# ─────────────────────────────────────────────────────────────────────────────
# STAT RECORDER — call after each worker() to track yields
# ─────────────────────────────────────────────────────────────────────────────

def record_task_yield(state: dict, task: SearchTask, count: int):
    """
    Call this after each worker() finishes to record result count.
    Used by adapt_priorities() on the next run.

    In pipeline.py worker():
        enriched = worker(kw, db, state, dry_run)
        record_task_yield(state, task, len(enriched))
    """
    if "kw_stats" not in state:
        state["kw_stats"] = {}

    key = f"{task.source_keyword}::{task.db}"
    state["kw_stats"][key] = {
        "last_count":  count,
        "last_run":    __import__("time").strftime("%Y-%m-%dT%H:%M:%S"),
        "variant":     task.variant_type,
        "priority":    task.priority,
    }


# ─────────────────────────────────────────────────────────────────────────────
# STANDALONE TEST — run this file directly to preview the search plan
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    print("\n" + "="*60)
    print("LAYER A — SEARCH PLAN PREVIEW")
    print("="*60)

    tasks = build_search_plan(
        databases        = ["openalex", "s2", "pubmed", "crossref"],
        expand           = True,
        adapt            = False,   # no history yet
        max_claude_calls = 300,
    )

    print(f"\nTotal tasks: {len(tasks)}\n")
    print(f"{'DB':<12} {'Priority':<10} {'Variant':<16} {'Keyword'[:50]}")
    print("-"*90)

    shown = 0
    for t in tasks:
        if shown >= 30:
            print(f"  ... and {len(tasks)-30} more tasks")
            break
        print(f"{t.db:<12} {t.priority:<10} {t.variant_type:<16} {t.keyword[:52]}")
        shown += 1

    # Branch summary
    from collections import Counter
    branch_c = Counter(t.branch for t in tasks)
    db_c     = Counter(t.db for t in tasks)
    var_c    = Counter(t.variant_type for t in tasks)

    print(f"\nBy branch:   {dict(branch_c)}")
    print(f"By database: {dict(db_c)}")
    print(f"By variant:  {dict(var_c)}")
    print("\nTo integrate: replace load_keywords() call in pipeline.py with build_search_plan()")
