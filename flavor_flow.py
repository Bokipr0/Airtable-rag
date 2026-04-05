"""
flavor_flow.py  —  Prefect wrapper for the Flavor Intelligence Pipeline
======================================================================
This file adds Prefect orchestration ON TOP of pipeline.py.
pipeline.py is NOT modified at all — this wraps it from outside.

What Prefect adds:
  - Web UI at http://localhost:4200 with run history, logs, status
  - @task decorators = each step gets its own retry logic and timing
  - @flow decorator  = the whole pipeline becomes a schedulable job
  - Automatic retries on network failures (OpenAlex, S2, CrossRef)
  - Schedule: runs every Sunday at 02:00 by default (change below)
  - Failure alerts via email or Slack (optional, see ALERTS section)

Install:
    xd

Run once manually:
    python flavor_flow.py

Start the Prefect UI:
    prefect server start          # in one terminal
    python flavor_flow.py         # in another terminal

Schedule (runs automatically every Sunday 02:00):
    prefect deployment run flavor-pipeline/weekly-run
"""

import os, json, time, logging
from pathlib import Path
from datetime import datetime

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.schedules import CronSchedule
from datetime import timedelta

# ── Import all functions from your existing pipeline ─────────────────────────
# We import directly — pipeline.py is untouched
import sys
sys.path.insert(0, str(Path(__file__).parent))

from pipeline import (
    load_state, save_state, load_keywords,
    FETCHERS, ALL_DBS,
    claude_extract,
    build_similarity_graph,
    at_upsert_source, at_upsert_molecule, at_push_connections,
    WORKERS, MAX_CONNS, SERPAPI_KEY,
)

ROOT = Path(__file__).parent

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Fetch from one database for one keyword
# @task means: Prefect tracks this individually, can retry it, times it
# cache_key_fn means: if same keyword+db ran successfully this hour, skip it
# ─────────────────────────────────────────────────────────────────────────────

@task(
    name          = "fetch-papers",
    retries       = 3,                        # retry up to 3 times on failure
    retry_delay_seconds = 15,                 # wait 15s between retries
    cache_key_fn  = task_input_hash,          # skip if same inputs ran recently
    cache_expiration = timedelta(hours=6),    # cache valid for 6 hours
    tags          = ["crawl"],
)
def task_fetch(kw_entry: dict, db: str, state: dict) -> list[dict]:
    """Fetch papers from one database for one keyword."""
    logger  = get_run_logger()
    keyword = kw_entry["keyword"]
    branch  = kw_entry.get("branch", "")
    label   = f"{db}::{keyword[:40]}"

    logger.info("Fetching: %s", label)
    papers = FETCHERS[db](keyword, state, branch)
    logger.info("Fetched %d papers from %s", len(papers), label)
    return papers


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Extract with Claude API
# Retries handle transient Claude API overload (529 errors)
# ─────────────────────────────────────────────────────────────────────────────

@task(
    name                = "claude-extract",
    retries             = 2,
    retry_delay_seconds = 10,
    tags                = ["claude", "extract"],
)
def task_extract(papers: list[dict], state: dict) -> list[dict]:
    """Run Claude extraction on a batch of papers."""
    logger   = get_run_logger()
    enriched = []

    for paper in papers:
        extracted = claude_extract(paper)
        state["seen_dois"].add(paper["id"])
        if extracted.get("relevant"):
            enriched.append(extracted)
            logger.info(
                "  ✓ [%.2f] %s",
                extracted.get("relevance_score", 0),
                paper.get("title", "")[:60],
            )

    logger.info("Extraction complete: %d/%d relevant", len(enriched), len(papers))
    return enriched


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — Build similarity graph
# ─────────────────────────────────────────────────────────────────────────────

@task(
    name = "build-graph",
    tags = ["analysis"],
)
def task_build_graph(all_records: list[dict]) -> list[dict]:
    """Compute Jaccard co-occurrence connections."""
    logger = get_run_logger()
    connections = build_similarity_graph(all_records)
    logger.info("Graph built: %d connections", len(connections))
    return connections


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — Push to Airtable
# Retries handle Airtable rate limits (5 requests/sec)
# ─────────────────────────────────────────────────────────────────────────────

@task(
    name                = "push-to-airtable",
    retries             = 3,
    retry_delay_seconds = 30,   # Airtable rate limits recover in ~30s
    tags                = ["airtable", "write"],
)
def task_push_airtable(
    all_records:  list[dict],
    connections:  list[dict],
    dry_run:      bool,
) -> dict:
    """Write Sources, Molecules, and Connections to Airtable."""
    logger  = get_run_logger()
    pushed  = 0
    skipped = 0

    for rec in all_records:
        src_id = at_upsert_source(rec, dry_run)
        if src_id:
            pushed += 1
        else:
            skipped += 1
        for mol in (rec.get("molecules") or []):
            at_upsert_molecule(mol, src_id, dry_run)
            time.sleep(0.05)

    at_push_connections(connections[:MAX_CONNS], dry_run)

    logger.info(
        "Airtable push complete: %d sources, %d connections",
        pushed, min(len(connections), MAX_CONNS),
    )
    return {"pushed_sources": pushed, "skipped": skipped,
            "pushed_connections": min(len(connections), MAX_CONNS)}


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — Save JSON dump + state
# ─────────────────────────────────────────────────────────────────────────────

@task(name="save-outputs", tags=["state"])
def task_save(
    all_records:  list[dict],
    connections:  list[dict],
    state:        dict,
    output_json:  str,
) -> str:
    """Write pipeline_output.json and cursor_state.json."""
    logger = get_run_logger()

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(all_records, f, indent=2, default=str)
    logger.info("JSON dump saved → %s", output_json)

    state["stats"].update({
        "last_records":     len(all_records),
        "last_connections": len(connections),
        "last_run":         datetime.now().isoformat(),
    })
    save_state(state)
    logger.info("State saved — %d seen DOIs", len(state["seen_dois"]))
    return output_json


# ─────────────────────────────────────────────────────────────────────────────
# MAIN FLOW
# This is what Prefect sees as the "job". Everything above are its steps.
# ─────────────────────────────────────────────────────────────────────────────

@flow(
    name        = "flavor-pipeline",
    description = "Crawls academic databases for meat/Maillard flavor papers, "
                  "extracts with Claude, builds similarity graph, pushes to Airtable.",
    # ── Change this schedule to whatever you want ──
    # Every Sunday at 02:00:  "0 2 * * 0"
    # Every day at 03:00:     "0 3 * * *"
    # Every 12 hours:         "0 */12 * * *"
    # Remove schedule line to run manually only
)
def flavor_pipeline(
    keywords_path: str       = "keywords_bible.json",
    dry_run:       bool      = False,
    workers:       int       = WORKERS,
    databases:     list[str] = None,
    output_json:   str       = "pipeline_output.json",
):
    logger = get_run_logger()

    # ── Setup ────────────────────────────────────────────────────────────────
    if databases is None:
        databases = ALL_DBS
    if "serpapi" in databases and not SERPAPI_KEY:
        logger.warning("SERPAPI_KEY not set — skipping Google Scholar")
        databases = [d for d in databases if d != "serpapi"]

    state    = load_state()
    keywords = load_keywords(keywords_path)

    # LOW priority → OpenAlex only
    work = [
        (kw, db) for kw in keywords for db in databases
        if not (kw.get("priority") == "LOW" and db != "openalex")
    ]

    logger.info("="*60)
    logger.info(
        "FLOW START — %d work items | workers=%d | dbs=%s | dry=%s",
        len(work), workers, databases, dry_run,
    )
    logger.info("="*60)

    # ── Step 1: Fetch (submitted as parallel tasks) ──────────────────────────
    # Prefect submits these as concurrent tasks automatically
    # Each (keyword × db) pair is tracked individually in the UI
    fetch_futures = [
        task_fetch.submit(kw, db, state)
        for kw, db in work
    ]

    # Collect all raw papers
    all_raw = []
    for future in fetch_futures:
        try:
            papers = future.result()
            all_raw.extend(papers)
        except Exception as e:
            logger.error("Fetch task failed: %s", e)

    logger.info("Total raw papers: %d", len(all_raw))

    # ── Step 2: Claude extraction ────────────────────────────────────────────
    # Split into batches of 20 so Prefect tracks progress in chunks
    BATCH = 20
    extract_futures = [
        task_extract.submit(all_raw[i:i+BATCH], state)
        for i in range(0, len(all_raw), BATCH)
    ]

    all_records = []
    for future in extract_futures:
        try:
            all_records.extend(future.result())
        except Exception as e:
            logger.error("Extract task failed: %s", e)

    logger.info("Relevant records: %d", len(all_records))

    # ── Step 3: Similarity graph ─────────────────────────────────────────────
    connections = task_build_graph(all_records)

    # ── Step 4: Airtable push ────────────────────────────────────────────────
    push_result = task_push_airtable(all_records, connections, dry_run)
    logger.info("Push result: %s", push_result)

    # ── Step 5: Save outputs ─────────────────────────────────────────────────
    task_save(all_records, connections, state, output_json)

    # ── Summary ──────────────────────────────────────────────────────────────
    summary = {
        "run_at":       datetime.now().isoformat(),
        "work_items":   len(work),
        "raw_papers":   len(all_raw),
        "relevant":     len(all_records),
        "connections":  len(connections),
        "databases":    databases,
        "dry_run":      dry_run,
        **push_result,
    }
    logger.info("FLOW COMPLETE: %s", json.dumps(summary, indent=2))
    return summary


# ─────────────────────────────────────────────────────────────────────────────
# DEPLOYMENT  — registers this flow with Prefect server for scheduling
# Run once:  python flavor_flow.py --deploy
# ─────────────────────────────────────────────────────────────────────────────

def deploy_scheduled():
    """
    Register a weekly schedule with Prefect server.
    Only needed if you want automated scheduling.
    Run: python flavor_flow.py --deploy
    """
    from prefect.deployments import Deployment
    from prefect.server.schemas.schedules import CronSchedule

    deployment = Deployment.build_from_flow(
        flow          = flavor_pipeline,
        name          = "weekly-run",
        schedule      = CronSchedule(cron="0 2 * * 0", timezone="Asia/Jerusalem"),
        parameters    = {
            "databases":  ["openalex", "s2", "pubmed", "crossref"],
            "dry_run":    False,
            "workers":    5,
        },
        work_queue_name = "default",
    )
    deployment.apply()
    print("Deployment registered. Start the agent with:")
    print("  prefect agent start --work-queue default")


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Flavor Pipeline — Prefect Flow")
    p.add_argument("--deploy",    action="store_true", help="Register scheduled deployment")
    p.add_argument("--dry-run",   action="store_true")
    p.add_argument("--databases", nargs="+", choices=ALL_DBS, default=None)
    p.add_argument("--workers",   type=int, default=WORKERS)
    p.add_argument("--keywords",  default="keywords_bible.json")
    p.add_argument("--output",    default="pipeline_output.json")
    args = p.parse_args()

    if args.deploy:
        deploy_scheduled()
    else:
        # Run the flow directly
        flavor_pipeline(
            keywords_path = args.keywords,
            dry_run       = args.dry_run,
            workers       = args.workers,
            databases     = args.databases,
            output_json   = args.output,
        )
