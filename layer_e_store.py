"""
layer_e_store.py
══════════════════════════════════════════════════════════════════════
The curated knowledge store sitting between extraction (Layer C)
and Airtable (Layer F).

WHAT IT DOES
  - Stores every extracted record in SQLite before anything touches Airtable
  - Computes a composite ranking score for every source, molecule, claim
  - Splits records into three tiers:
      auto-promote  score >= 0.70  → queued for Airtable immediately
      review queue  score 0.50–0.69 → waits for human approve/reject
      hold          score < 0.50   → stays in Layer E only
  - Hash-based pre-extraction lookup: if a paper was already extracted
    with the current extraction_version, skip the Claude call entirely
    (saves API cost on re-runs)
  - Stores instruction memory: topic rules, exclusion logic, ranking
    heuristics — the system's long-term preferences
  - Full provenance chain on every record

MOCK EXTRACTION (no API key needed)
  Use mock_extract(paper) instead of extract(paper) to run the full
  pipeline end-to-end without spending any API credits. Produces
  realistic fake ExtractionResult objects so every other layer can
  be tested completely.

HOW TO INTEGRATE
  In pipeline.py worker(), after claude_extract():
    from layer_e_store import Store
    store = Store()                         # open once, reuse
    already_done = store.check_cache(paper["id"], extraction_version=1)
    if already_done:
        continue                            # skip Claude call
    result = claude_extract(paper)          # only if not cached
    store.write(result)                     # write to SQLite
    # Airtable write stays unchanged below

  To run with mock extraction (no API needed):
    from layer_e_store import mock_extract
    result = mock_extract(paper)
    store.write(result)

STORAGE
  Default path: same folder as this file / layer_e_store.db
  Override:     Store(db_path="/path/to/custom.db")

INSTALL
  No extra packages — uses Python stdlib sqlite3 only.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import random
import re
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

log = logging.getLogger("layer_e")

DB_PATH = Path(__file__).with_name("layer_e_store.db")

# ─────────────────────────────────────────────────────────────────────────────
# RANKING WEIGHTS
# Composite score = weighted sum of normalised signals (all 0–1)
# Adjust these to change what gets promoted to Airtable
# ─────────────────────────────────────────────────────────────────────────────
WEIGHTS = {
    "relevance_score":     0.35,   # Claude's relevance rating
    "extraction_confidence": 0.25, # avg confidence across molecules + claims
    "citation_score":      0.15,   # log10(citations+1) normalised
    "evidence_density":    0.10,   # fraction of claims with evidence spans
    "metadata_quality":    0.10,   # has DOI + abstract + year + venue
    "molecule_count":      0.05,   # normalised molecule count (0–10 → 0–1)
}

PROMOTE_THRESHOLD = float(os.environ.get("LAYER_E_PROMOTE_THRESHOLD", "0.70"))
REVIEW_THRESHOLD  = float(os.environ.get("LAYER_E_REVIEW_THRESHOLD",  "0.50"))

# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS sources (
    source_uid          TEXT PRIMARY KEY,
    title               TEXT NOT NULL,
    year                INTEGER,
    venue               TEXT,
    url                 TEXT,
    authors             TEXT,
    abstract            TEXT,
    citation_count      INTEGER DEFAULT 0,
    keyword             TEXT,
    branch              TEXT,
    db_source           TEXT,
    relevance_score     REAL DEFAULT 0.0,
    composite_score     REAL DEFAULT 0.0,
    extraction_version  INTEGER DEFAULT 0,
    review_status       TEXT DEFAULT 'pending',
    promoted_at         TEXT,
    fetched_at          TEXT,
    raw_json            TEXT
);

CREATE TABLE IF NOT EXISTS molecules (
    entity_uid          TEXT PRIMARY KEY,
    source_uid          TEXT REFERENCES sources(source_uid),
    name                TEXT NOT NULL,
    type                TEXT,
    role                TEXT,
    sensory             TEXT,
    threshold_ppb       REAL,
    confidence          REAL DEFAULT 0.8,
    evidence_span       TEXT,
    composite_score     REAL DEFAULT 0.0,
    review_status       TEXT DEFAULT 'pending',
    promoted_at         TEXT,
    created_at          TEXT
);

CREATE TABLE IF NOT EXISTS claims (
    claim_uid           TEXT PRIMARY KEY,
    source_uid          TEXT REFERENCES sources(source_uid),
    claim_text          TEXT NOT NULL,
    claim_type          TEXT,
    supporting_excerpt  TEXT,
    confidence          REAL DEFAULT 0.8,
    composite_score     REAL DEFAULT 0.0,
    review_status       TEXT DEFAULT 'pending',
    promoted_at         TEXT,
    created_at          TEXT
);

CREATE TABLE IF NOT EXISTS connections (
    edge_uid            TEXT PRIMARY KEY,
    entity_a            TEXT NOT NULL,
    entity_b            TEXT NOT NULL,
    cooccurrence        INTEGER DEFAULT 1,
    jaccard             REAL DEFAULT 0.0,
    strength            TEXT DEFAULT 'weak',
    source_uids         TEXT,
    created_at          TEXT
);

CREATE TABLE IF NOT EXISTS review_queue (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    record_uid          TEXT NOT NULL,
    record_type         TEXT NOT NULL,
    composite_score     REAL,
    title_preview       TEXT,
    branch              TEXT,
    db_source           TEXT,
    decision            TEXT DEFAULT 'pending',
    decided_at          TEXT,
    created_at          TEXT
);

CREATE TABLE IF NOT EXISTS instruction_memory (
    key                 TEXT PRIMARY KEY,
    value               TEXT NOT NULL,
    updated_at          TEXT
);

CREATE TABLE IF NOT EXISTS run_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at              TEXT,
    sources_fetched     INTEGER DEFAULT 0,
    sources_extracted   INTEGER DEFAULT 0,
    cache_hits          INTEGER DEFAULT 0,
    promoted            INTEGER DEFAULT 0,
    queued_for_review   INTEGER DEFAULT 0,
    held                INTEGER DEFAULT 0,
    run_notes           TEXT
);

CREATE INDEX IF NOT EXISTS idx_sources_branch     ON sources(branch);
CREATE INDEX IF NOT EXISTS idx_sources_score      ON sources(composite_score DESC);
CREATE INDEX IF NOT EXISTS idx_sources_status     ON sources(review_status);
CREATE INDEX IF NOT EXISTS idx_sources_version    ON sources(extraction_version);
CREATE INDEX IF NOT EXISTS idx_molecules_source   ON molecules(source_uid);
CREATE INDEX IF NOT EXISTS idx_molecules_name     ON molecules(name);
CREATE INDEX IF NOT EXISTS idx_molecules_score    ON molecules(composite_score DESC);
CREATE INDEX IF NOT EXISTS idx_claims_source      ON claims(source_uid);
CREATE INDEX IF NOT EXISTS idx_claims_type        ON claims(claim_type);
CREATE INDEX IF NOT EXISTS idx_review_status      ON review_queue(decision);
CREATE INDEX IF NOT EXISTS idx_review_type        ON review_queue(record_type);
"""

# ─────────────────────────────────────────────────────────────────────────────
# COMPOSITE SCORE
# ─────────────────────────────────────────────────────────────────────────────

def compute_composite_score(rec: dict) -> float:
    """
    Compute a 0–1 composite score for a pipeline result dict.
    Higher = more worth promoting to Airtable.
    """
    signals = {}

    # 1. Relevance score (direct from Claude, already 0–1)
    signals["relevance_score"] = float(rec.get("relevance_score", 0) or 0)

    # 2. Extraction confidence (avg of molecule + claim confidences)
    mols   = rec.get("molecules", []) or []
    claims = rec.get("claims", [])    or []
    confs  = [m.get("confidence", 0.8) for m in mols] + \
             [c.get("confidence", 0.8) for c in claims]
    signals["extraction_confidence"] = sum(confs) / len(confs) if confs else 0.5

    # 3. Citation score — log10(n+1) normalised against 1000 citations = 1.0
    citations = int(rec.get("citations", 0) or 0)
    signals["citation_score"] = min(math.log10(citations + 1) / 3.0, 1.0)

    # 4. Evidence density — fraction of claims that have a supporting excerpt
    if claims:
        has_excerpt = sum(1 for c in claims if c.get("supporting_excerpt","").strip())
        signals["evidence_density"] = has_excerpt / len(claims)
    else:
        signals["evidence_density"] = 0.0

    # 5. Metadata quality — 0.25 each for DOI/URL, abstract, year, venue
    meta = 0.0
    if (rec.get("url") or rec.get("doi") or "").strip():
        meta += 0.25
    if (rec.get("abstract") or "").strip():
        meta += 0.25
    if rec.get("year"):
        meta += 0.25
    if (rec.get("venue") or "").strip():
        meta += 0.25
    signals["metadata_quality"] = meta

    # 6. Molecule count — normalised: 5+ molecules → 1.0
    signals["molecule_count"] = min(len(mols) / 5.0, 1.0)

    # Weighted sum
    score = sum(WEIGHTS[k] * signals[k] for k in WEIGHTS)
    return round(min(max(score, 0.0), 1.0), 4)


def tier(score: float) -> str:
    if score >= PROMOTE_THRESHOLD:
        return "promote"
    if score >= REVIEW_THRESHOLD:
        return "review"
    return "hold"


# ─────────────────────────────────────────────────────────────────────────────
# STORE CLASS
# ─────────────────────────────────────────────────────────────────────────────

class Store:
    """
    Layer E knowledge store.

    Usage:
        store = Store()                 # opens/creates layer_e_store.db
        store = Store("custom.db")      # custom path

        # check cache before calling Claude
        if store.check_cache(source_uid, extraction_version=1):
            pass  # already done, skip
        else:
            result = claude_extract(paper)
            store.write(result)

        # get records for review
        queue = store.get_review_queue()

        # approve/reject a record
        store.decide("uid-here", "approved")

        # get records ready to push to Airtable
        to_promote = store.get_pending_promotions()
    """

    def __init__(self, db_path: str | Path = DB_PATH):
        self.db_path = Path(db_path)
        self._init_db()

    @contextmanager
    def _conn(self):
        con = sqlite3.connect(self.db_path, timeout=10)
        con.row_factory = sqlite3.Row
        try:
            yield con
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()

    def _init_db(self):
        with self._conn() as con:
            con.executescript(SCHEMA)
        log.info("Layer E store ready: %s", self.db_path)

    # ── Cache lookup ──────────────────────────────────────────────────────────

    def check_cache(self, source_uid: str, extraction_version: int = 1) -> bool:
        """
        Returns True if this paper was already extracted with the current
        prompt version. If True, skip the Claude call entirely.
        """
        with self._conn() as con:
            row = con.execute(
                "SELECT extraction_version FROM sources WHERE source_uid = ?",
                (source_uid,)
            ).fetchone()
        if row is None:
            return False   # never seen
        if row["extraction_version"] < extraction_version:
            return False   # seen but with old prompt — re-extract
        return True        # already extracted, skip

    # ── Write a pipeline result dict ─────────────────────────────────────────

    def write(self, rec: dict) -> str:
        """
        Write one pipeline result dict (output of claude_extract or mock_extract)
        to the store.

        Returns the source_uid.
        Computes composite score, determines tier, adds to review_queue if needed.
        """
        # Stable source_uid — use existing id if it's already a sha256-style hash
        raw_uid = rec.get("id") or rec.get("source_uid") or ""
        source_uid = _stable_uid(rec)

        score = compute_composite_score(rec)
        record_tier = tier(score)
        now = _now()

        # Serialise lists to JSON strings for storage
        molecules = rec.get("molecules", []) or []
        claims    = rec.get("claims",    []) or []

        with self._conn() as con:
            # ── sources ──────────────────────────────────────────────────────
            con.execute("""
                INSERT INTO sources (
                    source_uid, title, year, venue, url, authors, abstract,
                    citation_count, keyword, branch, db_source,
                    relevance_score, composite_score, extraction_version,
                    review_status, fetched_at, raw_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(source_uid) DO UPDATE SET
                    composite_score     = excluded.composite_score,
                    relevance_score     = excluded.relevance_score,
                    extraction_version  = excluded.extraction_version,
                    review_status       = CASE
                        WHEN excluded.composite_score >= {pt} THEN 'auto_promoted'
                        WHEN excluded.composite_score >= {rt} THEN 'pending'
                        ELSE 'held' END,
                    raw_json            = excluded.raw_json
            """.format(pt=PROMOTE_THRESHOLD, rt=REVIEW_THRESHOLD), (
                source_uid,
                (rec.get("title") or "Untitled")[:500],
                rec.get("year"),
                (rec.get("venue") or "")[:250],
                (rec.get("url") or "")[:500],
                ", ".join(rec.get("authors", []) or [])[:500],
                (rec.get("abstract") or "")[:5000],
                int(rec.get("citations", 0) or 0),
                rec.get("keyword", ""),
                rec.get("branch", ""),
                rec.get("db", ""),
                float(rec.get("relevance_score", 0) or 0),
                score,
                int(rec.get("extraction_version", 1) or 1),
                ("auto_promoted" if record_tier == "promote"
                 else "pending"  if record_tier == "review"
                 else "held"),
                now,
                json.dumps(rec, default=str)[:50000],
            ))

            # ── molecules ────────────────────────────────────────────────────
            for mol in molecules:
                name = (mol.get("name") or "").strip()
                if not name or len(name) < 2:
                    continue
                entity_uid = "mol:" + hashlib.sha256(
                    name.lower().encode()).hexdigest()[:20]
                mol_score = float(mol.get("confidence", 0.8))
                con.execute("""
                    INSERT INTO molecules (
                        entity_uid, source_uid, name, type, role,
                        sensory, threshold_ppb, confidence, evidence_span,
                        composite_score, review_status, created_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(entity_uid) DO UPDATE SET
                        confidence      = MAX(confidence, excluded.confidence),
                        composite_score = MAX(composite_score, excluded.composite_score)
                """, (
                    entity_uid, source_uid, name[:200],
                    mol.get("type", "other"),
                    mol.get("role", "other"),
                    (mol.get("sensory") or "")[:300],
                    mol.get("threshold_ppb"),
                    mol_score, mol.get("evidence_span", ""),
                    mol_score,
                    "auto_promoted" if mol_score >= PROMOTE_THRESHOLD else "pending",
                    now,
                ))

            # ── claims ───────────────────────────────────────────────────────
            for claim in claims:
                text = (claim.get("claim_text") or "").strip()
                if not text:
                    continue
                claim_uid = claim.get("claim_uid") or (
                    "clm:" + hashlib.sha256(
                        f"{source_uid}::{text[:80]}".encode()
                    ).hexdigest()[:20]
                )
                cl_score = float(claim.get("confidence", 0.8))
                con.execute("""
                    INSERT INTO claims (
                        claim_uid, source_uid, claim_text, claim_type,
                        supporting_excerpt, confidence, composite_score,
                        review_status, created_at
                    ) VALUES (?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(claim_uid) DO NOTHING
                """, (
                    claim_uid, source_uid, text[:600],
                    claim.get("claim_type", "observational"),
                    claim.get("supporting_excerpt", "")[:500],
                    cl_score, cl_score,
                    "auto_promoted" if cl_score >= PROMOTE_THRESHOLD else "pending",
                    now,
                ))

            # ── review queue (only for review-tier records) ───────────────────
            if record_tier == "review":
                existing_q = con.execute(
                    "SELECT id FROM review_queue WHERE record_uid = ? AND decision = 'pending'",
                    (source_uid,)
                ).fetchone()
                if not existing_q:
                    con.execute("""
                        INSERT INTO review_queue
                            (record_uid, record_type, composite_score,
                             title_preview, branch, db_source, created_at)
                        VALUES (?,?,?,?,?,?,?)
                    """, (
                        source_uid, "source", score,
                        (rec.get("title") or "")[:120],
                        rec.get("branch", ""),
                        rec.get("db", ""),
                        now,
                    ))

        log.info(
            "Layer E: wrote '%s' score=%.3f tier=%s",
            (rec.get("title") or "")[:50], score, record_tier
        )
        return source_uid

    # ── Review queue ──────────────────────────────────────────────────────────

    def get_review_queue(self, limit: int = 50) -> list[dict]:
        """
        Returns records waiting for human decision.
        Sorted by composite_score descending (best first).
        """
        with self._conn() as con:
            rows = con.execute("""
                SELECT rq.id, rq.record_uid, rq.record_type,
                       rq.composite_score, rq.title_preview,
                       rq.branch, rq.db_source, rq.created_at,
                       s.url, s.year, s.venue
                FROM review_queue rq
                LEFT JOIN sources s ON s.source_uid = rq.record_uid
                WHERE rq.decision = 'pending'
                ORDER BY rq.composite_score DESC
                LIMIT ?
            """, (limit,)).fetchall()
        return [dict(r) for r in rows]

    def decide(self, record_uid: str, decision: str):
        """
        Set a review queue decision: 'approved' or 'rejected'.
        Approved records get review_status='approved' on the source.
        """
        assert decision in ("approved", "rejected"), \
            "decision must be 'approved' or 'rejected'"
        now = _now()
        with self._conn() as con:
            con.execute("""
                UPDATE review_queue SET decision=?, decided_at=?
                WHERE record_uid=? AND decision='pending'
            """, (decision, now, record_uid))
            status = "approved" if decision == "approved" else "rejected"
            con.execute(
                "UPDATE sources SET review_status=? WHERE source_uid=?",
                (status, record_uid)
            )
        log.info("Layer E: %s → %s", record_uid[:20], decision)

    # ── Promotion gate ────────────────────────────────────────────────────────

    def get_pending_promotions(self) -> list[dict]:
        """
        Returns records ready to push to Airtable:
          - auto_promoted (score >= 0.70) and not yet pushed
          - approved via review queue

        Call this from layer_f_writer.py instead of reading Airtable.
        """
        with self._conn() as con:
            rows = con.execute("""
                SELECT * FROM sources
                WHERE review_status IN ('auto_promoted', 'approved')
                AND promoted_at IS NULL
                ORDER BY composite_score DESC
            """).fetchall()
        return [dict(r) for r in rows]

    def mark_promoted(self, source_uid: str):
        """Call after successfully writing to Airtable."""
        with self._conn() as con:
            con.execute(
                "UPDATE sources SET promoted_at=? WHERE source_uid=?",
                (_now(), source_uid)
            )

    def get_molecules_for_source(self, source_uid: str) -> list[dict]:
        with self._conn() as con:
            rows = con.execute(
                "SELECT * FROM molecules WHERE source_uid=? ORDER BY confidence DESC",
                (source_uid,)
            ).fetchall()
        return [dict(r) for r in rows]

    def get_claims_for_source(self, source_uid: str) -> list[dict]:
        with self._conn() as con:
            rows = con.execute(
                "SELECT * FROM claims WHERE source_uid=? ORDER BY confidence DESC",
                (source_uid,)
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Stats ─────────────────────────────────────────────────────────────────

    def stats(self) -> dict:
        """Summary statistics — shown in dashboard."""
        with self._conn() as con:
            s = {}
            s["total_sources"]   = con.execute("SELECT COUNT(*) FROM sources").fetchone()[0]
            s["total_molecules"] = con.execute("SELECT COUNT(*) FROM molecules").fetchone()[0]
            s["total_claims"]    = con.execute("SELECT COUNT(*) FROM claims").fetchone()[0]
            s["review_pending"]  = con.execute(
                "SELECT COUNT(*) FROM review_queue WHERE decision='pending'"
            ).fetchone()[0]
            s["promoted"]        = con.execute(
                "SELECT COUNT(*) FROM sources WHERE promoted_at IS NOT NULL"
            ).fetchone()[0]
            s["auto_promoted"]   = con.execute(
                "SELECT COUNT(*) FROM sources WHERE review_status='auto_promoted'"
            ).fetchone()[0]
            s["held"]            = con.execute(
                "SELECT COUNT(*) FROM sources WHERE review_status='held'"
            ).fetchone()[0]
            by_branch = con.execute("""
                SELECT branch, COUNT(*) as n, ROUND(AVG(composite_score),3) as avg_score
                FROM sources GROUP BY branch ORDER BY n DESC
            """).fetchall()
            s["by_branch"] = [dict(r) for r in by_branch]
            avg = con.execute(
                "SELECT ROUND(AVG(composite_score),3) FROM sources"
            ).fetchone()[0]
            s["avg_composite_score"] = avg or 0.0
        return s

    # ── Instruction memory ────────────────────────────────────────────────────

    def remember(self, key: str, value: str):
        """Store a persistent instruction or preference."""
        with self._conn() as con:
            con.execute("""
                INSERT INTO instruction_memory (key, value, updated_at)
                VALUES (?,?,?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value,
                                               updated_at=excluded.updated_at
            """, (key, value, _now()))

    def recall(self, key: str, default: str = "") -> str:
        with self._conn() as con:
            row = con.execute(
                "SELECT value FROM instruction_memory WHERE key=?", (key,)
            ).fetchone()
        return row["value"] if row else default

    # ── Run logging ───────────────────────────────────────────────────────────

    def log_run(self, sources_fetched=0, sources_extracted=0,
                cache_hits=0, promoted=0, queued=0, held=0, notes=""):
        with self._conn() as con:
            con.execute("""
                INSERT INTO run_log
                    (run_at, sources_fetched, sources_extracted,
                     cache_hits, promoted, queued_for_review, held, run_notes)
                VALUES (?,?,?,?,?,?,?,?)
            """, (_now(), sources_fetched, sources_extracted,
                  cache_hits, promoted, queued, held, notes))

    def get_run_history(self, limit: int = 20) -> list[dict]:
        with self._conn() as con:
            rows = con.execute(
                "SELECT * FROM run_log ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _stable_uid(rec: dict) -> str:
    """
    Generate a stable SHA-256 source_uid from DOI or title+year.
    Same paper from two databases → same uid.
    """
    doi = (rec.get("url") or rec.get("id") or "").strip()
    if "doi.org/" in doi:
        doi = doi.split("doi.org/")[-1]
    if doi.startswith("10."):
        raw = doi.lower()
    else:
        title = re.sub(r"\s+", " ", (rec.get("title") or "").lower().strip())
        year  = str(rec.get("year") or "")
        raw   = f"{title}::{year}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


# ─────────────────────────────────────────────────────────────────────────────
# MOCK EXTRACTION — full pipeline testing with no API credits
# ─────────────────────────────────────────────────────────────────────────────

_MOCK_MOLECULES = [
    {"name": "2-methyl-3-furanthiol",  "type": "thiol",    "role": "marker",     "sensory": "meaty, roasted",       "confidence": 0.95, "evidence_span": "dominant meaty odorant identified by GC-olfactometry"},
    {"name": "hexanal",                "type": "aldehyde", "role": "marker",     "sensory": "grassy, fatty",        "confidence": 0.88, "evidence_span": "hexanal as primary indicator of lipid oxidation"},
    {"name": "pyrazine",               "type": "pyrazine", "role": "product",    "sensory": "roasted, nutty",       "confidence": 0.82, "evidence_span": "pyrazines formed via Maillard reaction at 140-160C"},
    {"name": "cysteine",               "type": "other",    "role": "precursor",  "sensory": "",                     "confidence": 0.90, "evidence_span": "cysteine as key sulfur-containing precursor"},
    {"name": "2-furfurylthiol",        "type": "thiol",    "role": "marker",     "sensory": "coffee, meaty",        "confidence": 0.78, "evidence_span": "2-furfurylthiol produced from furfural and H2S"},
    {"name": "methional",              "type": "aldehyde", "role": "product",    "sensory": "cooked potato, brothy","confidence": 0.72, "evidence_span": "methional identified as Strecker degradation product"},
    {"name": "thiamine",               "type": "other",    "role": "precursor",  "sensory": "",                     "confidence": 0.85, "evidence_span": "thiamine thermal degradation yielding sulfur volatiles"},
    {"name": "ribose",                 "type": "other",    "role": "precursor",  "sensory": "",                     "confidence": 0.80, "evidence_span": "ribose as reducing sugar in cysteine model system"},
    {"name": "IMP",                    "type": "other",    "role": "potentiator","sensory": "umami",                "confidence": 0.76, "evidence_span": "inosine monophosphate synergistic with glutamate"},
    {"name": "linoleic acid",          "type": "acid",     "role": "precursor",  "sensory": "",                     "confidence": 0.83, "evidence_span": "linoleic acid C18:2 primary substrate for lipid oxidation"},
]

_MOCK_CLAIMS = [
    {"claim_text": "Maillard reaction between cysteine and ribose at 140°C produces 2-methyl-3-furanthiol as the dominant meaty odorant.", "claim_type": "mechanistic",  "supporting_excerpt": "cysteine-ribose model system at 140C produced 2M3F as dominant odorant", "confidence": 0.92},
    {"claim_text": "Hexanal concentration above 0.5 ppm indicates significant lipid oxidation in beef samples.", "claim_type": "quantitative", "supporting_excerpt": "hexanal threshold of 0.5 ppm used as oxidation indicator in beef", "confidence": 0.87},
    {"claim_text": "Plant-based matrices require supplementation with sulfur-containing precursors to replicate meaty aroma.", "claim_type": "comparative",  "supporting_excerpt": "PBM matrices lack native sulfur pools found in animal muscle", "confidence": 0.79},
    {"claim_text": "GC-MS with SPME headspace extraction achieves detection limits below 1 ppb for key meat volatiles.", "claim_type": "methodological","supporting_excerpt": "SPME-GC-MS method validated for sub-ppb detection of thiols and furans","confidence": 0.85},
]

_MOCK_BRANCHES = [
    "Maillard Reaction", "Lipid Oxidation", "Volatile Compounds",
    "Precursors", "Sulfur Chemistry", "Analytical Methods", "Meat Analogs"
]

_MOCK_CONNECTIONS = [
    "cysteine::2-methyl-3-furanthiol",
    "Maillard reaction::pyrazine formation",
    "lipid oxidation::hexanal",
    "thiamine::sulfur volatiles",
    "ribose::Maillard reaction",
    "IMP::umami potentiation",
]


def mock_extract(paper: dict, relevant_probability: float = 0.65) -> dict:
    """
    Produces a realistic fake ExtractionResult for a paper dict.

    Use instead of claude_extract() when you have no API credits.
    Lets you test the full pipeline end-to-end: crawl → Layer E →
    ranking → review queue → Airtable push.

    relevant_probability: fraction of papers that will be marked relevant.
    Set lower (0.3) for a realistic irrelevant-paper rejection rate.
    """
    title    = (paper.get("title","")    or "").strip()
    abstract = (paper.get("abstract","") or "").strip()

    # Deterministic relevance: same paper always gets same result
    seed = int(hashlib.md5(title.lower().encode()).hexdigest()[:8], 16)
    rng  = random.Random(seed)

    relevant = rng.random() < relevant_probability

    if not relevant:
        return {
            **paper,
            "relevant":           False,
            "relevance_score":    round(rng.uniform(0.0, 0.35), 3),
            "branch":             "Other",
            "molecules":          [],
            "claims":             [],
            "key_claims":         [],
            "biological_processes": [],
            "connections":        [],
            "extraction_version": 1,
            "_mock":              True,
        }

    # Pick realistic data seeded from this paper's title
    branch       = rng.choice(_MOCK_BRANCHES)
    rel_score    = round(rng.uniform(0.55, 0.98), 3)
    n_molecules  = rng.randint(2, 5)
    n_claims     = rng.randint(1, 3)
    n_conn       = rng.randint(2, 4)

    molecules = []
    for mol in rng.sample(_MOCK_MOLECULES, min(n_molecules, len(_MOCK_MOLECULES))):
        m = dict(mol)
        # slight jitter so every paper's confidence is unique
        m["confidence"] = round(min(m["confidence"] + rng.uniform(-0.1, 0.05), 1.0), 2)
        molecules.append(m)

    claims = []
    source_uid = _stable_uid(paper)
    for claim in rng.sample(_MOCK_CLAIMS, min(n_claims, len(_MOCK_CLAIMS))):
        c = dict(claim)
        c["confidence"] = round(min(c["confidence"] + rng.uniform(-0.1, 0.05), 1.0), 2)
        c["claim_uid"]  = "clm:" + hashlib.sha256(
            f"{source_uid}::{c['claim_text'][:60]}".encode()
        ).hexdigest()[:20]
        claims.append(c)

    connections = rng.sample(_MOCK_CONNECTIONS, min(n_conn, len(_MOCK_CONNECTIONS)))

    processes = rng.sample([
        "Maillard reaction", "lipid oxidation", "Strecker degradation",
        "thiamine degradation", "protein hydrolysis"
    ], rng.randint(1, 3))

    return {
        **paper,
        "relevant":             True,
        "relevance_score":      rel_score,
        "branch":               branch,
        "molecules":            molecules,
        "claims":               claims,
        "key_claims":           [c["claim_text"] for c in claims],
        "biological_processes": processes,
        "connections":          connections,
        "extraction_version":   1,
        "_mock":                True,
    }


def mock_extract_batch(papers: list[dict]) -> list[dict]:
    """Extract a batch, returning only relevant results."""
    return [r for r in (mock_extract(p) for p in papers) if r.get("relevant")]


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE INTEGRATION HELPER
# Drop this into worker() in pipeline.py (or flavor_flow.py task_extract)
# ─────────────────────────────────────────────────────────────────────────────

def process_paper(
    paper:              dict,
    store:              Store,
    extractor,                  # pass claude_extract OR mock_extract
    extraction_version: int = 1,
    dry_run:            bool = False,
) -> dict | None:
    """
    Single-paper pipeline step with Layer E cache check.

    Returns the enriched record dict if relevant, None otherwise.

    Usage in worker():
        from layer_e_store import Store, process_paper
        store = Store()
        for paper in raw:
            result = process_paper(paper, store, claude_extract)
            if result:
                enriched.append(result)
    """
    uid = _stable_uid(paper)

    # Cache check — skip Claude if already extracted with this prompt version
    if store.check_cache(uid, extraction_version):
        log.debug("Cache hit: %s", (paper.get("title",""))[:50])
        return None  # already in store, skip re-extraction

    result = extractor(paper)

    if not result.get("relevant"):
        # Still mark as seen so we don't re-extract irrelevant papers
        if not dry_run:
            # Write minimal record just to cache the decision
            store.write({**result, "relevance_score": 0.0,
                        "molecules": [], "claims": [], "connections": []})
        return None

    if not dry_run:
        store.write(result)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# SELF-TEST — python layer_e_store.py
# Runs a complete mock pipeline: crawl → store → rank → review queue
# No API key, no Airtable needed
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import tempfile
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    # Use a temp DB so self-test never pollutes the real store
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        test_db = f.name

    print("\n" + "="*65)
    print("LAYER E — SQLITE KNOWLEDGE STORE SELF-TEST")
    print("="*65)
    print("Using temp DB:", test_db)

    store = Store(test_db)

    # Simulate 20 papers coming from the crawl (no API needed)
    fake_papers = [
        {
            "id":       f"10.1016/test.{i:03d}",
            "title":    title,
            "year":     2020 + (i % 5),
            "venue":    venue,
            "abstract": abstract,
            "authors":  ["Smith J", "Jones A"],
            "url":      f"https://doi.org/10.1016/test.{i:03d}",
            "citations": citations,
            "keyword":  keyword,
            "db":       "openalex",
        }
        for i, (title, venue, abstract, citations, keyword) in enumerate([
            ("Maillard reaction in beef tallow model systems",
             "Food Chemistry", "Cysteine and ribose react at 140C to form 2-methyl-3-furanthiol.", 142, "Maillard"),
            ("Lipid oxidation markers in cooked pork",
             "Meat Science", "Hexanal above 0.5 ppm indicates significant lipid oxidation in pork.", 88, "Lipid"),
            ("Volatile compounds in plant-based burger analogs",
             "Food Research International", "PBM products lack key sulfur volatiles present in beef.", 201, "PBM"),
            ("GC-MS analysis of beef headspace volatiles",
             "Journal of Agricultural and Food Chemistry", "SPME extraction with GC-MS detected 124 volatile compounds.", 67, "Analytical"),
            ("Thiamine degradation under thermal processing",
             "LWT Food Science", "Thiamine degrades above 100C yielding sulfur-containing odorants.", 55, "Precursor"),
            ("Effect of pH on pyrazine formation in Maillard systems",
             "Food Chemistry", "Pyrazine yield peaks at pH 7.5 in glucose-amino acid model systems.", 33, "Maillard"),
            ("Phospholipid composition of beef intramuscular fat",
             "Meat Science", "Species differences in phospholipid profiles drive flavor divergence.", 119, "Lipid"),
            ("Cysteine as sulfur precursor in meat flavor systems",
             "Flavour and Fragrance Journal", "Cysteine concentration above 5 mM maximises thiol yield.", 77, "Precursor"),
            ("IMP and glutamate synergy in beef broth",
             "Food Quality and Preference", "IMP-glutamate synergy enhances umami perception by 3-fold.", 44, "Precursor"),
            ("2-furfurylthiol formation kinetics",
             "Journal of Agricultural and Food Chemistry", "2-furfurylthiol formed via H2S addition to furfural at 120C.", 91, "Volatile"),
            ("Bovine genome wide association study for daily gain",
             "Journal of Animal Science", "14 SNPs associated with growth rate in Holstein cattle.", 22, "Genetics"),
            ("Antioxidant activity of plant polyphenols in storage",
             "Food Chemistry", "Quercetin extends shelf-life in oxidative conditions.", 56, "Antioxidant"),
            ("Sous vide cooking and myoglobin denaturation in beef",
             "Meat Science", "Myoglobin denaturation at 60C affects beef colour but not flavor.", 38, "Texture"),
            ("Sensory profiling of roasted chicken aroma compounds",
             "Food Quality and Preference", "2-acetylthiazole rated highest intensity in trained panel.", 63, "Analytical"),
            ("Lipid-Maillard interactions in beef tallow",
             "Food Chemistry", "Pre-oxidised tallow enhances beefy character via Maillard pathway.", 104, "Maillard"),
            ("Furan formation from carbohydrates during heating",
             "Journal of Agricultural and Food Chemistry", "Furfural precedes furan formation in ribose heated above 130C.", 48, "Heterocyclic"),
            ("Warmed-over flavor inhibition by rosemary extract",
             "Meat Science", "Rosemary extract at 0.1% inhibits WOF development over 5 days.", 71, "Lipid"),
            ("Peptide profiling of beef bone hydrolysate",
             "Food Chemistry", "Maillard-reactive peptides isolated from bone hydrolysate at pH 6.", 39, "Precursor"),
            ("Sensory quality of plant protein extrudates",
             "Journal of Food Science", "Sulfur off-notes in soy extrudates correlate with cysteine content.", 82, "Meat Analogs"),
            ("Temperature effect on aldehyde yield in lipid oxidation",
             "LWT Food Science", "Hexanal and nonanal yields increase linearly 50-150C in beef fat.", 57, "Lipid"),
        ])
    ]

    print(f"\nStep 1 — Running mock extraction on {len(fake_papers)} papers")
    promoted = review = held = cache = 0

    for paper in fake_papers:
        # First pass — extract and store
        result = process_paper(paper, store, mock_extract)
        if result:
            score = compute_composite_score(result)
            t = tier(score)
            if t == "promote": promoted += 1
            elif t == "review": review += 1
            else: held += 1
        # Second pass — verify cache works
        if store.check_cache(_stable_uid(paper), extraction_version=1):
            cache += 1

    store.log_run(
        sources_fetched=len(fake_papers),
        sources_extracted=promoted+review+held,
        cache_hits=cache,
        promoted=promoted,
        queued=review,
        held=held,
        notes="self-test mock run"
    )

    print(f"\nStep 2 — Results")
    s = store.stats()
    print(f"  Total sources     : {s['total_sources']}")
    print(f"  Total molecules   : {s['total_molecules']}")
    print(f"  Total claims      : {s['total_claims']}")
    print(f"  Auto-promoted     : {s['auto_promoted']}")
    print(f"  Review queue      : {s['review_pending']}")
    print(f"  Held (low score)  : {s['held']}")
    print(f"  Avg composite     : {s['avg_composite_score']:.3f}")
    print(f"  Cache hits (2nd)  : {cache}/{len(fake_papers)}")

    print(f"\nStep 3 — Branch breakdown")
    for b in s["by_branch"]:
        print(f"  {b['branch']:<30} n={b['n']:>2}  avg_score={b['avg_score']:.3f}")

    print(f"\nStep 4 — Review queue (pending decisions)")
    queue = store.get_review_queue()
    for item in queue[:5]:
        print(f"  [{item['composite_score']:.3f}] {item['title_preview'][:60]}")
    if len(queue) > 5:
        print(f"  ... and {len(queue)-5} more")

    print(f"\nStep 5 — Approve first queue item, reject second")
    if len(queue) >= 1:
        store.decide(queue[0]["record_uid"], "approved")
        print(f"  Approved: {queue[0]['title_preview'][:50]}")
    if len(queue) >= 2:
        store.decide(queue[1]["record_uid"], "rejected")
        print(f"  Rejected: {queue[1]['title_preview'][:50]}")

    print(f"\nStep 6 — Pending promotions (ready for Airtable)")
    to_push = store.get_pending_promotions()
    print(f"  {len(to_push)} records ready to push to Airtable")
    for rec in to_push[:3]:
        print(f"  [{rec['composite_score']:.3f}] {rec['title'][:55]}")

    print(f"\nStep 7 — Instruction memory")
    store.remember("exclusion_rule_1", "Ignore genetics papers unless directly linked to flavor phenotype")
    store.remember("promotion_note",   "Prefer papers with GC-MS quantification over sensory-only studies")
    print(f"  Stored: {store.recall('exclusion_rule_1')[:60]}")
    print(f"  Stored: {store.recall('promotion_note')[:60]}")

    print(f"\nStep 8 — Run history")
    history = store.get_run_history(3)
    for run in history:
        print(f"  {run['run_at']}  fetched={run['sources_fetched']}  "
              f"extracted={run['sources_extracted']}  "
              f"cache_hits={run['cache_hits']}")

    import os as _os
    _os.unlink(test_db)

    print("\n" + "="*65)
    print("Self-test complete — all steps passed.")
    print()
    print("To integrate into pipeline.py worker():")
    print("  from layer_e_store import Store, process_paper, mock_extract")
    print("  store = Store()   # open once before the worker loop")
    print("  # replace: extracted = claude_extract(paper)")
    print("  # with:    result = process_paper(paper, store, claude_extract)")
    print()
    print("To test without API credits:")
    print("  # replace claude_extract with mock_extract — zero API calls")
    print("  result = process_paper(paper, store, mock_extract)")
    print("="*65)
