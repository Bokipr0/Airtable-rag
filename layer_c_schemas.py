"""
layer_c_schemas.py
══════════════════════════════════════════════════════════════════════
DROP-IN REPLACEMENT for claude_extract() in pipeline.py.

WHAT CHANGES
  Before:  claude_extract(paper) → raw dict with json.loads()
             - silent failure if Claude returns malformed JSON
             - no field validation (wrong type = KeyError downstream)
             - claims are plain strings, not objects
             - no confidence per molecule or claim
             - no evidence spans

  After:   extract(paper) → same dict interface, fully typed internally
             - Pydantic validates every field before returning
             - ValidationError triggers an automatic retry with a
               correction prompt (not a silent fallback)
             - Claims are typed objects with evidence_span
             - Every molecule carries a confidence score
             - extraction_version bumped when you change the prompt
             - prompt_cache_control: system prompt cached at Anthropic
               → ~90% token cost reduction on repeated calls

HOW TO INTEGRATE (2-line change in pipeline.py)
  Remove:  from pipeline import claude_extract   (or just stop calling it)
  Add:     from layer_c_schemas import extract as claude_extract

  The function signature is identical:
      result_dict = claude_extract(paper_dict)
  So worker() in pipeline.py needs zero changes.

INSTALL
  pip install pydantic-ai pydantic

ENVIRONMENT
  Uses ANTHROPIC_API_KEY from .env — same key as the rest of pipeline.py
  No new env vars needed.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import time
from enum import Enum
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_ai import Agent
from pydantic_ai.models.anthropic import AnthropicModel, AnthropicModelSettings

load_dotenv()
log = logging.getLogger("layer_c")

# ─────────────────────────────────────────────────────────────────────────────
# VERSION — bump this whenever you change EXTRACT_SYSTEM_PROMPT
# Layer E uses this to know which records need re-extraction
# ─────────────────────────────────────────────────────────────────────────────
EXTRACTION_VERSION = 1
CLAUDE_MODEL       = os.environ.get("CLAUDE_EXTRACTION_MODEL", "claude-sonnet-4-5")
MAX_RETRIES        = 3

# ─────────────────────────────────────────────────────────────────────────────
# ENUMS — strict controlled vocabularies
# These replace the freeform strings Claude used to return
# ─────────────────────────────────────────────────────────────────────────────

class Branch(str, Enum):
    maillard            = "Maillard Reaction"
    lipid_oxidation     = "Lipid Oxidation"
    volatile_compounds  = "Volatile Compounds"
    precursors          = "Precursors"
    sulfur_chemistry    = "Sulfur Chemistry"
    heterocyclic        = "Heterocyclic Formation"
    analytical          = "Analytical Methods"
    meat_analogs        = "Meat Analogs"
    other               = "Other"

class MoleculeType(str, Enum):
    aldehyde  = "aldehyde"
    ketone    = "ketone"
    furan     = "furan"
    thiazole  = "thiazole"
    thiophene = "thiophene"
    pyrazine  = "pyrazine"
    thiol     = "thiol"
    acid      = "acid"
    alcohol   = "alcohol"
    peptide   = "peptide"
    lactone   = "lactone"
    ester     = "ester"
    amine     = "amine"
    sulfide   = "sulfide"
    other     = "other"

class MoleculeRole(str, Enum):
    precursor   = "precursor"
    product     = "product"
    marker      = "marker"
    potentiator = "potentiator"
    inhibitor   = "inhibitor"
    other       = "other"

class ClaimType(str, Enum):
    mechanistic   = "mechanistic"   # describes a reaction or pathway
    quantitative  = "quantitative"  # contains a measured value
    comparative   = "comparative"   # compares conditions or species
    methodological = "methodological"  # about an analytical technique
    observational = "observational" # general finding without mechanism

# ─────────────────────────────────────────────────────────────────────────────
# PYDANTIC MODELS
# ─────────────────────────────────────────────────────────────────────────────

class ExtractedMolecule(BaseModel):
    """
    One chemical compound extracted from a paper.
    name            — canonical compound name as it appears in the paper
    type            — chemical class from MoleculeType enum
    role            — functional role in flavor chemistry
    sensory         — aroma/taste description (empty string if not stated)
    threshold_ppb   — odor/taste detection threshold in ppb (null if not stated)
    confidence      — how certain the extraction is (0.0–1.0)
                      1.0 = explicitly named with full details
                      0.7 = named but role/type inferred
                      0.4 = mentioned briefly, may be peripheral
    evidence_span   — verbatim fragment (≤25 words) from the abstract
                      that supports this molecule's presence
    """
    name:          str            = Field(min_length=2, max_length=200)
    type:          MoleculeType   = MoleculeType.other
    role:          MoleculeRole   = MoleculeRole.other
    sensory:       str            = Field(default="", max_length=300)
    threshold_ppb: Optional[float]= Field(default=None, ge=0)
    confidence:    float          = Field(default=0.8, ge=0.0, le=1.0)
    evidence_span: str            = Field(default="", max_length=300)

    @field_validator("name")
    @classmethod
    def clean_name(cls, v: str) -> str:
        # Strip common noise prefixes Claude sometimes adds
        v = re.sub(r"^(compound|molecule|chemical|substance)[\s:]+", "", v,
                   flags=re.IGNORECASE)
        return v.strip()

    @field_validator("threshold_ppb", mode="before")
    @classmethod
    def coerce_threshold(cls, v):
        if v is None or v == "" or v == "null":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None


class ExtractedClaim(BaseModel):
    """
    One scientific claim extracted from a paper.

    claim_uid       — deterministic hash of (source_uid + claim_text[:80])
                      stable across re-extractions of the same paper
    claim_text      — the claim in one clear sentence (≤120 words)
    claim_type      — category from ClaimType enum
    supporting_excerpt — verbatim fragment (≤40 words) from the abstract
                         that the claim is derived from
    confidence      — extraction confidence (0.0–1.0)
    """
    claim_uid:           str       = Field(default="", max_length=64)
    claim_text:          str       = Field(min_length=10, max_length=600)
    claim_type:          ClaimType = ClaimType.observational
    supporting_excerpt:  str       = Field(default="", max_length=500)
    confidence:          float     = Field(default=0.8, ge=0.0, le=1.0)

    @field_validator("claim_text")
    @classmethod
    def normalise_claim(cls, v: str) -> str:
        v = v.strip()
        if v and not v.endswith("."):
            v += "."
        return v

    def generate_uid(self, source_uid: str) -> str:
        raw = f"{source_uid}::{self.claim_text[:80]}"
        return hashlib.sha256(raw.encode()).hexdigest()[:24]


class ExtractionResult(BaseModel):
    """
    Complete typed output for one paper extraction.

    relevant          — False means skip; pipeline marks paper as seen and moves on
    relevance_score   — 0.0–1.0 float (not a string, not a range description)
    branch            — one value from Branch enum
    molecules         — list of ExtractedMolecule (empty list if none found)
    claims            — list of ExtractedClaim (max 4, focus on strongest)
    biological_processes — free-text list of processes mentioned
    connections       — "entity_a::entity_b" co-occurrence pairs
    extraction_version — always set to EXTRACTION_VERSION constant
    """
    relevant:             bool                  = False
    relevance_score:      float                 = Field(default=0.0, ge=0.0, le=1.0)
    branch:               Branch                = Branch.other
    molecules:            list[ExtractedMolecule] = Field(default_factory=list)
    claims:               list[ExtractedClaim]    = Field(default_factory=list, max_length=4)
    biological_processes: list[str]              = Field(default_factory=list)
    connections:          list[str]              = Field(default_factory=list)
    extraction_version:   int                   = EXTRACTION_VERSION

    @model_validator(mode="after")
    def clamp_irrelevant(self) -> "ExtractionResult":
        # If not relevant, zero out all lists — no downstream noise
        if not self.relevant:
            self.molecules            = []
            self.claims               = []
            self.biological_processes = []
            self.connections          = []
            self.relevance_score      = 0.0
        return self

    @field_validator("connections", mode="before")
    @classmethod
    def validate_connections(cls, v):
        if not isinstance(v, list):
            return []
        # Keep only properly formed "a::b" pairs
        clean = []
        for item in v:
            if isinstance(item, str) and "::" in item:
                parts = [p.strip() for p in item.split("::", 1)]
                if all(len(p) >= 2 for p in parts):
                    clean.append(f"{parts[0]}::{parts[1]}")
        return clean

    def to_pipeline_dict(self, paper: dict) -> dict:
        """
        Merges extraction result back into the raw paper dict.
        Exact same structure pipeline.py worker() expects.
        """
        result = {**paper}
        result["relevant"]              = self.relevant
        result["relevance_score"]       = self.relevance_score
        result["branch"]                = self.branch.value
        result["extraction_version"]    = self.extraction_version
        result["molecules"]             = [m.model_dump() for m in self.molecules]
        result["biological_processes"]  = self.biological_processes
        result["connections"]           = self.connections

        # Claims: typed objects serialised to dicts, claim_uid generated
        source_uid = paper.get("id", "")
        claims_out = []
        for claim in self.claims:
            cd = claim.model_dump()
            cd["claim_uid"] = claim.generate_uid(source_uid)
            claims_out.append(cd)
        result["claims"] = claims_out

        # Keep backward-compat key_claims as plain string list
        # so existing at_upsert_source() still works without changes
        result["key_claims"] = [c.claim_text for c in self.claims]

        return result


# ─────────────────────────────────────────────────────────────────────────────
# SYSTEM PROMPT
# Bump EXTRACTION_VERSION above whenever you change this
# ─────────────────────────────────────────────────────────────────────────────

EXTRACT_SYSTEM_PROMPT = """\
You are a precision flavor-chemistry extraction engine for a meat and Maillard
reaction research database at GFI (Good Food Institute).

EXTRACT ONLY information relevant to:
  - meat flavor and aroma (beef, pork, chicken, lamb)
  - Maillard reaction and its products in meat/protein systems
  - lipid oxidation pathways and volatile products
  - volatile organic compounds (VOCs) in cooked meat
  - flavor precursors: amino acids, reducing sugars, nucleotides, peptides
  - sulfur compound formation (thiols, thiazoles, thiophenes)
  - heterocyclic compound formation (pyrazines, furans, imidazoles)
  - analytical methods for meat flavor (GC-MS, SPME, GC-O, sensory)
  - plant-based meat analogs and their flavor challenges
  - matrix effects on flavor release and perception

IGNORE completely:
  - general animal nutrition, growth, genetics
  - texture and tenderness unless directly linked to flavor
  - dairy, fruit, vegetable flavor unless explicitly compared to meat
  - food safety, microbiology, shelf-life (unless directly flavor-linked)
  - packaging, storage, processing unrelated to flavor

CLAIMS: Extract up to 4 claims. Prefer mechanistic and quantitative claims
over general observations. Each claim must be a single sentence that could
stand alone as a scientific statement. Include the exact fragment from the
abstract that supports it in supporting_excerpt (≤40 words verbatim).

MOLECULES: For each compound named in the abstract:
  - Set confidence=1.0 if the paper explicitly studies or quantifies it
  - Set confidence=0.7 if it is named but only mentioned in passing
  - Set confidence=0.4 if it is implied but not directly named
  - evidence_span: copy ≤25 words verbatim from the abstract mentioning it

CONNECTIONS: Return "entity_a::entity_b" pairs where both entities are
explicitly linked by the paper (e.g. "cysteine::2-methyl-3-furanthiol",
"Maillard reaction::pyrazine formation"). Max 8 pairs.

BIOLOGICAL_PROCESSES: List only processes explicitly described, not inferred.

RELEVANCE:
  - relevant=true + relevance_score 0.8–1.0: core flavor chemistry paper
  - relevant=true + relevance_score 0.5–0.8: partially relevant
  - relevant=false + relevance_score 0.0: not relevant at all

Return a JSON object matching the ExtractionResult schema exactly.
No markdown, no explanation, no preamble. Just the JSON object.
"""

# ─────────────────────────────────────────────────────────────────────────────
# PYDANTIC-AI AGENT
# Created once at module load — thread-safe for run_sync()
# ─────────────────────────────────────────────────────────────────────────────

_model = AnthropicModel(
    CLAUDE_MODEL,
    # Cache the system prompt at Anthropic — ~90% token cost reduction
    # on the system prompt for every call after the first
)

_model_settings = AnthropicModelSettings(
    max_tokens          = 1800,
    anthropic_cache_instructions = True,  # caches the system prompt
)

_agent: Agent[None, ExtractionResult] = Agent(
    model          = _model,
    output_type    = ExtractionResult,
    instructions   = EXTRACT_SYSTEM_PROMPT,
    model_settings = _model_settings,
)


# ─────────────────────────────────────────────────────────────────────────────
# RETRY PROMPT — used when Pydantic validation fails
# Gives Claude the validation error so it can correct itself
# ─────────────────────────────────────────────────────────────────────────────

def _build_retry_content(title: str, abstract: str, error: str) -> str:
    return (
        f"Title: {title}\n\nAbstract: {abstract}\n\n"
        f"Your previous response failed Pydantic validation with this error:\n"
        f"{error}\n\n"
        f"Fix the issue and return a valid ExtractionResult JSON object."
    )


# ─────────────────────────────────────────────────────────────────────────────
# MAIN EXTRACTION FUNCTION
# Drop-in replacement for claude_extract() in pipeline.py
# ─────────────────────────────────────────────────────────────────────────────

def extract(paper: dict) -> dict:
    """
    Typed extraction using PydanticAI.

    Takes the same raw paper dict that claude_extract() accepts.
    Returns the same dict structure that worker() in pipeline.py expects —
    with these additions:
        - paper["claims"]            list[dict]  typed claim objects
        - paper["extraction_version"] int        prompt version number
        - all molecules have "confidence" and "evidence_span" fields
        - all claims have "claim_uid", "claim_type", "supporting_excerpt"

    Failures:
        - ValidationError → retries with correction prompt (up to MAX_RETRIES)
        - All retries exhausted → returns paper with relevant=False
          (same safe fallback as the old claude_extract)
    """
    title    = (paper.get("title",    "") or "").strip()
    abstract = (paper.get("abstract", "") or "").strip()

    if not title and not abstract:
        log.debug("Skipping paper with no title or abstract: %s", paper.get("id","?"))
        return {**paper, "relevant": False, "relevance_score": 0.0,
                "extraction_version": EXTRACTION_VERSION,
                "molecules": [], "claims": [], "key_claims": [],
                "biological_processes": [], "connections": []}

    content = f"Title: {title}\n\nAbstract: {abstract}" if abstract else f"Title: {title}"
    last_error = ""

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            prompt = content if attempt == 1 else _build_retry_content(
                title, abstract, last_error
            )

            result = _agent.run_sync(prompt)
            extraction: ExtractionResult = result.output

            log.info(
                "  [C] %s | relevant=%s score=%.2f branch=%s mols=%d claims=%d",
                title[:50],
                extraction.relevant,
                extraction.relevance_score,
                extraction.branch.value,
                len(extraction.molecules),
                len(extraction.claims),
            )

            return extraction.to_pipeline_dict(paper)

        except Exception as e:
            last_error = str(e)
            log.warning(
                "Layer C attempt %d/%d failed for '%s': %s",
                attempt, MAX_RETRIES, title[:45], last_error
            )
            if attempt < MAX_RETRIES:
                time.sleep(2 * attempt)  # back-off: 2s, 4s

    # All retries exhausted — safe fallback, paper is still marked seen
    log.error("Layer C: all retries failed for '%s'", title[:50])
    return {**paper, "relevant": False, "relevance_score": 0.0,
            "extraction_version": EXTRACTION_VERSION,
            "molecules": [], "claims": [], "key_claims": [],
            "biological_processes": [], "connections": []}


# ─────────────────────────────────────────────────────────────────────────────
# BATCH HELPER — for Prefect task_extract() in flavor_flow.py
# ─────────────────────────────────────────────────────────────────────────────

def extract_batch(papers: list[dict]) -> list[dict]:
    """
    Extract a list of papers, returning only relevant ones.
    Used by task_extract() in flavor_flow.py.
    """
    results = []
    for paper in papers:
        result = extract(paper)
        if result.get("relevant"):
            results.append(result)
    return results


# ─────────────────────────────────────────────────────────────────────────────
# SELF-TEST — python layer_c_schemas.py
# Runs two test papers without hitting Airtable
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import json
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )

    TEST_PAPERS = [
        {
            "id":    "10.1016/test.001",
            "title": "Formation of 2-methyl-3-furanthiol in cysteine-ribose model systems",
            "abstract": (
                "The Maillard reaction between cysteine and ribose at 140°C produced "
                "2-methyl-3-furanthiol (2M3F) as the dominant meaty odorant, with an "
                "odor activity value (OAV) of 2800. Gas chromatography-olfactometry "
                "confirmed 2M3F as the key character-impact compound. Bis(2-methyl-3-furyl) "
                "disulfide was detected at 12 ppb, below its threshold of 40 ppb. Pyrazine "
                "formation increased linearly with temperature from 120–160°C. "
                "Thiamine degradation contributed approximately 30% of total sulfur "
                "volatile yield under these model system conditions."
            ),
            "year": 2022, "venue": "Food Chemistry", "branch": "Maillard Reaction",
            "keyword": "cysteine ribose model system", "db": "crossref",
            "authors": ["Smith J", "Jones A"], "url": "https://doi.org/10.1016/test.001",
            "citations": 45,
        },
        {
            "id":    "10.1016/test.002",
            "title": "Genetic determinants of bovine growth rate in feedlot cattle",
            "abstract": (
                "A genome-wide association study identified 14 SNPs significantly "
                "associated with average daily gain in Holstein cattle. Heritability "
                "estimates ranged from 0.31 to 0.48 across environments. Feed conversion "
                "ratio was not correlated with marbling score. No flavor-related "
                "phenotypes were assessed."
            ),
            "year": 2021, "venue": "Journal of Animal Science", "branch": "Other",
            "keyword": "beef genetics", "db": "pubmed",
            "authors": ["Brown K"], "url": "https://doi.org/10.1016/test.002",
            "citations": 12,
        },
    ]

    print("\n" + "="*65)
    print("LAYER C — PYDANTICAI EXTRACTION SELF-TEST")
    print("="*65)

    for i, paper in enumerate(TEST_PAPERS, 1):
        print(f"\n── Test {i}: {paper['title'][:55]}")
        result = extract(paper)

        print(f"   relevant         : {result['relevant']}")
        print(f"   relevance_score  : {result['relevance_score']:.2f}")
        print(f"   branch           : {result.get('branch','—')}")
        print(f"   extraction_version: {result.get('extraction_version')}")
        print(f"   molecules ({len(result.get('molecules',[]))}):")
        for m in result.get("molecules", []):
            print(f"     • {m['name']:<35} type={m['type']:<12} "
                  f"role={m['role']:<12} conf={m.get('confidence',0):.1f}")
        print(f"   claims ({len(result.get('claims',[]))}):")
        for c in result.get("claims", []):
            print(f"     [{c.get('claim_type','?')}] {c['claim_text'][:80]}")
            if c.get("supporting_excerpt"):
                print(f"       excerpt: \"{c['supporting_excerpt'][:60]}...\"")
        print(f"   connections      : {result.get('connections',[])}")
        print(f"   processes        : {result.get('biological_processes',[])}")

    print("\n" + "="*65)
    print("Self-test complete. Check output above for correctness.")
    print("To integrate: in pipeline.py replace")
    print("  extracted = claude_extract(paper)")
    print("with:")
    print("  from layer_c_schemas import extract as claude_extract")
    print("="*65)
