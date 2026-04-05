"""
enrich_expert.py
────────────────
Enriches all experts in the Airtable Experts table with:
  Research Field, Keywords, Key Research, Relevance Score,
  Co-Authors in Network, Related Researchers (linked records)

Data sources (in priority order):
  1. Semantic Scholar  — author search by name, topics, citation metrics,
                         most-cited paper, research fields
  2. ORCID API         — email, current affiliation (if public)
  3. OpenAlex          — co-author graph only (best graph coverage)

Works for ALL experts regardless of whether they have an OpenAlex ID.
Semantic Scholar searches by name so manually-curated experts are covered too.

Skip logic:
  An expert is considered "fully enriched" only when BOTH Research Field
  AND Keywords are already set. H-Index alone is NOT enough to skip.

Usage:
    python enrich_expert.py                     # enrich all that need it
    python enrich_expert.py --dry               # preview without writing
    python enrich_expert.py --name "Hofmann"    # single expert (substring match)
    python enrich_expert.py --relink            # only re-run Related Researchers links
    python enrich_expert.py --force             # re-enrich even if already done

Environment variables (.env):
    AIRTABLE_TOKEN
    AIRTABLE_BASE_ID
    S2_API_KEY    (optional — Semantic Scholar key for higher rate limits)
"""

import os, sys, time, requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).with_name('.env'))

TOKEN   = os.environ['AIRTABLE_TOKEN']
BASE_ID = os.environ.get('AIRTABLE_BASE_ID', 'appcS9K0FZK2DIPbZ')
TABLE   = 'Experts'
AT_BASE = f'https://api.airtable.com/v0/{BASE_ID}/{TABLE}'
AT_HDR  = {'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'}

S2_KEY  = os.environ.get('S2_API_KEY', '')   # optional — raises rate limit to 1 req/sec

# ── Field IDs ─────────────────────────────────────────────────────────────────
F_NAME     = 'fldRnZyFrNsqQkDEr'
F_OPENALEX = 'fldQJDzLi85nRMpSv'
F_ORCID    = 'fldq3fCfYMaClhhrG'
F_AFFIL    = 'fldxLZGgtGkCziPFA'
F_COUNTRY  = 'fldRWlCMBIsdonoU2'
F_HINDEX   = 'fldE24TDMc8GCxcQL'
F_PAPERS   = 'fldSqUbbQa0lXl16P'
F_EMAIL    = 'fldAnf9rwBkwNjbbJ'
F_KEYWORDS = 'fldoGXunqDohbi4m7'
F_COAUTH   = 'fldBgL9wV5889JI4U'
F_RESEARCH = 'fldJ2vtcVxg8c2awx'   # Key Research (most-cited paper)
F_FIELD    = 'fldlMWf6AOm7ZfZC5'   # Research Field singleSelect
F_SCORE    = 'fld5Aq61uimgyVMeK'   # Relevance Score
F_RELATED  = 'fldoC28JP8NVYK4yL'   # Related Researchers (linked)

# ── Research Field classification ─────────────────────────────────────────────
FIELD_CONCEPT_MAP = {
    'Lipid Oxidation':   ['lipid', 'oxidation', 'fatty acid', 'peroxide', 'rancid',
                          'phospholipid', 'unsaturated', 'free radical'],
    'Maillard Reaction': ['maillard', 'pyrazine', 'thiophene', 'browning', 'amino acid',
                          'reducing sugar', 'amadori', 'furan', 'acrylamide', 'strecker'],
    'Volatile Analysis': ['volatile', 'gc-ms', 'gas chromatography', 'odorant', 'aroma compound',
                          'headspace', 'spme', 'olfactometry', 'mass spectrometry', 'gcms'],
    'Sensory Science':   ['sensory', 'perception', 'consumer', 'psychophysics', 'hedonic',
                          'panel', 'organoleptic', 'palatability', 'taste receptor', 'preference'],
    'Matrix Effects':    ['food matrix', 'encapsulation', 'emulsion', 'texture', 'gelation',
                          'protein structure', 'biopolymer', 'release', 'microstructure', 'rheology'],
}

FLAVOR_TERMS = {
    'flavor', 'flavour', 'aroma', 'taste', 'odor', 'odour', 'olfaction',
    'maillard', 'volatile', 'sensory', 'lipid oxidation', 'meat', 'food',
    'odorant', 'pyrazine', 'gc-ms', 'food chemistry', 'food science',
    'cooking', 'roasting', 'thermal processing', 'spice', 'herb',
}


def classify_field(text):
    """Pick the best Research Field from a block of free text (topics, paper titles)."""
    text = text.lower()
    scores = {
        field: sum(1 for kw in kws if kw in text)
        for field, kws in FIELD_CONCEPT_MAP.items()
    }
    best = max(scores, key=lambda x: scores[x])
    return best if scores[best] > 0 else None


def compute_relevance(hindex, papers, topic_text):
    """0–1 score: h-index 35%, paper count 25%, flavor-term overlap 40%."""
    h_norm = min(hindex / 80.0, 1.0)
    p_norm = min(papers / 500.0, 1.0)
    words  = set(topic_text.lower().split())
    overlap = len(FLAVOR_TERMS & words) / len(FLAVOR_TERMS)
    return round(max(0.10, min(0.35 * h_norm + 0.25 * p_norm + 0.40 * overlap, 1.0)), 2)


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 1: Semantic Scholar
# ══════════════════════════════════════════════════════════════════════════════
S2_BASE = 'https://api.semanticscholar.org/graph/v1'
S2_HDR  = {'x-api-key': S2_KEY} if S2_KEY else {}

def s2_search_author(name):
    """
    Search Semantic Scholar for an author by name.
    Returns the best-matching author dict, or None.
    Picks the match with the highest paperCount as tie-breaker.
    """
    r = requests.get(
        f'{S2_BASE}/author/search',
        headers=S2_HDR,
        params={
            'query': name,
            'fields': 'authorId,name,affiliations,paperCount,citationCount,hIndex',
            'limit': 5,
        },
        timeout=20,
    )
    if not r.ok:
        return None
    results = r.json().get('data', [])
    if not results:
        return None
    # Pick candidate whose name is closest to query, break ties by paperCount
    name_lower = name.lower()
    def score(a):
        n = a.get('name', '').lower()
        exact = 1 if n == name_lower else 0
        partial = 1 if name_lower.split()[-1] in n else 0  # last name match
        return (exact, partial, a.get('paperCount', 0))
    return max(results, key=score)


def s2_get_author_details(s2_id):
    """
    Full author profile from Semantic Scholar: topics, papers, co-authors.
    Returns dict with: hindex, papers, keywords, research_field,
                       key_research, coauthor_names, coauthor_s2ids
    """
    r = requests.get(
        f'{S2_BASE}/author/{s2_id}',
        headers=S2_HDR,
        params={
            'fields': 'name,affiliations,paperCount,citationCount,hIndex,'
                      'papers.title,papers.citationCount,papers.year,'
                      'papers.fieldsOfStudy,papers.authors',
        },
        timeout=30,
    )
    if not r.ok:
        return None
    d = r.json()

    papers_list = d.get('papers', [])
    hindex  = d.get('hIndex', 0) or 0
    papers  = d.get('paperCount', 0) or 0

    # Keywords: aggregate fieldsOfStudy labels across all papers
    field_freq = {}
    for p in papers_list:
        for f in (p.get('fieldsOfStudy') or []):
            field_freq[f] = field_freq.get(f, 0) + 1
    keywords = ', '.join(k for k, _ in sorted(field_freq.items(), key=lambda x: -x[1])[:8])

    # Top co-authors by frequency
    coauth_freq = {}
    for p in papers_list:
        for a in (p.get('authors') or []):
            aid  = a.get('authorId', '')
            aname = a.get('name', '')
            if not aid or aid == s2_id or not aname:
                continue
            if aid not in coauth_freq:
                coauth_freq[aid] = {'name': aname, 'count': 0}
            coauth_freq[aid]['count'] += 1
    ranked = sorted(coauth_freq.items(), key=lambda x: -x[1]['count'])[:10]
    coauth_names = [info['name'] for _, info in ranked]
    coauth_ids   = [aid for aid, _ in ranked]

    # Most-cited paper
    key_paper = max(papers_list, key=lambda p: p.get('citationCount', 0), default=None)
    key_research = ''
    if key_paper:
        title = key_paper.get('title', '')
        cites = key_paper.get('citationCount', 0)
        key_research = f'{title} ({cites} citations)' if title else ''

    # Research field from paper titles + field labels
    all_text = keywords + ' ' + ' '.join(p.get('title', '') for p in papers_list[:20])
    research_field = classify_field(all_text)

    relevance = compute_relevance(hindex, papers, all_text)

    return {
        'hindex':         hindex,
        'papers':         papers,
        'keywords':       keywords,
        'coauthor_names': coauth_names,
        'coauthor_s2ids': coauth_ids,
        'key_research':   key_research,
        'research_field': research_field,
        'relevance':      relevance,
    }


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 2: ORCID  (email + current affiliation)
# ══════════════════════════════════════════════════════════════════════════════
ORCID_BASE = 'https://pub.orcid.org/v3.0'
ORCID_HDR  = {'Accept': 'application/json'}

def _clean_orcid(raw):
    return raw.replace('https://orcid.org/', '').strip() if raw else ''

def orcid_get_contact(orcid_raw):
    """
    Query ORCID for email and current affiliation.
    Returns dict with 'email' and 'affil' (may be empty strings).
    ORCID only exposes emails the researcher has made public.
    """
    orcid = _clean_orcid(orcid_raw)
    if not orcid:
        return {}
    try:
        r = requests.get(f'{ORCID_BASE}/{orcid}/person',
                         headers=ORCID_HDR, timeout=15)
        if not r.ok:
            return {}
        person = r.json()
        # Email
        email = ''
        emails = (person.get('emails') or {}).get('email', [])
        for e in emails:
            if e.get('primary') and e.get('visibility') == 'public':
                email = e.get('email', '')
                break
        if not email and emails:
            email = emails[0].get('email', '') if emails[0].get('visibility') == 'public' else ''

        return {'email': email}
    except Exception:
        return {}


def orcid_search_by_name(name):
    """Search ORCID by name to find ORCID ID + email for manually-entered experts."""
    try:
        r = requests.get(
            f'{ORCID_BASE}/search/',
            headers=ORCID_HDR,
            params={'q': f'family-name:{name.split()[-1]} given-names:{name.split()[0]}',
                    'rows': 3},
            timeout=15,
        )
        if not r.ok:
            return None, ''
        results = r.json().get('result', [])
        if not results:
            return None, ''
        orcid_id = results[0].get('orcid-identifier', {}).get('path', '')
        contact = orcid_get_contact(orcid_id) if orcid_id else {}
        return orcid_id, contact.get('email', '')
    except Exception:
        return None, ''


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 3: OpenAlex co-author graph
# ══════════════════════════════════════════════════════════════════════════════
def oa_get_coauthor_ids(openalex_id, limit=15):
    """Return list of (openalex_id, name) of top co-authors."""
    r = requests.get(
        'https://api.openalex.org/works',
        params={
            'filter': f'author.id:{openalex_id}',
            'per-page': 25,
            'select': 'authorships',
            'mailto': 'lior@gfi.org',
        },
        timeout=30,
    )
    if not r.ok:
        return []
    freq = {}
    for work in r.json().get('results', []):
        for auth in work.get('authorships', []):
            a = auth.get('author') or {}
            aid  = a.get('id', '').split('/')[-1]
            name = a.get('display_name', '')
            if not aid or openalex_id in aid:
                continue
            if aid not in freq:
                freq[aid] = {'name': name, 'count': 0}
            freq[aid]['count'] += 1
    return [(aid, info['name']) for aid, info in
            sorted(freq.items(), key=lambda x: -x[1]['count'])[:limit]]


# ══════════════════════════════════════════════════════════════════════════════
# Airtable helpers
# ══════════════════════════════════════════════════════════════════════════════
def get_all_experts():
    """Fetch all expert records. Returns (records_list, oa_to_record dict)."""
    records, oa_to_record = [], {}
    params = {
        'pageSize': 100,
        'fields[]': [F_NAME, F_OPENALEX, F_ORCID, F_HINDEX, F_PAPERS,
                     F_FIELD, F_KEYWORDS, F_EMAIL, F_AFFIL],
    }
    while True:
        r = requests.get(AT_BASE, headers=AT_HDR, params=params, timeout=30)
        r.raise_for_status()
        j = r.json()
        for rec in j.get('records', []):
            records.append(rec)
            oa_id = rec.get('fields', {}).get(F_OPENALEX, '').strip()
            if oa_id:
                oa_to_record[oa_id] = rec['id']
        if not j.get('offset'):
            break
        params['offset'] = j['offset']
    return records, oa_to_record


def patch_record(record_id, fields):
    """PATCH Airtable record. Retries once without singleSelect if 422."""
    r = requests.patch(f'{AT_BASE}/{record_id}', headers=AT_HDR,
                       json={'fields': fields}, timeout=30)
    if r.status_code == 422 and F_FIELD in fields:
        fields = {k: v for k, v in fields.items() if k != F_FIELD}
        r = requests.patch(f'{AT_BASE}/{record_id}', headers=AT_HDR,
                           json={'fields': fields}, timeout=30)
    if not r.ok:
        print(f'    WARN PATCH {r.status_code}: {r.text[:120]}')
        return False
    return True


# ══════════════════════════════════════════════════════════════════════════════
# Related Researchers linking
# ══════════════════════════════════════════════════════════════════════════════
def link_related_researchers(oa_to_record, dry_run=False):
    """
    For every expert with an OpenAlex ID, find their co-authors that are
    also in our Experts table and patch the Related Researchers linked field.
    """
    print('\n── Linking Related Researchers ──')
    if not oa_to_record:
        print('  No experts with OpenAlex IDs.')
        return

    updated = 0
    for oa_id, record_id in oa_to_record.items():
        coauthors = oa_get_coauthor_ids(oa_id)
        time.sleep(0.3)

        linked = [
            oa_to_record[ca_id]
            for ca_id, _ in coauthors
            if ca_id in oa_to_record and ca_id != oa_id
        ]
        if not linked:
            continue

        names = [n for ca_id, n in coauthors if ca_id in oa_to_record and ca_id != oa_id]
        print(f'  {oa_id}: {", ".join(names[:3])}{"..." if len(names) > 3 else ""}')

        if dry_run:
            print(f'    [DRY] Would link {len(linked)} record(s)')
            updated += 1
            continue

        r = requests.patch(
            f'{AT_BASE}/{record_id}', headers=AT_HDR,
            json={'fields': {F_RELATED: [{'id': rid} for rid in linked]}},
            timeout=30,
        )
        if r.ok:
            updated += 1
        else:
            print(f'    WARN {r.status_code}: {r.text[:80]}')
        time.sleep(0.2)

    print(f'  Linked {updated} expert(s).')


# ══════════════════════════════════════════════════════════════════════════════
# Main enrichment loop
# ══════════════════════════════════════════════════════════════════════════════
def run(dry_run=False, target_name=None, relink_only=False, force=False):
    print('=== enrich_expert.py ===')
    records, oa_to_record = get_all_experts()
    print(f'  Loaded {len(records)} experts.\n')

    if relink_only:
        link_related_researchers(oa_to_record, dry_run=dry_run)
        return

    enriched_count = 0
    skipped_count  = 0

    for rec in records:
        f    = rec.get('fields', {})
        rid  = rec['id']
        name = f.get(F_NAME, '').strip()

        if not name:
            continue
        if target_name and target_name.lower() not in name.lower():
            continue

        # Skip only when BOTH Research Field AND Keywords are already filled
        already_done = bool(f.get(F_FIELD)) and bool(f.get(F_KEYWORDS))
        if already_done and not (target_name or force):
            skipped_count += 1
            continue

        print(f'\n  Enriching: {name}')

        # ── Step 1: Semantic Scholar (primary source) ──────────────────────
        s2_author = s2_search_author(name)
        time.sleep(0.6)   # S2 rate limit without key: ~1 req/sec

        enriched = None
        if s2_author:
            s2_id = s2_author.get('authorId', '')
            print(f'    S2 match: {s2_author.get("name")} (id={s2_id})')
            enriched = s2_get_author_details(s2_id)
            time.sleep(0.6)
        else:
            print(f'    S2: no match found')

        # ── Step 2: ORCID (email + verify ORCID ID) ────────────────────────
        raw_orcid = f.get(F_ORCID, '') or ''
        email     = f.get(F_EMAIL, '') or ''

        if not email:
            if raw_orcid:
                contact = orcid_get_contact(raw_orcid)
                email = contact.get('email', '')
            else:
                found_orcid, found_email = orcid_search_by_name(name)
                if found_orcid and not raw_orcid:
                    raw_orcid = found_orcid
                if found_email:
                    email = found_email
            time.sleep(0.4)

        # ── Step 3: Build Airtable patch payload ───────────────────────────
        fields_to_write = {}

        if enriched:
            if enriched.get('hindex') and not f.get(F_HINDEX):
                fields_to_write[F_HINDEX] = enriched['hindex']
            if enriched.get('papers') and not f.get(F_PAPERS):
                fields_to_write[F_PAPERS] = enriched['papers']
            if enriched.get('keywords'):
                fields_to_write[F_KEYWORDS] = enriched['keywords']
            if enriched.get('key_research'):
                fields_to_write[F_RESEARCH] = enriched['key_research']
            if enriched.get('research_field'):
                fields_to_write[F_FIELD] = enriched['research_field']
            if enriched.get('relevance'):
                # Only update if still at the default 0.5 placeholder
                if not f.get(F_SCORE) or f.get(F_SCORE) == 0.5:
                    fields_to_write[F_SCORE] = enriched['relevance']
            if enriched.get('coauthor_names'):
                fields_to_write[F_COAUTH] = ', '.join(enriched['coauthor_names'][:5])

        if email:
            fields_to_write[F_EMAIL] = email

        # ── Print summary ──────────────────────────────────────────────────
        if enriched:
            print(f'    h={enriched.get("hindex", "?")}  '
                  f'papers={enriched.get("papers", "?")}  '
                  f'field={enriched.get("research_field", "?")}  '
                  f'score={enriched.get("relevance", "?")}')
            print(f'    keywords: {enriched.get("keywords","")[:70]}')
            print(f'    key research: {enriched.get("key_research","")[:80]}')
        if email:
            print(f'    email: {email}')
        if not fields_to_write:
            print(f'    Nothing new to write.')
            skipped_count += 1
            continue

        if dry_run:
            print(f'    [DRY] Would update {len(fields_to_write)} field(s): '
                  f'{list(fields_to_write.keys())}')
        else:
            ok = patch_record(rid, fields_to_write)
            print(f'    {"Updated" if ok else "FAILED"} Airtable.')
            time.sleep(0.25)

        enriched_count += 1

    print(f'\n=== Done. Enriched {enriched_count} | Already complete {skipped_count} ===')

    # Re-link Related Researchers at end of full run
    if not dry_run and not target_name:
        link_related_researchers(oa_to_record)


if __name__ == '__main__':
    dry    = '--dry'    in sys.argv
    relink = '--relink' in sys.argv
    force  = '--force'  in sys.argv
    name   = None
    if '--name' in sys.argv:
        idx  = sys.argv.index('--name')
        name = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else None
    run(dry_run=dry, target_name=name, relink_only=relink, force=force)
