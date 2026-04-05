"""
discover_experts.py
───────────────────
Weekly auto-discovery of new flavor & aroma scientists via OpenAlex.
Pushes new candidates to Airtable Experts table with all fields populated:
  Name, Affiliation, Country, Research Field, Relevance Score, H-Index,
  Total Papers, ORCID, Key Research, Keywords, Related Researchers.

Usage:
    python discover_experts.py             # full run
    python discover_experts.py --dry       # preview without writing
    python discover_experts.py --relink    # only re-compute Related Researchers links

Schedule:
    GitHub Actions cron — see .github/workflows/discover.yml
    Or run manually: python discover_experts.py

Environment variables (set in .env):
    AIRTABLE_TOKEN      — Airtable personal access token
    AIRTABLE_BASE_ID    — GFI database base ID (appcS9K0FZK2DIPbZ)
"""

import os, time, requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).with_name('.env'))

# ── Config ────────────────────────────────────────────────────────────────────
TOKEN      = os.environ['AIRTABLE_TOKEN']
BASE_ID    = os.environ.get('AIRTABLE_BASE_ID', 'appcS9K0FZK2DIPbZ')
TABLE_NAME = 'Experts'
AT_BASE    = f'https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}'
HEADERS    = {'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'}

# Keywords driving discovery — expand over time
QUERIES = [
    'Maillard reaction meat flavor volatile',
    'lipid oxidation meat aroma compounds',
    'plant-based meat flavor alternative protein',
    'GC-MS meat volatile analysis odorant',
    'food matrix flavor release protein',
    'sensory evaluation plant-based meat consumer',
    'precursor Maillard pyrazine thiophene meat',
]

# ── Field IDs ─────────────────────────────────────────────────────────────────
F_NAME     = 'fldRnZyFrNsqQkDEr'
F_AFFIL    = 'fldxLZGgtGkCziPFA'
F_COUNTRY  = 'fldRWlCMBIsdonoU2'
F_FIELD    = 'fldlMWf6AOm7ZfZC5'   # singleSelect: Research Field
F_SCORE    = 'fld5Aq61uimgyVMeK'   # Relevance Score
F_HINDEX   = 'fldE24TDMc8GCxcQL'
F_PAPERS   = 'fldSqUbbQa0lXl16P'
F_EMAIL    = 'fldAnf9rwBkwNjbbJ'
F_ORCID    = 'fldq3fCfYMaClhhrG'
F_OPENALEX = 'fldQJDzLi85nRMpSv'
F_RESEARCH = 'fldJ2vtcVxg8c2awx'   # Key Research (top-cited paper summary)
F_KEYWORDS = 'fldoGXunqDohbi4m7'
F_STATUS   = 'fldf5EEkgQQumjga8'   # Outreach Status singleSelect
F_RELATED  = 'fldoC28JP8NVYK4yL'   # Related Researchers (linked records)

# ── Research Field classification ─────────────────────────────────────────────
# Maps OpenAlex concept names → the singleSelect choices in Airtable
FIELD_CONCEPT_MAP = {
    'Lipid Oxidation':   ['lipid', 'oxidation', 'fatty acid', 'peroxide', 'rancidity',
                          'phospholipid', 'unsaturated'],
    'Maillard Reaction': ['maillard', 'pyrazine', 'thiophene', 'browning', 'amino acid',
                          'reducing sugar', 'amadori', 'furan', 'acrylamide'],
    'Volatile Analysis': ['volatile', 'gc-ms', 'gas chromatography', 'chromatography',
                          'odorant', 'aroma compound', 'headspace', 'spme', 'olfactometry'],
    'Sensory Science':   ['sensory', 'perception', 'consumer', 'psychophysics', 'hedonic',
                          'panel', 'organoleptic', 'palatability', 'taste receptor'],
    'Matrix Effects':    ['food matrix', 'encapsulation', 'emulsion', 'texture', 'gelation',
                          'protein structure', 'biopolymer', 'release', 'microstructure'],
}

# Flavor-science terms for relevance scoring
FLAVOR_TERMS = {
    'flavor', 'flavour', 'aroma', 'taste', 'odor', 'odour', 'olfaction',
    'maillard', 'volatile', 'sensory', 'lipid oxidation', 'meat', 'food',
    'odorant', 'pyrazine', 'gc-ms', 'food chemistry', 'food science',
}


def classify_research_field(concepts):
    """Pick the best-matching research field from the expert's OpenAlex concepts."""
    concept_text = ' '.join(c['display_name'].lower() for c in concepts)
    scores = {
        field: sum(1 for kw in kws if kw in concept_text)
        for field, kws in FIELD_CONCEPT_MAP.items()
    }
    best = max(scores, key=lambda x: scores[x])
    return best if scores[best] > 0 else 'Volatile Analysis'


def compute_relevance_score(hindex, papers, concepts):
    """
    Composite 0–1 relevance score:
      35% h-index (benchmark: 80 = world-class)
      25% papers  (benchmark: 500)
      40% concept overlap with flavor-science terms
    """
    h_norm = min(hindex / 80.0, 1.0)
    p_norm = min(papers / 500.0, 1.0)
    concept_names = {c['display_name'].lower() for c in concepts}
    overlap = len(FLAVOR_TERMS & concept_names) / len(FLAVOR_TERMS)
    score = round(0.35 * h_norm + 0.25 * p_norm + 0.40 * overlap, 2)
    return max(0.10, min(score, 1.0))


# ── Step 1: Load existing experts ────────────────────────────────────────────
def load_existing():
    """
    Returns:
      existing_names    — set of lowercase names
      existing_orcids   — set of ORCID strings
      existing_openalex — set of OpenAlex IDs
      oa_to_record      — dict: openalex_id -> airtable record id
    """
    names, orcids, openalex_ids = set(), set(), set()
    oa_to_record = {}
    params = {'fields[]': [F_NAME, F_ORCID, F_OPENALEX], 'pageSize': 100}
    while True:
        r = requests.get(AT_BASE, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()
        j = r.json()
        for rec in j.get('records', []):
            f = rec.get('fields', {})
            if f.get(F_NAME):
                names.add(f[F_NAME].strip().lower())
            if f.get(F_ORCID):
                orcids.add(f[F_ORCID].strip())
            if f.get(F_OPENALEX):
                oid = f[F_OPENALEX].strip()
                openalex_ids.add(oid)
                oa_to_record[oid] = rec['id']
        if not j.get('offset'):
            break
        params['offset'] = j['offset']
    print(f'  Loaded {len(names)} existing experts from Airtable.')
    return names, orcids, openalex_ids, oa_to_record


# ── Step 2: Query OpenAlex works ──────────────────────────────────────────────
def search_openalex(query, per_page=25):
    """Search OpenAlex works and collect unique authors from results."""
    url = 'https://api.openalex.org/works'
    params = {
        'search': query,
        'per-page': per_page,
        'select': 'id,title,authorships,publication_year,cited_by_count',
        'mailto': 'lior@gfi.org',
    }
    r = requests.get(url, params=params, timeout=30)
    if not r.ok:
        print(f'  OpenAlex error {r.status_code} for query: {query[:50]}')
        return []
    authors = []
    for work in r.json().get('results', []):
        for auth in work.get('authorships', []):
            a = auth.get('author', {})
            if not a.get('id'):
                continue
            insts = auth.get('institutions', [])
            inst_name    = insts[0].get('display_name', '') if insts else ''
            inst_country = insts[0].get('country_code', '') if insts else ''
            authors.append({
                'openalex_id': a['id'].split('/')[-1],
                'name':        a.get('display_name', ''),
                'orcid':       a.get('orcid', '') or '',
                'affil':       inst_name,
                'country':     inst_country,
            })
    return authors


# ── Step 3: Enrich one author via OpenAlex author endpoint ────────────────────
def get_key_research(openalex_id):
    """Title + citation count of this author's single most-cited paper."""
    r = requests.get(
        'https://api.openalex.org/works',
        params={
            'filter': f'author.id:{openalex_id}',
            'sort': 'cited_by_count:desc',
            'per-page': 1,
            'select': 'title,cited_by_count',
            'mailto': 'lior@gfi.org',
        },
        timeout=30,
    )
    if not r.ok:
        return ''
    results = r.json().get('results', [])
    if results:
        title = results[0].get('title', '')
        count = results[0].get('cited_by_count', 0)
        return f'{title} ({count} citations)' if title else ''
    return ''


def enrich_author(openalex_id):
    """
    Fetch from OpenAlex author profile:
      hindex, papers, keywords, research_field, relevance, key_research, orcid
    """
    url = f'https://api.openalex.org/authors/{openalex_id}'
    r = requests.get(url, params={'mailto': 'lior@gfi.org'}, timeout=30)
    if not r.ok:
        return {}
    d = r.json()

    concepts = d.get('x_concepts', [])
    keywords = ', '.join(
        c['display_name']
        for c in sorted(concepts, key=lambda x: -x.get('score', 0))[:6]
    )
    hindex = d.get('summary_stats', {}).get('h_index', 0)
    papers = d.get('works_count', 0)

    # ORCID — prefer the author-level record over the work-level one
    raw_orcid = (d.get('ids') or {}).get('orcid', '')
    clean_orcid = raw_orcid.replace('https://orcid.org/', '').strip()

    key_research = get_key_research(openalex_id)

    return {
        'hindex':         hindex,
        'papers':         papers,
        'keywords':       keywords,
        'key_research':   key_research,
        'research_field': classify_research_field(concepts),
        'relevance':      compute_relevance_score(hindex, papers, concepts),
        'orcid':          clean_orcid,
    }


# ── Step 4: Push new expert to Airtable ──────────────────────────────────────
def push_expert(author, enriched):
    """
    POST a new expert record.  Returns (success: bool, airtable_record_id: str|None).
    Falls back to posting without Research Field if Airtable returns 422
    (happens when the singleSelect choice doesn't exist yet).
    """
    orcid = enriched.get('orcid') or author.get('orcid', '').replace('https://orcid.org/', '')

    fields = {
        F_NAME:     author['name'],
        F_AFFIL:    author['affil'],
        F_COUNTRY:  author['country'],
        F_OPENALEX: author['openalex_id'],
        F_STATUS:   'Auto-discovered',
        F_SCORE:    enriched.get('relevance', 0.5),
    }
    if orcid:
        fields[F_ORCID] = orcid
    if enriched.get('hindex'):
        fields[F_HINDEX] = enriched['hindex']
    if enriched.get('papers'):
        fields[F_PAPERS] = enriched['papers']
    if enriched.get('keywords'):
        fields[F_KEYWORDS] = enriched['keywords']
    if enriched.get('research_field'):
        fields[F_FIELD] = enriched['research_field']
    if enriched.get('key_research'):
        fields[F_RESEARCH] = enriched['key_research']

    r = requests.post(AT_BASE, headers=HEADERS, json={'records': [{'fields': fields}]}, timeout=30)

    # singleSelect 422: choice may not exist yet — retry without it
    if r.status_code == 422:
        fields.pop(F_FIELD, None)
        r = requests.post(AT_BASE, headers=HEADERS, json={'records': [{'fields': fields}]}, timeout=30)

    if r.status_code == 422:
        print(f'  422 pushing {author["name"]}: {r.text[:200]}')
        return False, None

    r.raise_for_status()
    record_id = r.json()['records'][0]['id']
    return True, record_id


# ── Step 5: Related Researchers linking ───────────────────────────────────────
def get_coauthor_openalex_ids(openalex_id, limit=15):
    """Return list of (coauthor_openalex_id, name) sorted by co-occurrence count."""
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


def link_related_researchers(oa_to_record, dry_run=False):
    """
    For every expert in oa_to_record:
      - Fetch their top co-authors from OpenAlex
      - Cross-reference with other experts already in our Airtable
      - PATCH the Related Researchers field with matched record IDs
    """
    print('\n── Linking Related Researchers ──')
    if not oa_to_record:
        print('  No experts with OpenAlex IDs found.')
        return

    updated = 0
    for oa_id, record_id in oa_to_record.items():
        coauthors = get_coauthor_openalex_ids(oa_id)
        time.sleep(0.3)

        linked_ids = [
            oa_to_record[ca_id]
            for ca_id, _ in coauthors
            if ca_id in oa_to_record and ca_id != oa_id
        ]
        if not linked_ids:
            continue

        names = [name for ca_id, name in coauthors if ca_id in oa_to_record and ca_id != oa_id]
        print(f'  Linking {oa_id}: {", ".join(names[:3])}{"..." if len(names) > 3 else ""}')

        if dry_run:
            print(f'    [DRY] Would link {len(linked_ids)} record(s)')
            updated += 1
            continue

        r = requests.patch(
            f'{AT_BASE}/{record_id}',
            headers=HEADERS,
            json={'fields': {F_RELATED: [{'id': rid} for rid in linked_ids]}},
            timeout=30,
        )
        if r.ok:
            updated += 1
        else:
            print(f'    WARN patch failed {r.status_code}: {r.text[:100]}')
        time.sleep(0.2)

    print(f'  Linked Related Researchers for {updated} expert(s).')


# ── Main ─────────────────────────────────────────────────────────────────────
def run(dry_run=False, relink_only=False):
    print('=== discover_experts.py ===')
    existing_names, existing_orcids, existing_openalex, oa_to_record = load_existing()

    if relink_only:
        link_related_researchers(oa_to_record, dry_run=dry_run)
        return

    seen_this_run = set()
    new_count = 0

    for query in QUERIES:
        print(f'\nQuery: {query[:60]}')
        authors = search_openalex(query, per_page=20)
        print(f'  Found {len(authors)} authors.')
        time.sleep(0.5)

        for author in authors:
            oid  = author['openalex_id']
            name = author['name'].strip().lower()
            orc  = author['orcid'].replace('https://orcid.org/', '')

            if oid in existing_openalex or oid in seen_this_run:
                continue
            if name in existing_names:
                continue
            if orc and orc in existing_orcids:
                continue
            if not author['name']:
                continue

            seen_this_run.add(oid)
            enriched = enrich_author(oid)
            time.sleep(0.3)

            if dry_run:
                print(f'  [DRY] {author["name"]} | {author["affil"]} | '
                      f'{enriched.get("research_field","?")} | '
                      f'h={enriched.get("hindex",0)} | '
                      f'score={enriched.get("relevance",0.5)}')
                new_count += 1
            else:
                ok, new_record_id = push_expert(author, enriched)
                if ok:
                    print(f'  + {author["name"]} | {author["affil"]} | '
                          f'{enriched.get("research_field","?")} | '
                          f'h={enriched.get("hindex",0)} | '
                          f'score={enriched.get("relevance",0.5)}')
                    new_count += 1
                    oa_to_record[oid] = new_record_id
                    existing_openalex.add(oid)
                time.sleep(0.2)

    print(f'\n=== {"Would add" if dry_run else "Added"} {new_count} new expert(s). ===')

    # Re-link all experts at the end of every full run
    if not dry_run:
        link_related_researchers(oa_to_record)


if __name__ == '__main__':
    import sys
    dry    = '--dry' in sys.argv
    relink = '--relink' in sys.argv
    run(dry_run=dry, relink_only=relink)
