"""
mvp_molecules_from_sources.py
──────────────────────────────
Pipeline  (TOP_N=20 default):

  1. Fetch top-20 cited papers from OpenAlex
  2. Per paper → ensure Source row exists in Airtable
  3. Reconstruct abstract
  4. Extract molecules  → upsert Molecules table
       • category : Fats / Peptides / Proteins / Unclassified
       • smell / taste / use : compact keyword terms
  5. Extract claims  → upsert Claims table
       • claim_key  : "claim:<molecule_slug>:<property>"
       • claim_text : one plain-English sentence
       • confidence : 0.0–1.0  (based on signal strength in abstract)
       • stance     : "supports" | "neutral" | "contradicts"
       • Molecules  : linked record(s)
       • Sources    : linked record(s)

.env keys:
  AIRTABLE_TOKEN, AIRTABLE_BASE_ID
  SOURCES_TABLE   (default: Sources)
  MOLECULES_TABLE (default: Molecules)
  CLAIMS_TABLE    (default: Claims)
  DRY_RUN         (1 = print only, 0 = write)
  TOP_N           (default: 20)
  MAX_MOLS        (default: 12)
  MVP_QUERY       (default: Flavor and Metabolite Profiles of Meat)
"""

import os, re, time
import requests
from urllib.parse import quote
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

TOKEN   = os.environ['AIRTABLE_TOKEN']
BASE_ID = os.environ['AIRTABLE_BASE_ID']

SOURCES_TABLE   = os.environ.get('SOURCES_TABLE',   'Sources')
MOLECULES_TABLE = os.environ.get('MOLECULES_TABLE', 'Molecules')
CLAIMS_TABLE    = os.environ.get('CLAIMS_TABLE',    'Claims')

AIRTABLE_API_BASE = f'https://api.airtable.com/v0/{BASE_ID}'
HEADERS = {'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'}

# ── Source field names ────────────────────────────────────────────────────────
SRC_F_NAME     = os.environ.get('SRC_F_NAME',     'Name')
SRC_F_URL      = os.environ.get('SRC_F_URL',      'URL')
SRC_F_QUERY    = os.environ.get('SRC_F_QUERY',    'query')
SRC_F_YEAR     = os.environ.get('SRC_F_YEAR',     'Year')
SRC_F_VENUE    = os.environ.get('SRC_F_VENUE',    'Venue')
SRC_F_AUTHORS  = os.environ.get('SRC_F_AUTHORS',  'Authors')
SRC_F_CITES    = os.environ.get('SRC_F_CITES',    'citation_count')
SRC_F_KEYWORDS = os.environ.get('SRC_F_KEYWORDS', 'top_keywords')

# ── Molecule field names ──────────────────────────────────────────────────────
MOL_F_KEY          = os.environ.get('MOL_F_KEY',          'molecule_key')
MOL_F_NAME         = os.environ.get('MOL_F_NAME',         'Name')
MOL_F_COUNT        = os.environ.get('MOL_F_COUNT',        'mentions_count')
MOL_F_SMELL        = os.environ.get('MOL_F_SMELL',        'smell')
MOL_F_TASTE        = os.environ.get('MOL_F_TASTE',        'taste')
MOL_F_USE          = os.environ.get('MOL_F_USE',          'use')
MOL_F_CATEGORY     = os.environ.get('MOL_F_CATEGORY',     'category')
MOL_F_MELTING      = os.environ.get('MOL_F_MELTING',      'melting_point')
MOL_F_SOLUBILITY   = os.environ.get('MOL_F_SOLUBILITY',   'water_solubility')
MOL_F_LINK_SOURCES = os.environ.get('MOL_F_LINK_SOURCES', 'Sources')

# ── Claim field names (hardcoded to match your Airtable exactly) ─────────────
# NOT read from .env — hardcoded to prevent accidental override.
# If you rename a field in Airtable, change it here directly.
CLM_F_KEY          = 'claim_key'    # single line text — unique ID per claim
CLM_F_TEXT         = 'claim_text'   # long text — the primary field in Claims table
CLM_F_CONFIDENCE   = 'confidence'   # number field
CLM_F_STANCE       = 'stance'       # single select: supports / neutral / contradicts
CLM_F_LINK_MOLS    = 'Molecules'    # link to Molecules table
CLM_F_LINK_SOURCES = 'Sources'      # link to Sources table


# ==============================================================================
# SECTION 1 — AIRTABLE HELPERS
# ==============================================================================

def _add(fields: dict, key: str, value):
    """Only write field if key is non-empty and value is not None/empty."""
    if not key or not str(key).strip():
        return
    if value is None:
        return
    if isinstance(value, str) and not value.strip():
        return
    fields[key] = value


def airtable_find_record_id(table: str, key_field: str, key_value: str):
    safe = str(key_value).replace("'", "''")
    r = requests.get(
        f"{AIRTABLE_API_BASE}/{quote(table)}",
        headers=HEADERS,
        params={'filterByFormula': f"{{{key_field}}}='{safe}'", 'maxRecords': 1},
        timeout=30,
    )
    if r.status_code == 422:
        raise RuntimeError(f"Airtable 422 FIND ({table}) field={key_field}\n{r.text}")
    r.raise_for_status()
    recs = r.json().get('records', [])
    return recs[0]['id'] if recs else None


def airtable_upsert_by_key(table: str, key_field: str,
                            key_value: str, fields: dict) -> str:
    """
    Upsert a record by a unique key field.
    - On PATCH (update): only sends changed fields — does NOT re-send key_field
      because Airtable rejects writes to primary-field-like columns on some tables.
    - On POST (create): sends key_field + all fields together.
    - On 422: prints the exact Airtable error so you can see which field is wrong.
    """
    rec_id = airtable_find_record_id(table, key_field, key_value)
    if rec_id:
        # PATCH: update existing — send only the data fields, not the key again
        r = requests.patch(
            f"{AIRTABLE_API_BASE}/{quote(table)}/{rec_id}",
            headers=HEADERS,
            json={'fields': fields},   # key_field already set on this record
            timeout=30,
        )
    else:
        # POST: create new — must include key_field so the record is findable later
        r = requests.post(
            f"{AIRTABLE_API_BASE}/{quote(table)}",
            headers=HEADERS,
            json={'records': [{'fields': {key_field: key_value, **fields}}]},
            timeout=30,
        )
    if r.status_code == 422:
        raise RuntimeError(
            f"Airtable 422 UPSERT ({table})\n"
            f"  key_field={key_field!r}  key_value={key_value!r}\n"
            f"  fields sent={list(fields.keys())}\n"
            f"  response={r.text}"
        )
    r.raise_for_status()
    j = r.json()
    return j['records'][0]['id'] if 'records' in j else j['id']


def airtable_append_links_unique(table: str, record_id: str,
                                  link_field: str, new_ids: list):
    """Merges new_ids into a linked-record field — no duplicates."""
    if not link_field or not new_ids:
        return
    rec = requests.get(
        f"{AIRTABLE_API_BASE}/{quote(table)}/{record_id}",
        headers=HEADERS, timeout=30,
    )
    rec.raise_for_status()
    existing = rec.json().get('fields', {}).get(link_field, []) or []
    merged = list(existing) + [i for i in new_ids if i not in existing]
    r = requests.patch(
        f"{AIRTABLE_API_BASE}/{quote(table)}/{record_id}",
        headers=HEADERS,
        json={'fields': {link_field: merged}},
        timeout=30,
    )
    if r.status_code == 422:
        raise RuntimeError(f"Airtable 422 LINK ({table}) field={link_field}\n{r.text}")
    r.raise_for_status()


def airtable_find_source_by_url(url_value: str):
    if not url_value:
        return None
    safe = url_value.replace("'", "''")
    r = requests.get(
        f"{AIRTABLE_API_BASE}/{quote(SOURCES_TABLE)}",
        headers=HEADERS,
        params={'filterByFormula': f"{{{SRC_F_URL}}}='{safe}'", 'maxRecords': 1},
        timeout=30,
    )
    if not r.ok:
        return None
    recs = r.json().get('records', [])
    return recs[0]['id'] if recs else None


def airtable_create_source(fields: dict) -> str:
    r = requests.post(
        f"{AIRTABLE_API_BASE}/{quote(SOURCES_TABLE)}",
        headers=HEADERS,
        json={'records': [{'fields': fields}]},
        timeout=30,
    )
    if r.status_code == 422:
        raise RuntimeError(f"Airtable 422 CREATE SOURCE\n{r.text}")
    r.raise_for_status()
    return r.json()['records'][0]['id']


# ==============================================================================
# SECTION 2 — OPENALEX
# ==============================================================================

def openalex_top_papers(query: str, total: int = 20) -> list:
    """
    Fetches top `total` papers sorted by citation count descending.
    WHY: Most-cited = most validated findings in the field.
    """
    papers = []
    per_page = 25

    for page in range(1, 10):          # safety cap: never more than 10 pages
        params = {
            'search':   query,
            'sort':     'cited_by_count:desc',
            'per-page': per_page,
            'page':     page,
            'select':   (
                'id,title,publication_year,primary_location,'
                'authorships,doi,cited_by_count,abstract_inverted_index'
            ),
        }
        r = requests.get('https://api.openalex.org/works', params=params, timeout=30)
        if not r.ok:
            print(f'  OpenAlex page {page} failed: {r.status_code}')
            break
        results = r.json().get('results', [])
        if not results:
            break

        for w in results:
            doi = w.get('doi') or ''
            doi_url = doi if doi.startswith('http') else (
                f'https://doi.org/{doi}' if doi else ''
            )
            venue   = ((w.get('primary_location') or {}).get('source') or {})
            authors = [
                (a.get('author') or {}).get('display_name', '')
                for a in (w.get('authorships') or [])
            ]
            papers.append({
                'title':     w.get('title') or 'Untitled',
                'year':      w.get('publication_year'),
                'venue':     venue.get('display_name'),
                'authors':   [n for n in authors if n],
                'doi_url':   doi_url,
                'oa_id':     w.get('id', ''),
                'citations': w.get('cited_by_count', 0),
                'inv_idx':   w.get('abstract_inverted_index'),
            })

        print(f'  OpenAlex page {page}: +{len(results)} (total so far: {len(papers)})')
        if len(papers) >= total:
            break
        time.sleep(0.2)

    return papers[:total]


def reconstruct_abstract(inv_index) -> str:
    if not inv_index:
        return ''
    pos_word = {}
    for word, positions in inv_index.items():
        for p in positions:
            pos_word[p] = word
    return ' '.join(pos_word[i] for i in range(max(pos_word) + 1))


# ==============================================================================
# SECTION 3 — MOLECULE EXTRACTION
# ==============================================================================
#
# Four-layer filter — only specific real molecules pass through.
#
# Layer 1 — BLOCKLIST: instruments, method names, generic class names, verbs
# Layer 2 — CHEMICAL SUFFIX: token must end in a known chemical suffix
# Layer 3 — SPECIFICITY: name must have a specific chemical prefix
#            blocks "Fatty Acid" (generic class) while passing "Linoleic Acid"
# Layer 4 — MINIMUM LENGTH: >= 5 chars

# Layer 1a: explicit junk — verbs, instruments, method terms
_JUNK = {
    'candidate', 'determine', 'regulate', 'alkaline', 'indicate',
    'associate', 'correlate', 'evaluate', 'calculate', 'generate',
    'isolate', 'demonstrate', 'moderate', 'separate', 'estimate',
    'contribute', 'activate', 'accumulate', 'concentrate', 'participate',
    'iodo-l-tyrosine',
    # instruments
    'e-nose', 'electronic-nose', 'headspace', 'spme', 'hplc', 'gc-ms',
    'gc-fid', 'nmr', 'machine', 'baseline', 'response', 'variance',
}

# Layer 1b: molecule CLASS names — categories not specific compounds.
# "Fatty Acid" = a class; "Linoleic Acid" = a real molecule.
_GENERIC_CLASSES = {
    'fatty acid', 'amino acid', 'organic acid', 'nucleic acid',
    'bile acid', 'short-chain fatty acid', 'long-chain fatty acid',
    'medium-chain fatty acid', 'free fatty acid', 'saturated fatty acid',
    'unsaturated fatty acid', 'polyunsaturated fatty acid',
    'monounsaturated fatty acid', 'essential fatty acid',
    'trans fatty acid', 'volatile fatty acid',
    'carbohydrate', 'polysaccharide', 'monosaccharide', 'disaccharide',
    'lipid', 'phospholipid', 'glycolipid', 'sphingolipid',
    'volatile compound', 'flavor compound', 'aroma compound',
    'phenolic compound', 'polyphenol', 'flavonoid', 'terpenoid',
    'aldehyde', 'ketone', 'ester', 'lactone', 'alcohol', 'amine',
    'alkene', 'alkane', 'aromatic compound',
    'metabolite', 'biomarker', 'substrate', 'precursor', 'product',
    'enzyme', 'protein', 'peptide', 'hormone', 'vitamin', 'mineral',
    'antioxidant', 'preservative', 'additive', 'ingredient',
    'volatile', 'non-volatile', 'semi-volatile',
    'tricarboxylic acid', 'dicarboxylic acid',
}

# Layer 1c: generic single-word terms that slip through suffix check
_GENERIC_SINGLE = {
    'oxide', 'chloride', 'sulfide', 'bromide', 'iodide', 'fluoride',
    'phosphate', 'sulfate', 'nitrate', 'carbonate',
    'glucose', 'fructose', 'lactose', 'sucrose', 'ribose', 'xylose',
    'cellulose', 'starch', 'glycerol',
}

# Layer 2: chemical suffix pattern
_CHEM_SUFFIX = re.compile(
    r'(ine|one|ol\b|ene|ate|acid|aldehyde|ketone|ester|lactone|ose|ide|anol|amine)$'
)

# Layer 3: specific chemical prefix — must match to pass
# Blocks generic "Fatty Acid" while passing "Linoleic Acid", "Lactic Acid", etc.
_SPECIFIC_PREFIX = re.compile(
    r'^('
    r'meth|eth|prop|but|pent|hex|hept|oct|non|dec|undec|dodec|'
    r'tetradec|hexadec|octadec|eicos|docos|'
    r'acet|form|benz|tolu|phenyl|indol|pyrrol|furan|thio|'
    r'hydroxy|dihydroxy|dehydro|oxo|keto|'
    r'lactic|succinic|malic|citric|glutamic|aspartic|linoleic|'
    r'linolenic|oleic|palmitic|stearic|arachidonic|butyric|myristic|'
    r'malonaldehyde|benzaldehyde|acetaldehyde|hexanal|nonanal|decanal|'
    r'carnosine|anserine|creatine|inosine|taurine|carnitine|'
    r'pyruvate|lactate|malonate|succinate|fumarate|citrate|'
    r'plasmany|phosphatid|sphingo|cholesterol|'
    r'd-lactic|l-lactic|dl-|d-|l-'
    r')',
    re.I
)

_MOL_PAT = re.compile(
    r'\b([A-Za-z][A-Za-z0-9\-]{2,}(?:\s+(?:acid|aldehyde|ketone|ester|lactone))?)\b'
)
_STOP = set(
    'the and for with from this that were have has into also using used between '
    'based study paper results method methods analysis engine engines model models '
    'data effect effects system systems sample samples figure table abstract'.split()
)


def is_real_molecule(name: str) -> bool:
    low = name.lower().strip()

    if low in _JUNK:                          # Layer 1a
        return False
    if low in _GENERIC_CLASSES:               # Layer 1b
        return False
    if low in _GENERIC_SINGLE:               # Layer 1c
        return False
    if not (_CHEM_SUFFIX.search(low) or ' acid' in low):  # Layer 2
        return False
    if len(low) < 5:                          # Layer 4
        return False

    # Layer 3 — specificity check
    # If name is 1 or 2 words, it MUST start with a specific chemical prefix.
    # 3+ word names are usually specific enough (e.g. "D-Lactic Acid").
    word_count = len(low.split())
    if word_count <= 2 and not _SPECIFIC_PREFIX.match(low):
        return False

    return True


def extract_molecule_mentions(text: str, max_molecules: int = 12) -> list:
    if not text:
        return []
    sentences = re.split(r'(?<=[.!?])\s+', text)
    freq, ctx = {}, {}
    for sent in sentences:
        if len(sent) < 20:
            continue
        for m in _MOL_PAT.finditer(sent):
            raw = m.group(1).strip()
            low = raw.lower()
            if low in _STOP:
                continue
            if not is_real_molecule(raw):
                continue
            freq[low] = freq.get(low, 0) + 1
            ctx.setdefault(low, []).append(sent)
    ranked = sorted(freq.items(), key=lambda kv: kv[1], reverse=True)[:max_molecules]
    return [
        (' '.join(p.capitalize() for p in low.split()), cnt, ctx.get(low, [])[:6])
        for low, cnt in ranked
    ]


# ==============================================================================
# SECTION 4 — ATTRIBUTE EXTRACTION  (compact keyword terms)
# ==============================================================================

_SMELL_MAP = {
    'vanilla':   'vanilla',   'floral':    'floral',
    'fruity':    'fruity',    'woody':     'woody',
    'spicy':     'spicy',     'citrus':    'citrus',
    'earthy':    'earthy',    'buttery':   'buttery',
    'smoky':     'smoky',     'musty':     'musty',
    'rancid':    'rancid',    'aroma':     'aromatic',
    'fragrance': 'fragrant',  'odor':      'odorous',
    'odour':     'odorous',   'scent':     'scented',
    'smell':     'odorant',   'volatile':  'volatile',
}
_TASTE_MAP = {
    'sweet':      'sweet',       'bitter':     'bitter',
    'sour':       'sour',        'umami':      'umami',
    'salty':      'salty',       'astringent': 'astringent',
    'caramel':    'caramel',     'nutty':      'nutty',
    'chocolate':  'chocolate',   'meaty':      'meaty',
    'savory':     'savory',      'savour':     'savory',
    'flavour':    'flavor',      'flavor':     'flavor',
    'taste':      'taste-active','palatab':    'palatable',
    'mouthfeel':  'mouthfeel',   'pungent':    'pungent',
    'acidic':     'acidic',
}
_USE_MAP = {
    'preservative':    'preservative',
    'antioxidant':     'antioxidant',
    'antimicrobial':   'antimicrobial',
    'additive':        'food additive',
    'ingredient':      'ingredient',
    'biomarker':       'biomarker',
    'marker':          'biomarker',
    'bioactive':       'bioactive',
    'flavor enhancer': 'flavor enhancer',
    'used for':        'functional use',
    'used as':         'functional use',
    'application':     'application',
    'fermentation':    'fermentation substrate',
    'curing':          'curing agent',
    'coloring':        'colorant',
    'colouring':       'colorant',
    'emulsif':         'emulsifier',
    'texturis':        'texturiser',
    'texturiz':        'texturiser',
    'tenderis':        'tenderiser',
    'tenderiz':        'tenderiser',
    'nutrient':        'nutrient',
    'supplement':      'supplement',
}


def _extract_terms(sentences: list, term_map: dict) -> str:
    found = []
    for sent in sentences:
        low = sent.lower()
        for trigger, canonical in term_map.items():
            if trigger in low and canonical not in found:
                found.append(canonical)
    return ', '.join(found[:6])


def summarize_attributes(sentences: list):
    return (
        _extract_terms(sentences, _SMELL_MAP),
        _extract_terms(sentences, _TASTE_MAP),
        _extract_terms(sentences, _USE_MAP),
    )


# ==============================================================================
# SECTION 5 — MOLECULE CATEGORY CLASSIFIER
# ==============================================================================

_CATEGORY_RULES = [
    # FATS
    (re.compile(r'\b(linoleic|linolenic|oleic|palmitic|stearic|arachidonic|'
                r'butyric|capric|caprylic|lauric|myristic|eicosapentaenoic|'
                r'docosahexaenoic|nervonic)\b', re.I), 'Fats'),
    (re.compile(r'\b\w+(enoic|anoic)\s+acid\b', re.I), 'Fats'),
    (re.compile(r'\b(triglyceride|diglyceride|monoglyceride|glycerol|'
                r'phospholipid|lysophospholipid|sphingomyelin|ceramide|'
                r'cholesterol|sitosterol|stigmasterol|plasmalogen|'
                r'plasmanylethanolamine|phosphatidyl\w+)\b', re.I), 'Fats'),
    (re.compile(r'\b\w+(lactone|ester)\b', re.I), 'Fats'),
    (re.compile(r'\b(hexanal|nonanal|decanal|octanal|pentanal|heptanal|'
                r'benzaldehyde|acetaldehyde|malonaldehyde|'
                r'methylketone|diacetyl|acetoin)\b', re.I), 'Fats'),
    (re.compile(r'\b(lactic\s+acid|acetic\s+acid|succinic\s+acid|'
                r'malic\s+acid|citric\s+acid|fumaric\s+acid|'
                r'pyruvic\s+acid|oxaloacetic|malonic\s+acid|'
                r'tricarboxylic\s+acid|butyrolactone|butanediol|'
                r'ethanol|acetate|aconitate|monophosphate|phosphate)\b', re.I), 'Fats'),
    # PEPTIDES
    (re.compile(r'\b(carnosine|anserine|balenine|glutathione|homocarnosine)\b',
                re.I), 'Peptides'),
    (re.compile(r'\b(dipeptide|tripeptide|oligopeptide|bioactive\s+peptide|'
                r'antimicrobial\s+peptide|hydrolysate|peptide)\b',
                re.I), 'Peptides'),
    # PROTEINS
    (re.compile(r'\b(myosin|actin|tropomyosin|troponin|titin|nebulin|'
                r'collagen|elastin|fibronectin|laminin|gelatin|gelatine)\b',
                re.I), 'Proteins'),
    (re.compile(r'\b(calpain|cathepsin|protease|peptidase|lipase|'
                r'oxidase|reductase|synthase|kinase|albumin|globulin|'
                r'hemoglobin|haemoglobin|myoglobin|ferritin|casein)\b',
                re.I), 'Proteins'),
    (re.compile(r'\b(glycine|alanine|valine|leucine|isoleucine|proline|'
                r'phenylalanine|tryptophan|methionine|serine|threonine|'
                r'cysteine|tyrosine|asparagine|glutamine|aspartate|'
                r'glutamate|glutamic\s+acid|aspartic\s+acid|lysine|'
                r'arginine|histidine|hydroxyproline|carnitine|taurine|'
                r'inosine|hypoxanthine|creatine|creatinine)\b', re.I), 'Proteins'),
    (re.compile(r'\bprotein\b', re.I), 'Proteins'),
]


def classify_molecule(name: str) -> str:
    for pattern, label in _CATEGORY_RULES:
        if pattern.search(name):
            return label
    return 'Unclassified'



# ==============================================================================
# SECTION 6 — PUBCHEM PHYSICAL PROPERTIES LOOKUP
# ==============================================================================
#
# WHY PubChem and not the abstract?
#   Melting point and water solubility are fixed physical constants — they are
#   almost never stated in an abstract.  PubChem is the authoritative free
#   database for this data.  No API key needed.
#
# Strategy (two-step):
#   Step A — name -> CID  (PubChem Compound ID)
#   Step B — CID  -> properties JSON  (MeltingPoint, Solubility)
#
# Return format:
#   melting_point    : "86-88 C"  or  None
#   water_solubility : "Soluble in water" / "Insoluble"  or  None
#
# Caching: in-process dict so the same molecule is never looked up twice.

# Cache: mol_name_lower -> full props dict
_pubchem_cache = {}


def _pubchem_cid(mol_name):
    """Resolves molecule name to PubChem CID integer, or None."""
    url = (
        "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/"
        + requests.utils.quote(mol_name)
        + "/cids/JSON"
    )
    try:
        r = requests.get(url, timeout=10)
        if not r.ok:
            return None
        cids = r.json().get("IdentifierList", {}).get("CID", [])
        return cids[0] if cids else None
    except Exception:
        return None


def _first_string(section_info_list):
    """Extracts the first non-empty String value from a PubChem Information list."""
    for info in section_info_list:
        val = (
            info.get("Value", {})
                .get("StringWithMarkup", [{}])[0]
                .get("String", "")
        )
        if val and val.strip():
            return val.strip()
    return None


def _all_strings(section_info_list, max_vals=6):
    """Collects up to max_vals non-empty String values from an Information list."""
    vals = []
    for info in section_info_list:
        for swm in info.get("Value", {}).get("StringWithMarkup", []):
            v = swm.get("String", "").strip()
            if v and v not in vals:
                vals.append(v)
                if len(vals) >= max_vals:
                    return vals
    return vals


# ── Sensory keyword normaliser ────────────────────────────────────────────────
# Maps PubChem raw text -> compact canonical terms.
# WHY so many entries: PubChem descriptions are unpredictable.
# EPA is described as "polyunsaturated fatty acid found in marine food chain"
# — no odor/taste section at all. We need to catch terms from description text.

_ODOR_KEYWORDS = {
    # Classic sensory descriptors
    'vanilla': 'vanilla',       'caramel': 'caramel',
    'floral': 'floral',         'rose': 'floral',
    'fruity': 'fruity',         'fruit': 'fruity',
    'apple': 'fruity',          'banana': 'fruity',
    'woody': 'woody',           'wood': 'woody',
    'spicy': 'spicy',           'pepper': 'spicy',
    'citrus': 'citrus',         'lemon': 'citrus',
    'orange': 'citrus',         'lime': 'citrus',
    'earthy': 'earthy',         'mushroom': 'earthy',
    'buttery': 'buttery',       'butter': 'buttery',
    'cream': 'creamy',          'milky': 'creamy',
    'smoky': 'smoky',           'smoke': 'smoky',
    'musty': 'musty',           'mold': 'musty',
    'rancid': 'rancid',         'stale': 'rancid',
    'nutty': 'nutty',           'almond': 'nutty',
    'roast': 'roasted',         'roasted': 'roasted',
    'toast': 'toasty',          'burnt': 'burnt',
    'char': 'charred',          'carameliz': 'caramelized',
    'sweet': 'sweet',           'honey': 'honey-like',
    'sour': 'sour',             'acetic': 'vinegar-like',
    'vinegar': 'vinegar-like',  'ferment': 'fermented',
    'yeast': 'yeasty',          'bread': 'bread-like',
    'meat': 'meaty',            'meaty': 'meaty',
    'broth': 'broth-like',      'beef': 'meaty',
    'fatty': 'fatty',           'waxy': 'waxy',
    'fishy': 'fishy',           'fish': 'fishy',
    'marine': 'marine',         'ocean': 'marine',
    'green': 'green/grassy',    'grass': 'green/grassy',
    'hay': 'hay-like',          'herbal': 'herbal',
    'mint': 'minty',            'menthol': 'minty',
    'camphor': 'camphoraceous', 'pine': 'piney',
    'alcoholic': 'alcoholic',   'ethereal': 'ethereal',
    'solvent': 'solvent-like',  'chemical': 'chemical',
    'pleasant': 'pleasant',     'agreeable': 'pleasant',
    'pungent': 'pungent',       'sharp': 'sharp',
    'sulfur': 'sulfurous',      'sulphur': 'sulfurous',
    'egg': 'eggy',              'garlic': 'garlic-like',
    'onion': 'onion-like',      'cheese': 'cheesy',
    'cheesy': 'cheesy',         'lactic': 'lactic',
    'chocolate': 'chocolate',   'cocoa': 'chocolate',
    'coffee': 'coffee-like',    'cinnamon': 'spicy',
}

_TASTE_KEYWORDS = {
    'sweet': 'sweet',           'sugar': 'sweet',
    'bitter': 'bitter',         'astringent': 'astringent',
    'sour': 'sour',             'tart': 'tart',
    'acid': 'acidic',           'acidic': 'acidic',
    'umami': 'umami',           'savory': 'savory',
    'savoury': 'savory',        'brothy': 'umami',
    'meaty': 'umami/meaty',     'glutamat': 'umami',
    'salty': 'salty',           'mineral': 'mineral',
    'pungent': 'pungent',       'spicy': 'spicy',
    'hot': 'pungent/hot',       'burning': 'burning',
    'cooling': 'cooling',       'refreshing': 'cooling',
    'metallic': 'metallic',     'iron': 'metallic',
    'bland': 'bland',           'neutral': 'neutral',
    'fatty': 'fatty/oily',      'oily': 'fatty/oily',
    'fishy': 'fishy',           'rancid': 'rancid',
    'cheesy': 'cheesy',         'fermented': 'fermented',
    'lactic': 'lactic/sour',    'vinegar': 'vinegar-like',
    'mouthfeel': 'mouthfeel',   'smooth': 'smooth',
    'creamy': 'creamy',         'watery': 'watery',
}

_USE_KEYWORDS = {
    # Food & flavour
    'flavoring agent': 'flavoring agent',
    'flavoring': 'flavoring agent',
    'flavouring': 'flavoring agent',
    'flavor': 'flavoring agent',
    'food additive': 'food additive',
    'food ingredient': 'food ingredient',
    'additive': 'food additive',
    'food industry': 'food industry use',
    'food chain': 'food chain component',
    'dietary': 'dietary component',
    'nutrition': 'nutritional',
    'nutrient': 'nutrient',
    'supplement': 'dietary supplement',
    'omega': 'omega fatty acid supplement',
    'fish oil': 'fish oil component',
    # Preservation & safety
    'preservative': 'preservative',
    'antimicrobial': 'antimicrobial',
    'antifungal': 'antifungal',
    'antioxidant': 'antioxidant',
    'spoilage': 'spoilage indicator',
    'indicator': 'spoilage indicator',
    'curing': 'curing agent',
    'tenderis': 'tenderiser',
    'tenderiz': 'tenderiser',
    # Biomedical
    'pharmaceutical': 'pharmaceutical',
    'drug': 'pharmaceutical',
    'therapeutic': 'therapeutic',
    'anti-inflammatory': 'anti-inflammatory',
    'cardiovascular': 'cardiovascular health',
    'lipid-lowering': 'lipid-lowering',
    'omega-3': 'omega-3 supplement',
    'omega-6': 'omega-6 supplement',
    'biomarker': 'biomarker',
    'metabolite': 'metabolite',
    # Industrial
    'fragrance': 'fragrance ingredient',
    'perfume': 'fragrance ingredient',
    'cosmetic': 'cosmetic ingredient',
    'emulsif': 'emulsifier',
    'surfactant': 'surfactant',
    'solvent': 'solvent',
    'lubricant': 'lubricant',
    'fermentation': 'fermentation product',
}


def _compact_sensory(raw_text, keyword_map):
    """
    Scans raw PubChem text for keyword triggers and returns
    deduplicated comma-separated canonical terms.
    e.g. "pleasant sweet caramel-like odor" -> "sweet, caramel, pleasant"

    WHY no raw fallback:
      Storing raw PubChem sentences like "A polyunsaturated fatty acid found
      in the marine food chain" in a 'use' field is noisy and inconsistent.
      If no keywords match, we return None so the field stays clean.
      The abstract fallback in the pipeline then gets a chance to fill it.
    """
    low = raw_text.lower()
    found = []
    for trigger, canonical in keyword_map.items():
        if trigger in low and canonical not in found:
            found.append(canonical)
    # Return None if nothing matched — do NOT fall back to raw text
    return ', '.join(found[:6]) if found else None


def _pubchem_full_record(cid):
    """
    Fetches the FULL PubChem record for a compound and extracts:
      melting_point, water_solubility, odor, taste, uses
    from multiple sections of the PubChem data tree.
    """
    result = {
        "melting_point":    None,
        "water_solubility": None,
        "smell":            None,
        "taste":            None,
        "use":              None,
    }

    # We fetch two endpoints:
    # 1. Experimental Properties   -> melting point, solubility, odor, taste
    # 2. Use and Manufacturing      -> uses / applications

    for heading_filter in ["Experimental+Properties", "Use+and+Manufacturing", "Names+and+Identifiers"]:
        url = (
            f"https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/"
            f"{cid}/JSON?heading={heading_filter}"
        )
        try:
            r = requests.get(url, timeout=15)
            if not r.ok:
                continue
            sections = r.json().get("Record", {}).get("Section", [])
        except Exception:
            continue

        def walk(sec_list):
            for sec in sec_list:
                heading = sec.get("TOCHeading", "")
                info    = sec.get("Information", [])

                # Melting point
                if "Melting Point" in heading and result["melting_point"] is None:
                    val = _first_string(info)
                    if val:
                        result["melting_point"] = val

                # Solubility
                if "Solubility" in heading and result["water_solubility"] is None:
                    val = _first_string(info)
                    if val:
                        result["water_solubility"] = val.split(";")[0].strip()

                # Odor — PubChem headings vary by compound
                if any(h in heading for h in (
                    "Odor", "Aroma", "Olfactory", "Organoleptic",
                    "Odor Threshold", "Odor and Aroma"
                )):
                    if result["smell"] is None:
                        vals = _all_strings(info)
                        if vals:
                            compact = _compact_sensory(' '.join(vals), _ODOR_KEYWORDS)
                            if compact:
                                result["smell"] = compact

                # Taste — PubChem heading: "Taste"
                if "Taste" in heading and result["taste"] is None:
                    vals = _all_strings(info)
                    if vals:
                        compact = _compact_sensory(' '.join(vals), _TASTE_KEYWORDS)
                        if compact:
                            result["taste"] = compact

                # Uses — multiple headings
                if any(h in heading for h in (
                    "Uses", "Use ", "Application", "Function",
                    "Flavoring Agent", "Food Additive", "Consumer Uses",
                    "Industrial Uses", "Therapeutic Uses"
                )):
                    if result["use"] is None:
                        vals = _all_strings(info, max_vals=6)
                        if vals:
                            compact = _compact_sensory(' '.join(vals), _USE_KEYWORDS)
                            if compact:
                                result["use"] = compact

                # Description fallback — scan any section's text for sensory info
                # WHY: EPA/DHA have no dedicated Odor/Taste sections; their
                # properties appear in general description: "found in marine food
                # chain", "fish oil", "anti-inflammatory", etc.
                if "Description" in heading or "Record Description" in heading:
                    desc_vals = _all_strings(info, max_vals=8)
                    if desc_vals:
                        desc_text = ' '.join(desc_vals)
                        if result["smell"] is None:
                            c = _compact_sensory(desc_text, _ODOR_KEYWORDS)
                            if c: result["smell"] = c
                        if result["taste"] is None:
                            c = _compact_sensory(desc_text, _TASTE_KEYWORDS)
                            if c: result["taste"] = c
                        if result["use"] is None:
                            c = _compact_sensory(desc_text, _USE_KEYWORDS)
                            if c: result["use"] = c

                if sec.get("Section"):
                    walk(sec["Section"])

        walk(sections)

    return result


def pubchem_lookup(mol_name):
    """
    Returns {melting_point, water_solubility, smell, taste, use} — all str|None.
    Cached per run — duplicate lookups cost nothing.
    """
    key = mol_name.lower().strip()
    if key in _pubchem_cache:
        return _pubchem_cache[key]

    cid = _pubchem_cid(mol_name)
    if not cid:
        empty = {"melting_point": None, "water_solubility": None,
                 "smell": None, "taste": None, "use": None}
        _pubchem_cache[key] = empty
        return empty

    props = _pubchem_full_record(cid)
    _pubchem_cache[key] = props
    time.sleep(0.35)   # polite PubChem rate-limit pause
    return props


# ==============================================================================
# SECTION 7 — CLAIM EXTRACTION
# ==============================================================================
#
# A "claim" is one factual statement about a molecule's property extracted
# from the abstract.  We generate one claim per (molecule, property) pair
# where we found evidence.
#
# claim_key   : "claim:<mol_slug>:<property>"   e.g. "claim:lactic-acid:taste"
# claim_text  : plain English sentence           e.g. "Lactic acid contributes
#               to sour taste in fermented meat products."
# confidence  : 0.0–1.0  (see rules below)
# stance      : "supports" | "neutral" | "contradicts"
#
# Confidence scoring rules:
#   +0.5  base for having any evidence at all
#   +0.2  abstract mentions the property word ≥2 times near the molecule
#   +0.2  paper has ≥50 citations (well-validated finding)
#   +0.1  abstract contains "significant", "demonstrated", "confirmed"
#   -0.3  abstract contains "may", "possible", "suggest", "unclear"
#   capped to [0.1, 1.0]
#
# Stance rules:
#   "contradicts" if abstract contains "not", "no significant", "failed to"
#                 near a taste/smell/use claim
#   "supports"    otherwise (default)
#   "neutral"     if confidence < 0.4

_STRONG_WORDS  = re.compile(r'\b(significant|demonstrated|confirmed|established|showed)\b', re.I)
_WEAK_WORDS    = re.compile(r'\b(may|might|possible|possibly|suggest|unclear|appears|could)\b', re.I)
_NEGATE_WORDS  = re.compile(r'\b(not|no significant|failed to|did not|does not|none)\b', re.I)

# Maps property → human-readable claim template
# {mol} and {prop_phrase} are filled in at runtime
_CLAIM_TEMPLATES = {
    'smell': '__MOL__ has been associated with aromatic/volatile odour properties (__PROP__) in meat.',
    'taste': '__MOL__ contributes to taste characteristics (__PROP__) in meat products.',
    'use':   '__MOL__ has functional relevance (__PROP__) in meat science applications.',
}


def _confidence_score(sents: list, citations: int, mol_name: str) -> float:
    text = ' '.join(sents).lower()
    score = 0.5
    mol_low = mol_name.lower()

    # Boost: molecule mentioned multiple times
    if text.count(mol_low) >= 2:
        score += 0.2
    # Boost: high-citation paper
    if citations >= 50:
        score += 0.2
    # Boost: strong confirmation words
    if _STRONG_WORDS.search(text):
        score += 0.1
    # Penalty: hedged language
    if _WEAK_WORDS.search(text):
        score -= 0.3

    return round(max(0.1, min(1.0, score)), 2)


def _stance(sents: list, confidence: float) -> str:
    text = ' '.join(sents).lower()
    if _NEGATE_WORDS.search(text):
        return 'contradicts'
    if confidence < 0.4:
        return 'neutral'
    return 'supports'


def extract_claims(mol_name: str, smell_kw: str, taste_kw: str, use_kw: str,
                   sents: list, citations: int) -> list:
    """
    Returns a list of claim dicts, one per non-empty property.
    Each dict: {key, text, confidence, stance}
    """
    mol_slug = re.sub(r'[^a-z0-9]+', '-', mol_name.lower()).strip('-')
    claims = []
    confidence = _confidence_score(sents, citations, mol_name)
    stance     = _stance(sents, confidence)

    for prop_key, kw_value in [('smell', smell_kw), ('taste', taste_kw), ('use', use_kw)]:
        if not kw_value:
            continue
        claim_key  = f'claim:{mol_slug}:{prop_key}'
        claim_text = (
            _CLAIM_TEMPLATES[prop_key]
            .replace('__MOL__', mol_name)
            .replace('__PROP__', kw_value)
        )
        claims.append({
            'key':        claim_key,
            'text':       claim_text,
            'confidence': confidence,
            'stance':     stance,
        })
    return claims


# ==============================================================================
# SECTION 8 — MISC HELPERS
# ==============================================================================

def slug_key(name: str) -> str:
    return 'mol:' + re.sub(r'[^a-z0-9]+', '-', name.lower()).strip('-')[:60]


def extract_keywords_from_title(title: str, max_kw: int = 12) -> str:
    stop = {
        'with','that','from','this','were','have','their','into','also',
        'using','used','such','between','based','study','paper','results',
        'method','methods','analysis','these','those','about',
    }
    text = re.sub(r'[^a-z0-9\s]', ' ', title.lower())
    words = [w for w in text.split() if len(w) >= 4 and w not in stop]
    freq = {}
    for w in words:
        freq[w] = freq.get(w, 0) + 1
    top = sorted(freq.items(), key=lambda kv: kv[1], reverse=True)[:max_kw]
    return ', '.join(w for w, _ in top)


# ==============================================================================
# SECTION 9 — MAIN PIPELINE
# ==============================================================================

def run(query: str, top_n: int = 20,
        max_molecules_per_paper: int = 12, dry_run: bool = False):
    """
    Step 1  Fetch top-N papers from OpenAlex (sorted by citations)
    Step 2  Per paper: find or create Source row in Airtable
    Step 3  Reconstruct abstract
    Step 4  Extract molecules → classify → upsert Molecules
    Step 5  Extract claims    → upsert Claims
                               → link Claim → Molecule + Source
    """
    print(f'\n{"="*64}')
    print(f'Query   : {query}')
    print(f'Top-N   : {top_n}  |  dry_run={dry_run}')
    print(f'{"="*64}\n')

    # Step 1 ──────────────────────────────────────────────────────────────────
    print('Step 1: Fetching papers from OpenAlex ...')
    papers = openalex_top_papers(query, total=top_n)
    print(f'  -> {len(papers)} papers fetched\n')

    total_mols   = 0
    total_claims = 0
    skipped      = 0

    for idx, paper in enumerate(papers, 1):
        title     = paper['title']
        doi_url   = paper['doi_url']
        oa_id     = paper['oa_id']
        citations = paper['citations']

        print(f'[{idx:02d}/{top_n}] {title[:72]}')

        # Step 3: rebuild abstract ────────────────────────────────────────────
        abstract = reconstruct_abstract(paper['inv_idx'])
        if not abstract:
            print('        SKIP - no abstract\n')
            skipped += 1
            continue

        # Step 4a: extract molecules ───────────────────────────────────────────
        mols = extract_molecule_mentions(abstract, max_molecules=max_molecules_per_paper)
        if not mols:
            print('        SKIP - no molecules found\n')
            skipped += 1
            continue

        print(f'        {len(mols)} molecules | {citations} citations | {paper["year"]}')

        # Step 2: ensure Source row ───────────────────────────────────────────
        url_key = doi_url or oa_id
        src_id  = None
        if not dry_run:
            src_id = airtable_find_source_by_url(url_key)
            if src_id:
                print(f'        Source exists  -> {src_id}')
            else:
                src_fields = {}
                _add(src_fields, SRC_F_NAME,     title)
                _add(src_fields, SRC_F_URL,      url_key)
                _add(src_fields, SRC_F_YEAR,     paper['year'])
                _add(src_fields, SRC_F_VENUE,    paper['venue'])
                _add(src_fields, SRC_F_AUTHORS,  ', '.join(paper['authors'][:6]))
                _add(src_fields, SRC_F_CITES,    citations)
                _add(src_fields, SRC_F_QUERY,    query)
                _add(src_fields, SRC_F_KEYWORDS, extract_keywords_from_title(title))
                src_id = airtable_create_source(src_fields)
                print(f'        Source created -> {src_id}')
                time.sleep(0.2)

        # Step 4b: upsert molecules + Step 5: upsert claims ───────────────────
        for mol_name, count, sents in mols:
            category = classify_molecule(mol_name)
            mol_key  = slug_key(mol_name)

            # ── PubChem lookup (primary source for ALL properties) ────────────
            # WHY PubChem first:
            #   smell/taste/use from abstracts = almost always empty because
            #   papers don't describe sensory properties inline per molecule.
            #   PubChem has dedicated Odor, Taste, Uses sections per compound.
            print(f'         PubChem lookup: {mol_name} ...')
            pc         = pubchem_lookup(mol_name)
            melting_pt = pc.get('melting_point')
            solubility = pc.get('water_solubility')
            pc_smell   = pc.get('smell')
            pc_taste   = pc.get('taste')
            pc_use     = pc.get('use')

            # ── Abstract fallback (only if PubChem returned nothing) ──────────
            # WHY keep abstract fallback:
            #   Some niche / newly-described molecules have no PubChem entry.
            #   In those cases, abstract context sentences are better than nothing.
            abs_smell, abs_taste, abs_use = summarize_attributes(sents)
            smell_kw = pc_smell or abs_smell or None
            taste_kw = pc_taste or abs_taste or None
            use_kw   = pc_use   or abs_use   or None

            # ── dry run output ────────────────────────────────────────────────
            if dry_run:
                src_tag = "PC" if pc_smell or pc_taste or pc_use else "abstract"
                print(f'  [MOL]  {mol_name:<30} | {category:<12} | src={src_tag}')
                print(f'         mp={melting_pt or "-"} | sol={solubility or "-"}')
                print(f'         smell={smell_kw or "-"}')
                print(f'         taste={taste_kw or "-"}')
                print(f'         use={use_kw or "-"}')
                claims = extract_claims(mol_name, smell_kw, taste_kw, use_kw,
                                        sents, citations)
                for c in claims:
                    print(f'  [CLM]    key={c["key"]}')
                    print(f'           text={c["text"]}')
                    print(f'           conf={c["confidence"]}  stance={c["stance"]}')
                total_mols   += 1
                total_claims += len(claims)
                continue

            # ── live write: molecule ──────────────────────────────────────────
            mol_fields = {}
            _add(mol_fields, MOL_F_NAME,       mol_name)
            _add(mol_fields, MOL_F_COUNT,      count)
            _add(mol_fields, MOL_F_CATEGORY,   category)
            _add(mol_fields, MOL_F_SMELL,      smell_kw)
            _add(mol_fields, MOL_F_TASTE,      taste_kw)
            _add(mol_fields, MOL_F_USE,        use_kw)
            _add(mol_fields, MOL_F_MELTING,    melting_pt)
            _add(mol_fields, MOL_F_SOLUBILITY, solubility)

            mol_id = airtable_upsert_by_key(MOLECULES_TABLE, MOL_F_KEY, mol_key, mol_fields)
            if src_id:
                airtable_append_links_unique(
                    MOLECULES_TABLE, mol_id, MOL_F_LINK_SOURCES, [src_id]
                )
            total_mols += 1
            time.sleep(0.15)

            # ── live write: claims ────────────────────────────────────────────
            claims = extract_claims(mol_name, smell_kw, taste_kw, use_kw,
                                    sents, citations)
            for c in claims:
                # Skip claims that have no text — nothing useful to store
                if not c.get('text'):
                    continue

                clm_fields = {}
                # claim_text is the primary field in your Claims table.
                # It must be non-empty for Airtable to accept the record.
                _add(clm_fields, CLM_F_TEXT,       c['text'])
                _add(clm_fields, CLM_F_CONFIDENCE, c['confidence'])
                _add(clm_fields, CLM_F_STANCE,     c['stance'])

                clm_id = airtable_upsert_by_key(
                    CLAIMS_TABLE, CLM_F_KEY, c['key'], clm_fields
                )
                # Link claim -> molecule
                airtable_append_links_unique(
                    CLAIMS_TABLE, clm_id, CLM_F_LINK_MOLS, [mol_id]
                )
                # Link claim -> source
                if src_id:
                    airtable_append_links_unique(
                        CLAIMS_TABLE, clm_id, CLM_F_LINK_SOURCES, [src_id]
                    )
                total_claims += 1
                time.sleep(0.15)

        print()

    # Summary ─────────────────────────────────────────────────────────────────
    print(f'{"="*64}')
    print(f'Done.')
    print(f'  Papers processed  : {len(papers) - skipped}')
    print(f'  Papers skipped    : {skipped}')
    print(f'  Molecules written : {total_mols}')
    print(f'  Claims written    : {total_claims}')
    print(f'{"="*64}\n')


# ==============================================================================
# ENTRY POINT
# ==============================================================================

if __name__ == '__main__':
    _query   = os.environ.get('MVP_QUERY',    'Flavor and Metabolite Profiles of Meat')
    _dry     = os.environ.get('DRY_RUN',      '1').strip() == '1'
    _top_n   = int(os.environ.get('TOP_N',    '20'))
    _max_mol = int(os.environ.get('MAX_MOLS', '12'))

    run(query=_query, top_n=_top_n, max_molecules_per_paper=_max_mol, dry_run=_dry)
