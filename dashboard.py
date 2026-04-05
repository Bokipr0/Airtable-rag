"""
Flavor Pipeline Dashboard  — run with:  python dashboard.py
Opens at http://localhost:5050
"""
import os, json, subprocess, threading, time
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

ROOT = Path(__file__).parent

# ── shared state ─────────────────────────────────────────────────────────────
_proc: subprocess.Popen | None = None
_proc_lock = threading.Lock()

def pipeline_running():
    with _proc_lock:
        return _proc is not None and _proc.poll() is None

def read_log(n=80):
    lf = ROOT / "pipeline_run.log"
    if not lf.exists():
        return "No log yet."
    lines = lf.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-n:])

def read_state():
    sf = ROOT / "cursor_state.json"
    if not sf.exists():
        return {}
    try:
        with sf.open() as f:
            s = json.load(f)
        s["seen_dois_count"] = len(s.get("seen_dois", []))
        s.pop("seen_dois", None)
        return s
    except Exception:
        return {}

def read_output_summary():
    of = ROOT / "pipeline_output.json"
    if not of.exists():
        return {}
    try:
        with of.open() as f:
            data = json.load(f)
        branches = {}
        dbs      = {}
        for rec in data:
            b = rec.get("branch","unknown")
            d = rec.get("db","unknown")
            branches[b] = branches.get(b,0) + 1
            dbs[d]      = dbs.get(d,0) + 1
        return {"total": len(data), "branches": branches, "dbs": dbs}
    except Exception:
        return {}

# ── HTML page ────────────────────────────────────────────────────────────────

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Flavor Intelligence Pipeline</title>
<style>
  :root {
    --bg:#0f1117; --surface:#1a1d27; --card:#21253a;
    --accent:#4f8ef7; --accent2:#7c5ce0; --success:#2ecc71;
    --warn:#f39c12; --danger:#e74c3c; --text:#e0e4f0; --muted:#6b7280;
    --border:#2d3350; --radius:10px; --mono:'JetBrains Mono',monospace;
  }
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:system-ui,sans-serif;font-size:14px;min-height:100vh}
  header{background:var(--surface);border-bottom:1px solid var(--border);padding:14px 24px;
         display:flex;align-items:center;gap:12px}
  header h1{font-size:18px;font-weight:600;color:var(--text)}
  header span{font-size:11px;background:var(--accent2);color:#fff;padding:2px 8px;border-radius:20px}
  .main{display:grid;grid-template-columns:1fr 1fr;gap:18px;padding:20px 24px;max-width:1400px;margin:0 auto}
  @media(max-width:900px){.main{grid-template-columns:1fr}}
  .card{background:var(--card);border:1px solid var(--border);border-radius:var(--radius);padding:18px}
  .card h2{font-size:13px;font-weight:600;color:var(--muted);text-transform:uppercase;
           letter-spacing:.05em;margin-bottom:14px;display:flex;align-items:center;gap:6px}
  .card h2 .dot{width:8px;height:8px;border-radius:50%;background:var(--accent)}
  .full{grid-column:1/-1}
  /* buttons */
  .btn{display:inline-flex;align-items:center;gap:6px;padding:8px 16px;border:none;
       border-radius:6px;font-size:13px;font-weight:500;cursor:pointer;transition:.15s}
  .btn-primary{background:var(--accent);color:#fff}
  .btn-primary:hover{filter:brightness(1.15)}
  .btn-danger{background:var(--danger);color:#fff}
  .btn-danger:hover{filter:brightness(1.15)}
  .btn-ghost{background:var(--surface);color:var(--text);border:1px solid var(--border)}
  .btn-ghost:hover{border-color:var(--accent);color:var(--accent)}
  .btn:disabled{opacity:.4;cursor:not-allowed}
  /* status pill */
  .pill{display:inline-block;padding:3px 10px;border-radius:20px;font-size:11px;font-weight:600}
  .pill-run{background:#1a3a28;color:var(--success)}
  .pill-idle{background:#1a1d2e;color:var(--muted)}
  /* log */
  .log{background:#080b13;border:1px solid var(--border);border-radius:6px;
       padding:12px;font-family:var(--mono);font-size:11px;line-height:1.6;
       max-height:380px;overflow-y:auto;color:#a8b4cc;white-space:pre-wrap;word-break:break-all}
  /* form */
  .form-row{display:flex;flex-wrap:wrap;gap:10px;margin-bottom:10px}
  label{font-size:12px;color:var(--muted);margin-bottom:4px;display:block}
  input[type=text],input[type=number],select{
    background:var(--surface);border:1px solid var(--border);color:var(--text);
    border-radius:6px;padding:7px 10px;font-size:13px;width:100%}
  input:focus,select:focus{outline:none;border-color:var(--accent)}
  .check-group{display:flex;flex-wrap:wrap;gap:10px}
  .check-group label{display:flex;align-items:center;gap:5px;color:var(--text);
                     font-size:12px;cursor:pointer}
  /* stat cards */
  .stats{display:grid;grid-template-columns:repeat(auto-fill,minmax(120px,1fr));gap:10px}
  .stat{background:var(--surface);border:1px solid var(--border);border-radius:8px;
        padding:12px;text-align:center}
  .stat .val{font-size:24px;font-weight:700;color:var(--accent)}
  .stat .lbl{font-size:11px;color:var(--muted);margin-top:2px}
  /* table */
  table{width:100%;border-collapse:collapse;font-size:12px}
  th{color:var(--muted);text-align:left;padding:6px 8px;border-bottom:1px solid var(--border)}
  td{padding:6px 8px;border-bottom:1px solid var(--border)22}
  tr:hover td{background:#ffffff06}
  /* progress bar */
  .bar-wrap{background:var(--surface);border-radius:20px;height:8px;margin:6px 0}
  .bar{height:8px;border-radius:20px;background:linear-gradient(90deg,var(--accent),var(--accent2));
       transition:width .5s}
  /* tag */
  .tag{display:inline-block;padding:1px 6px;border-radius:4px;font-size:10px;
       font-weight:600;background:var(--surface);border:1px solid var(--border)}
  .tag-maillard{border-color:#f39c12;color:#f39c12}
  .tag-lipid{border-color:#e74c3c;color:#e74c3c}
  .tag-volatile{border-color:#9b59b6;color:#9b59b6}
  .tag-precursor{border-color:#2ecc71;color:#2ecc71}
  .tag-analog{border-color:#4f8ef7;color:#4f8ef7}
  .tag-analytical{border-color:#1abc9c;color:#1abc9c}
  .tag-other{border-color:var(--muted);color:var(--muted)}
</style>
</head>
<body>
<header>
  <h1>🧪 Flavor Intelligence Pipeline</h1>
  <span id="status-pill" class="pill pill-idle">IDLE</span>
</header>

<div class="main">

  <!-- CONTROLS -->
  <div class="card">
    <h2><span class="dot"></span>Run Controls</h2>
    <div style="margin-bottom:14px">
      <label>Databases</label>
      <div class="check-group" id="db-checks">
        <label><input type="checkbox" value="openalex" checked> OpenAlex</label>
        <label><input type="checkbox" value="s2"       checked> Semantic Scholar</label>
        <label><input type="checkbox" value="pubmed"   checked> PubMed</label>
        <label><input type="checkbox" value="crossref" checked> CrossRef</label>
        <label><input type="checkbox" value="serpapi">          SerpAPI (needs key)</label>
      </div>
    </div>
    <div class="form-row">
      <div style="flex:1;min-width:100px">
        <label>Workers</label>
        <input type="number" id="workers" value="5" min="1" max="16">
      </div>
      <div style="flex:2;min-width:140px">
        <label>Keywords file</label>
        <input type="text" id="kw-file" value="keywords_bible.json">
      </div>
    </div>
    <div class="form-row">
      <div style="flex:1">
        <label>Output JSON</label>
        <input type="text" id="out-file" value="pipeline_output.json">
      </div>
    </div>
    <div class="form-row" style="margin-top:6px">
      <button class="btn btn-primary" id="btn-run"  onclick="startPipeline(false)">▶ Run Pipeline</button>
      <button class="btn btn-ghost"   id="btn-dry"  onclick="startPipeline(true)">🧪 Dry Run</button>
      <button class="btn btn-danger"  id="btn-stop" onclick="stopPipeline()" disabled>■ Stop</button>
      <button class="btn btn-ghost"               onclick="refreshAll()">↺ Refresh</button>
    </div>
  </div>

  <!-- STATS -->
  <div class="card">
    <h2><span class="dot" style="background:var(--success)"></span>Pipeline Stats</h2>
    <div class="stats" id="stats-grid">
      <div class="stat"><div class="val" id="stat-seen">—</div><div class="lbl">Seen DOIs</div></div>
      <div class="stat"><div class="val" id="stat-records">—</div><div class="lbl">Records</div></div>
      <div class="stat"><div class="val" id="stat-connections">—</div><div class="lbl">Connections</div></div>
      <div class="stat"><div class="val" id="stat-cursors">—</div><div class="lbl">Active Cursors</div></div>
    </div>
    <div style="margin-top:14px" id="branch-table"></div>
  </div>

  <!-- LOG -->
  <div class="card full">
    <h2><span class="dot" style="background:var(--warn)"></span>Live Log
      <span style="margin-left:auto;font-size:11px;font-weight:400;color:var(--muted)" id="log-time"></span>
    </h2>
    <div class="log" id="log-box">Waiting for pipeline run...</div>
  </div>

  <!-- KEYWORD MANAGER -->
  <div class="card full">
    <h2><span class="dot" style="background:var(--accent2)"></span>Keyword Bible
      <button class="btn btn-ghost" style="margin-left:auto;font-size:11px" onclick="loadKeywords()">↺ Reload</button>
    </h2>
    <div style="margin-bottom:10px;display:flex;gap:8px;flex-wrap:wrap">
      <select id="kw-branch" style="width:200px">
        <option>Maillard Reaction</option><option>Lipid Oxidation</option>
        <option>Volatile Compounds</option><option>Precursors</option>
        <option>Sulfur Chemistry</option><option>Heterocyclic Formation</option>
        <option>Analytical Methods</option><option>Meat Analogs</option>
        <option>Processes</option>
      </select>
      <select id="kw-priority" style="width:120px">
        <option>HIGH</option><option>MEDIUM</option><option>LOW</option>
      </select>
      <input type="text" id="kw-new" placeholder="New keyword..." style="flex:1;min-width:240px">
      <button class="btn btn-primary" onclick="addKeyword()">+ Add</button>
    </div>
    <div id="kw-table" style="max-height:320px;overflow-y:auto"></div>
  </div>

</div>

<script>
let keywords = [];

async function api(path, body=null) {
  const opts = body ? {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)} : {};
  const r = await fetch(path, opts);
  return r.json();
}

function selectedDBs() {
  return [...document.querySelectorAll('#db-checks input:checked')].map(i=>i.value);
}

async function startPipeline(dryRun) {
  const dbs     = selectedDBs();
  const workers = document.getElementById('workers').value;
  const kwFile  = document.getElementById('kw-file').value;
  const outFile = document.getElementById('out-file').value;
  if (!dbs.length) { alert('Select at least one database'); return; }
  await api('/api/start', {dbs, workers:parseInt(workers), kwFile, outFile, dryRun});
  setRunning(true);
  pollStatus();
}

async function stopPipeline() {
  await api('/api/stop');
  setRunning(false);
}

function setRunning(running) {
  document.getElementById('btn-run').disabled  = running;
  document.getElementById('btn-dry').disabled  = running;
  document.getElementById('btn-stop').disabled = !running;
  const pill = document.getElementById('status-pill');
  pill.textContent = running ? 'RUNNING' : 'IDLE';
  pill.className = 'pill ' + (running ? 'pill-run' : 'pill-idle');
}

async function pollStatus() {
  const data = await api('/api/status');
  setRunning(data.running);
  document.getElementById('log-box').textContent = data.log;
  document.getElementById('log-box').scrollTop = 9999;
  document.getElementById('log-time').textContent = new Date().toLocaleTimeString();

  if (data.state) {
    document.getElementById('stat-seen').textContent    = (data.state.seen_dois_count||0).toLocaleString();
    document.getElementById('stat-cursors').textContent = Object.keys(data.state.cursors||{}).length;
    const stats = data.state.stats || {};
    document.getElementById('stat-records').textContent     = (stats.last_records||0).toLocaleString();
    document.getElementById('stat-connections').textContent = (stats.last_connections||0).toLocaleString();
  }

  if (data.output) {
    renderBranchTable(data.output);
  }

  if (data.running) setTimeout(pollStatus, 3000);
}

function tagClass(branch) {
  const m = {
    'Maillard Reaction':'maillard','Lipid Oxidation':'lipid',
    'Volatile Compounds':'volatile','Precursors':'precursor',
    'Meat Analogs':'analog','Analytical Methods':'analytical'
  };
  return 'tag tag-' + (m[branch] || 'other');
}

function renderBranchTable(output) {
  if (!output.branches) return;
  const total = output.total || 1;
  let html = '<table><thead><tr><th>Branch</th><th>Records</th><th>Share</th></tr></thead><tbody>';
  const sorted = Object.entries(output.branches).sort((a,b)=>b[1]-a[1]);
  for (const [branch, count] of sorted) {
    const pct = Math.round(count/total*100);
    html += `<tr>
      <td><span class="${tagClass(branch)}">${branch}</span></td>
      <td>${count}</td>
      <td style="width:120px">
        <div class="bar-wrap"><div class="bar" style="width:${pct}%"></div></div>
        <span style="font-size:10px;color:var(--muted)">${pct}%</span>
      </td>
    </tr>`;
  }
  html += '</tbody></table>';
  document.getElementById('branch-table').innerHTML = html;
}

// ── Keyword Manager ──────────────────────────────────────────────────────────

async function loadKeywords() {
  const data = await api('/api/keywords');
  keywords = data.keywords || [];
  renderKeywords();
}

function renderKeywords() {
  const tc = tagClass;
  let html = '<table><thead><tr><th>Keyword</th><th>Branch</th><th>Priority</th><th></th></tr></thead><tbody>';
  keywords.forEach((kw, i) => {
    html += `<tr>
      <td style="max-width:360px">${kw.keyword}</td>
      <td><span class="${tc(kw.branch)}">${kw.branch}</span></td>
      <td style="color:${kw.priority==='HIGH'?'#2ecc71':kw.priority==='MEDIUM'?'#f39c12':'#6b7280'};font-weight:600">${kw.priority}</td>
      <td><button class="btn btn-ghost" style="padding:2px 8px;font-size:11px" onclick="removeKeyword(${i})">✕</button></td>
    </tr>`;
  });
  html += '</tbody></table>';
  document.getElementById('kw-table').innerHTML = html;
}

function addKeyword() {
  const kw = document.getElementById('kw-new').value.trim();
  if (!kw) return;
  keywords.push({
    keyword:  kw,
    branch:   document.getElementById('kw-branch').value,
    priority: document.getElementById('kw-priority').value,
  });
  document.getElementById('kw-new').value = '';
  api('/api/keywords/save', {keywords});
  renderKeywords();
}

function removeKeyword(i) {
  keywords.splice(i, 1);
  api('/api/keywords/save', {keywords});
  renderKeywords();
}

async function refreshAll() {
  await pollStatus();
  await loadKeywords();
}

// init
pollStatus();
loadKeywords();
setInterval(()=>{ if(document.getElementById('status-pill').textContent==='RUNNING') pollStatus(); }, 4000);
</script>
</body>
</html>"""

# ── HTTP handler ──────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):

    def log_message(self, *a):
        pass   # silence default HTTP log

    def _json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def _html(self, body):
        enc = body.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", len(enc))
        self.end_headers()
        self.wfile.write(enc)

    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("/", "/index.html"):
            self._html(HTML)
        elif path == "/api/status":
            self._json({
                "running": pipeline_running(),
                "log":     read_log(),
                "state":   read_state(),
                "output":  read_output_summary(),
            })
        elif path == "/api/keywords":
            kf = ROOT / "keywords_bible.json"
            if kf.exists():
                with kf.open() as f:
                    kws = json.load(f)
            else:
                kws = []
            self._json({"keywords": kws})
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        global _proc
        length = int(self.headers.get("Content-Length", 0))
        body   = json.loads(self.rfile.read(length) or b"{}")
        path   = urlparse(self.path).path

        if path == "/api/start":
            if not pipeline_running():
                dbs     = body.get("dbs", ["openalex","s2","pubmed","crossref"])
                workers = str(body.get("workers", 5))
                kwFile  = body.get("kwFile",  "keywords_bible.json")
                outFile = body.get("outFile", "pipeline_output.json")
                dry     = body.get("dryRun",  False)
                cmd     = ["python", str(ROOT/"pipeline.py"),
                           "--databases"] + dbs + [
                           "--workers", workers,
                           "--keywords", kwFile,
                           "--output",   outFile]
                if dry:
                    cmd.append("--dry-run")
                with _proc_lock:
                    _proc = subprocess.Popen(cmd, cwd=str(ROOT))
            self._json({"ok": True})

        elif path == "/api/stop":
            with _proc_lock:
                if _proc and _proc.poll() is None:
                    _proc.terminate()
            self._json({"ok": True})

        elif path == "/api/keywords/save":
            kws = body.get("keywords", [])
            kf  = ROOT / "keywords_bible.json"
            with kf.open("w") as f:
                json.dump(kws, f, indent=2)
            self._json({"ok": True, "count": len(kws)})

        else:
            self.send_response(404); self.end_headers()


# ─────────────────────────────────────────────────────────────────────────────

def main(port=5050):
    print(f"\n🧪 Flavor Pipeline Dashboard")
    print(f"   http://localhost:{port}")
    print(f"   Press Ctrl+C to stop\n")
    server = HTTPServer(("", port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nDashboard stopped.")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=5050)
    main(p.parse_args().port)
