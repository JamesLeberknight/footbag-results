#!/usr/bin/env python3
import csv, json, sys, os
from pathlib import Path

# Increase field limit for large text blobs
csv.field_size_limit(sys.maxsize)

# --- PATH SETTINGS ---
ROOT = Path(__file__).resolve().parent.parent
OUT  = ROOT / "out"
STAGE2_CSV     = OUT / "stage2_canonical_events.csv"
SCAN_INDEX_CSV = ROOT / "inputs" / "magazine_scan_index.csv"
OUT_HTML       = OUT / "fbw_archive_viewer.html"

def get_fingerprint(year, name):
    """Creates a unique string from year and name to bypass ID mismatches."""
    if not year or not name: return ""
    combined = f"{year}{name}"
    return "".join(filter(str.isalnum, combined)).lower()

def load_data():
    # 1. Load Scan Index and create fingerprints
    scans = {}
    if not SCAN_INDEX_CSV.exists():
        print(f"!! Error: {SCAN_INDEX_CSV} not found.")
        return []

    with open(SCAN_INDEX_CSV, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for r in reader:
            # Column naming drift: older indexes used `source_file`, current uses `source_jpg`.
            fname = r.get("source_jpg") or r.get("source_file") or ""
            if not fname: continue
            
            # Use ID if available, but always create a Fingerprint fallback
            eid = r.get("event_id", "")
            fingerprint = get_fingerprint(r.get("year"), r.get("event_name"))
            
            if eid: scans[eid] = fname
            if fingerprint: scans[fingerprint] = fname

    # 2. Match with Stage 2 Events
    matched = []
    if not STAGE2_CSV.exists():
        print(f"!! Error: {STAGE2_CSV} not found.")
        return []

    with open(STAGE2_CSV, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for r in reader:
            eid = r.get("event_id", "")
            year = r.get("year", "")
            name = r.get("event_name", "")
            fingerprint = get_fingerprint(year, name)
            
            # Check for match in order: ID -> Fingerprint
            img_file = scans.get(eid) or scans.get(fingerprint)

            if img_file:
                matched.append({
                    "id": eid if eid else fingerprint,
                    "label": f"{year} - {name}",
                    "text": r.get("results_raw", "No raw results found in CSV."),
                    "jpg": img_file
                })

    print(f"Scan Index size: {len(scans)}")
    print(f"Successfully matched {len(matched)} events.")
    return sorted(matched, key=lambda x: x['label'], reverse=True)

# --- HTML/JS REMAINS THE SAME (Simplified for JPG + Text) ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>FBW Archive Reviewer</title>
    <style>
        body { margin:0; display:grid; grid-template-columns: 320px 1fr 1fr; height:100vh; font-family:sans-serif; background:#111; color:#eee; }
        #sidebar { background:#1e1e1e; border-right:1px solid #333; display:flex; flex-direction:column; overflow:hidden; }
        #list { flex:1; overflow-y:auto; }
        .item { padding:12px; border-bottom:1px solid #222; cursor:pointer; font-size:11px; }
        .item:hover { background:#2a2d2e; }
        .item.active { background:#094771; font-weight:bold; }
        #text-pane { background:white; color:#222; overflow-y:auto; padding:20px; border-right:1px solid #ccc; }
        pre { white-space:pre-wrap; font-family:monospace; font-size:11px; background:#f8f8f8; padding:15px; border:1px solid #ddd; }
        #scan-pane { background:#2d2d2d; display:flex; flex-direction:column; overflow:hidden; }
        #toolbar { padding:10px; background:#1a1a1a; display:flex; gap:10px; align-items:center; }
        #viewport { flex:1; overflow:auto; display:flex; justify-content:center; align-items:flex-start; padding:40px; }
        img { transition: transform 0.2s; box-shadow: 0 0 40px black; transform-origin: center center; }
        button { cursor:pointer; padding:6px 12px; background:#444; color:white; border:1px solid #555; border-radius:3px; }
    </style>
</head>
<body>
    <div id="sidebar"><div id="list"></div></div>
    <div id="text-pane"><h3 id="title">Select an Entry</h3><pre id="mirror"></pre></div>
    <div id="scan-pane">
        <div id="toolbar">
            <button onclick="rotate(-90)">↺</button>
            <button onclick="rotate(90)">Rotate ↻</button>
            <span id="fname" style="font-size:10px; opacity:0.5; margin-left:auto;"></span>
        </div>
        <div id="viewport"><img id="pic" style="display:none;"></div>
    </div>
    <script>
        const DATA = %JSON_DATA%;
        let rotation = 0;
        const listEl = document.getElementById('list');
        DATA.forEach(ev => {
            const div = document.createElement('div');
            div.className = 'item';
            div.id = 'item-' + ev.id;
            div.innerText = ev.label;
            div.onclick = () => {
                document.querySelectorAll('.item').forEach(i => i.classList.remove('active'));
                div.classList.add('active');
                document.getElementById('title').innerText = ev.label;
                document.getElementById('mirror').innerText = ev.text;
                document.getElementById('fname').innerText = ev.jpg;
                rotation = 0;
                const img = document.getElementById('pic');
                img.style.transform = 'rotate(0deg)';
                img.style.margin = '0';
                img.src = "scans/" + ev.jpg;
                img.style.display = "block";
            };
            listEl.appendChild(div);
        });
        function rotate(d) {
            rotation += d;
            const img = document.getElementById('pic');
            img.style.transform = `rotate(${rotation}deg)`;
            img.style.margin = (Math.abs(rotation)/90)%2 === 1 ? "150px 0" : "0";
        }
    </script>
</body>
</html>
"""

def main():
    events = load_data()
    if not events:
        print("!! Still matched 0 events. Please check if magazine_scan_index.csv has 'year', 'event_name', and 'source_jpg' columns (or legacy 'source_file').")
        return

    html = HTML_TEMPLATE.replace("%JSON_DATA%", json.dumps(events))
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Success! Viewer created at {OUT_HTML}")

if __name__ == "__main__":
    main()
