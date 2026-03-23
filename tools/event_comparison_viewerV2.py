# --- 1. Enhanced Image Mapping Logic ---
def load_scan_index() -> dict[str, str]:
    """Returns a map of event_id -> filename, with fuzzy fallback for Vols 2-14."""
    index = {}
    if not SCAN_INDEX_CSV.exists():
        return index
    
    with open(SCAN_INDEX_CSV, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            eid = r.get("event_id")
            fname = r.get("source_file")
            if eid and fname:
                index[eid] = fname
                # Also create a normalized key (Year + Name) for fuzzy matching
                fuzzy_key = f"{r.get('year')}_{r.get('event_name','')}".lower().replace(" ","")
                index[fuzzy_key] = fname
    return index

# --- 2. Interactive HTML Template (Update the selectEvent & UI) ---
# Replace the scan-container div and script in your HTML_TEMPLATE string:

HTML_TEMPLATE = """
...
<div id="scan-pane" style="background:#2d2d2d; display:flex; flex-direction:column; overflow:hidden; position:relative;">
    <div id="scan-tools" style="background:#1a1a1a; padding:8px; display:flex; gap:10px; border-bottom:1px solid #444; z-index:100;">
        <button onclick="rotateImg(-90)" style="cursor:pointer; padding:4px 12px; border-radius:3px; border:1px solid #666;">Rotate ↺</button>
        <button onclick="rotateImg(90)" style="cursor:pointer; padding:4px 12px; border-radius:3px; border:1px solid #666;">Rotate ↻</button>
        <span id="scan-name" style="color:#aaa; font-family:monospace; font-size:11px; align-self:center; margin-left:auto;"></span>
    </div>
    
    <div id="img-viewport" style="flex:1; overflow:auto; display:flex; align-items:flex-start; justify-content:center; padding:40px;">
        <img id="scan-img" style="display:none; transition: transform 0.2s cubic-bezier(0.4, 0, 0.2, 1); box-shadow: 0 0 30px rgba(0,0,0,0.5); transform-origin: center center;">
    </div>
</div>

<script>
let rotation = 0;

function selectEvent(id) {
    const ev = EVENTS.find(e => e.id === id);
    const img = document.getElementById('scan-img');
    const nameLabel = document.getElementById('scan-name');
    
    rotation = 0; // Reset for new event
    img.style.transform = `rotate(0deg)`;
    img.style.margin = "0";

    if (ev && ev.scan_jpg) {
        // Adjust this path to match your local 'out/scans/' directory
        img.src = "scans/" + ev.scan_jpg;
        img.style.display = "block";
        nameLabel.innerText = ev.scan_jpg;
    } else {
        img.style.display = "none";
        nameLabel.innerText = "No scan mapped";
    }
}

function rotateImg(deg) {
    rotation += deg;
    const img = document.getElementById('scan-img');
    img.style.transform = `rotate(${rotation}deg)`;
    
    // Adjust margins for landscape/portrait shifts to prevent overlap
    const isVertical = (Math.abs(rotation) / 90) % 2 === 1;
    img.style.margin = isVertical ? "120px 0" : "0";
}
</script>
...
"""
