"""Download ABS 2021 Census data from ArcGIS FeatureServer.
Pulls SA1, SA2, LGA population + demographics with geometry centroids.
"""
import json, os, sys, time, urllib.request, urllib.parse

BASE = "https://services1.arcgis.com/vHnIGBHHqDR6y0CR/arcgis/rest/services/2021_ABS_General_Community_Profile/FeatureServer"
OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "abs_census")
os.makedirs(OUT_DIR, exist_ok=True)

LAYERS = {
    "sa1": 6,  # 61,811 features
    "sa2": 4,  # ~2,454 features
    "lga": 3,  # ~547 features
}

BATCH = 2000

def fetch_layer(name, layer_id):
    print(f"\n{'='*50}", flush=True)
    print(f"Downloading {name.upper()} (layer {layer_id})", flush=True)
    
    # Get count
    count_url = f"{BASE}/{layer_id}/query?where=1%3D1&returnCountOnly=true&f=json"
    with urllib.request.urlopen(count_url, timeout=30) as r:
        total = json.loads(r.read())["count"]
    print(f"Total features: {total:,}", flush=True)
    
    all_features = []
    offset = 0
    
    while offset < total:
        params = urllib.parse.urlencode({
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "returnCentroid": "true",
            "outSR": "4326",
            "resultOffset": offset,
            "resultRecordCount": BATCH,
            "f": "json",
        })
        url = f"{BASE}/{layer_id}/query?{params}"
        
        retries = 0
        while retries < 3:
            try:
                with urllib.request.urlopen(url, timeout=120) as r:
                    data = json.loads(r.read())
                break
            except Exception as e:
                retries += 1
                print(f"  Retry {retries}: {e}", flush=True)
                time.sleep(2 ** retries)
        else:
            print(f"  FAILED at offset {offset}", flush=True)
            offset += BATCH
            continue
        
        features = data.get("features", [])
        if not features:
            break
        
        for f in features:
            attrs = f.get("attributes", {})
            # Get centroid from geometry
            geom = f.get("geometry")
            if geom:
                if "rings" in geom and geom["rings"]:
                    ring = geom["rings"][0]
                    attrs["_lat"] = round(sum(p[1] for p in ring) / len(ring), 6)
                    attrs["_lon"] = round(sum(p[0] for p in ring) / len(ring), 6)
                elif "x" in geom and "y" in geom:
                    attrs["_lat"] = round(geom["y"], 6)
                    attrs["_lon"] = round(geom["x"], 6)
            all_features.append(attrs)
        
        offset += len(features)
        pct = min(100, offset * 100 // total)
        print(f"  {offset:>6,} / {total:,} ({pct}%)", flush=True)
        
        time.sleep(0.5)
    
    # Save
    out_path = os.path.join(OUT_DIR, f"census_2021_{name}.json")
    with open(out_path, "w") as fp:
        json.dump(all_features, fp)
    size_mb = os.path.getsize(out_path) / 1024 / 1024
    print(f"Saved {len(all_features):,} records ({size_mb:.1f}MB) to {out_path}", flush=True)
    return len(all_features)


if __name__ == "__main__":
    start = time.time()
    totals = {}
    for name, lid in LAYERS.items():
        totals[name] = fetch_layer(name, lid)
    
    elapsed = time.time() - start
    print(f"\n{'='*50}", flush=True)
    print(f"ALL DONE in {elapsed:.0f}s", flush=True)
    for name, count in totals.items():
        print(f"  {name}: {count:,} records", flush=True)
