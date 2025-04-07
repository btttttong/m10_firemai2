import requests
from google.cloud import bigquery
import os
import time
from dotenv import load_dotenv

# === Load .env for local dev ===
load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
VIEW_NAME = f"{PROJECT_ID}.Firemai.v_hotspot_centroid_by_area"
TARGET_TABLE = f"{PROJECT_ID}.Firemai.fire_poi_grouped"
API_KEY = os.getenv("POI_API_KEY", "")
LIMIT = int(os.getenv("LIMIT", 5))

bq_client = bigquery.Client(project=PROJECT_ID)

def get_centroids():
    query = f"""
        SELECT pv_en, amphoe, tambon, center_lat, center_lon
        FROM `{VIEW_NAME}`
        WHERE center_lat IS NOT NULL AND center_lon IS NOT NULL
    """
    return [dict(row) for row in bq_client.query(query).result()]

def get_pois(lat, lon):
    try:
        url = "https://api.sphere.gistda.or.th/services/poi/search"
        params = {"lat": lat, "lon": lon, "limit": LIMIT, "key": API_KEY}
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json().get("data", [])
    except Exception as e:
        print(f"❌ POI API error: {e}")
        return []

def enrich_and_insert():
    centroids = get_centroids()
    records = []
    print(f"📦 Processing {len(centroids)} centroids...")
    for c in centroids:
        pois = get_pois(c["center_lat"], c["center_lon"])
        for poi in pois:
            records.append({
                "pv_en": c["pv_en"],
                "amphoe": c["amphoe"],
                "tambon": c["tambon"],
                "poi_id": poi.get("id"),
                "poi_name": poi.get("name"),
                "poi_type": poi.get("type"),
                "poi_lat": poi.get("lat"),
                "poi_lon": poi.get("lon"),
                "verified": poi.get("verified"),
                "contributor": poi.get("contributor")
            })
        time.sleep(0.5)

    if records:
        print(f"🚀 Inserting {len(records)} POIs...")
        errors = bq_client.insert_rows_json(TARGET_TABLE, records)
        if errors:
            print("❌ BQ insert errors:", errors)
        else:
            print("✅ Done.")
    else:
        print("⚠️ No POI records found.")

# ✅ Cloud Run HTTP entrypoint
def main(request):
    enrich_and_insert()
    return "✅ POI Enrichment complete", 200