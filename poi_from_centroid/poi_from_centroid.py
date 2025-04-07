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
API_KEY = os.getenv("POI_API_KEY")
LIMIT = int(os.getenv("LIMIT", 100))

bq_client = bigquery.Client(project=PROJECT_ID)

def get_centroids():
    query = f"""
        SELECT pv_en, amphoe, tambol, center_lat, center_lon
        FROM `{VIEW_NAME}`
        WHERE center_lat IS NOT NULL AND center_lon IS NOT NULL
    """
    rows = [dict(row) for row in bq_client.query(query).result()]
    print(f"📦 Found {len(rows)} centroids from view.")
    return rows

def get_pois(lat, lon):
    try:
        url = "https://api.sphere.gistda.or.th/services/poi/search"
        params = {"lat": lat, "lon": lon, "limit": LIMIT, "key": API_KEY}
        print(f"🌐 Calling POI API with: {params}")
        r = requests.get(url, params=params, timeout=10)

        print(f"🔁 Response status: {r.status_code}")
        if r.status_code != 200 or not r.text.strip():
            print("⚠️ Empty or invalid response")
            return []

        json_data = r.json()
        pois = json_data.get("data", [])
        print(f"✅ Found {len(pois)} POIs")
        return pois
    except Exception as e:
        print(f"❌ POI API error: {e}")
        return []

def enrich_and_insert():
    centroids = get_centroids()
    records = []

    for c in centroids:
        print(f"📍 Checking centroid at {c['center_lat']}, {c['center_lon']}")
        pois = get_pois(c["center_lat"], c["center_lon"])
        for poi in pois:
            raw_tags = poi.get("tag", [])
            tags = [t for t in raw_tags if all(ord(c) < 128 for c in t)]
            if not isinstance(tags, list):
                tags = []
            records.append({
                "pv_en": c.get("pv_en"),
                "amphoe": c.get("amphoe"),
                "tambol": c.get("tambol"),
                "poi_id": poi.get("id"),
                "poi_name": poi.get("name"),
                "poi_type": poi.get("type"),
                "poi_lat": poi.get("lat"),
                "poi_lon": poi.get("lon"),
                "verified": poi.get("verified"),
                "contributor": poi.get("contributor"),
                "tags": tags,
                "address": poi.get("address", "") or "",
                "tel": poi.get("tel", "") or ""
            })
        time.sleep(0.5)

    print(f"📊 Total POI records: {len(records)}")
    if records:
        print(f"🚀 Inserting {len(records)} POIs to BigQuery...")
        errors = bq_client.insert_rows_json(TARGET_TABLE, records)
        if errors:
            print("❌ BQ insert errors:")
            for err in errors:
                print(err)
        else:
            print("✅ Insert completed.")
    else:
        print("⚠️ No POI records to insert.")

# ✅ Cloud Run entrypoint
def main(request):
    print("🚀 Cloud Run HTTP request received")
    enrich_and_insert()
    return "✅ POI Enrichment complete", 200

# ✅ Local test
if __name__ == "__main__":
    class FakeRequest:
        def __init__(self):
            self.method = "GET"
    response = main(FakeRequest())
    print(response)