import os, time, requests
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
VIEW_NAME = f"{PROJECT_ID}.Firemai.v_hotspot_centroid_by_area"
TARGET_TABLE = f"{PROJECT_ID}.Firemai.fire_poi_grouped"
API_KEY = os.getenv("POI_API_KEY")
LIMIT = int(os.getenv("LIMIT", 20))
bq = bigquery.Client(project=PROJECT_ID)

def get_centroids():
    query = f"""SELECT pv_en, amphoe, tambol, center_lat, center_lon FROM `{VIEW_NAME}` 
                WHERE center_lat IS NOT NULL AND center_lon IS NOT NULL"""
    return [dict(row) for row in bq.query(query).result()]

def get_pois(lat, lon):
    url = "https://api.sphere.gistda.or.th/services/poi/search"
    params = {"lat": lat, "lon": lon, "limit": LIMIT, "key": API_KEY}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json().get("data", [])
    except:
        return []

def enrich_and_insert():
    records = []
    for c in get_centroids():
        pois = get_pois(c["center_lat"], c["center_lon"])
        for p in pois:
            tags = [t for t in p.get("tag", []) if all(ord(c) < 128 for c in t)]
            records.append({
                "pv_en": c["pv_en"],
                "amphoe": c["amphoe"],
                "tambol": c["tambol"],
                "poi_id": p.get("id"),
                "poi_name": p.get("name"),
                "poi_type": p.get("type"),
                "poi_lat": p.get("lat"),
                "poi_lon": p.get("lon"),
                "verified": p.get("verified"),
                "contributor": p.get("contributor"),
                "tags": tags,
                "address": p.get("address", ""),
                "tel": p.get("tel", "")
            })
        time.sleep(0.5)
    if records:
        bq.insert_rows_json(TARGET_TABLE, records)
        print(f"✅ Inserted {len(records)} POIs")

def main(request):
    enrich_and_insert()
    return "✅ POI Enrichment done", 200

if __name__ == "__main__":
    class FakeRequest: pass
    print(main(FakeRequest()))