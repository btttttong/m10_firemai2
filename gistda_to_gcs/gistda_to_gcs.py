import requests, json, time
from datetime import datetime
from google.cloud import storage
import os
from dotenv import load_dotenv

# === LOAD ENV ===
load_dotenv()

API_URL = "https://disaster.gistda.or.th/api/1.0/documents/fire/hotspot/modis/7days"
API_KEY = os.getenv("GISTDA_API_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
SUBFOLDER = os.getenv("SUBFOLDER")

HEADERS = {"API-Key": API_KEY}

timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
output_file = f"hotspot_properties_{timestamp}.json"
gcs_path = f"{SUBFOLDER}/{output_file}"

# === FETCH DATA ===
def fetch_all_properties():
    all_properties = []
    offset = 0
    while True:
        params = {"limit": 1000, "offset": offset, "ct_tn": "ราชอาณาจักรไทย"}
        r = requests.get(API_URL, headers=HEADERS, params=params, timeout=15)
        print(f"📥 Fetching offset {offset}...")
        r.raise_for_status()
        features = r.json().get("features", [])
        if not features:
            print("✅ No more data.")
            break
        all_properties.extend([f["properties"] for f in features])
        offset += 1000
        time.sleep(0.5)
    return all_properties

# === UPLOAD TO GCS ===
def upload_to_gcs(local_path, gcs_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path, content_type="application/json")
    print(f"✅ Uploaded to GCS: gs://{BUCKET_NAME}/{gcs_path}")

# === MAIN ===
if __name__ == "__main__":
    properties_list = fetch_all_properties()

    with open(output_file, "w", encoding="utf-8") as out:
        for row in properties_list:
            json.dump(row, out, ensure_ascii=False)
            out.write("\n")

    upload_to_gcs(output_file, gcs_path)