import os
import time
import json
import requests
from datetime import datetime
from google.cloud import storage
import traceback

API_URL = "https://disaster.gistda.or.th/api/1.0/documents/fire/hotspot/modis/3days"
API_KEY = os.getenv("API_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
SUBFOLDER = os.getenv("SUBFOLDER", "")

def fetch_all_properties():
    headers = {"API-Key": API_KEY}
    all_properties, offset = [], 0

    while True:
        params = {"limit": 1000, "offset": offset, "ct_tn": "ราชอาณาจักรไทย"}
        try:
            r = requests.get(API_URL, headers=headers, params=params)
            r.raise_for_status()
            features = r.json().get("features", [])
        except Exception as e:
            print(f"❌ Error fetching data (offset={offset}): {e}")
            break

        if not features:
            print("✅ Fetched all data.")
            break

        batch = [f["properties"] for f in features]
        all_properties.extend(batch)
        print(f"📦 Retrieved {len(batch)} records (offset={offset})")
        offset += 1000
        time.sleep(0.5)

    print(f"🎯 Total records fetched: {len(all_properties)}")
    return all_properties

def upload_to_gcs(filename, gcs_path):
    try:
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(filename)
        print(f"✅ Uploaded to GCS: gs://{BUCKET_NAME}/{gcs_path}")
    except Exception as e:
        print(f"❌ Failed to upload to GCS: {e}")


def main(request):
    print("🚀 Cloud Scheduler triggered")

    if not API_KEY or not BUCKET_NAME:
        print("⚠️ Missing required environment variables.")
        return "Missing config", 500
    
    try:
        message = request.get_json()
        print("📦 Message received:", message)

        data = fetch_all_properties()
        if not data:
            print("⚠️ No data to save.")
            return "No data", 204

        timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        local_filename = f"hotspot_{timestamp}.json"
        with open(local_filename, "w", encoding="utf-8") as f:
            for record in data:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        gcs_filename = f"{SUBFOLDER}/hotspot_{timestamp}.json" if SUBFOLDER else f"hotspot_{timestamp}.json"
        upload_to_gcs(local_filename, gcs_filename)

        return {
            "status": "success",
            "timestamp": timestamp,
            "record_count": len(data),
            "gcs_path": f"gs://{BUCKET_NAME}/{gcs_filename}"
        }, 200

    except Exception as e:
        print(f"❌ Error processing the request: {e}")
        traceback.print_exc()
        return "Error", 500

# class FakeRequest:
#     def get_json(self):
#         return {
#             "bucket": "firemai",
#             "file_path": "firemai_data"
#         }

# if __name__ == "__main__":
#     main(FakeRequest())