import os
import time
import json
import requests
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

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


# Mocking the event and context parameters (as they would come from Cloud Pub/Sub)
class FakeRequest:
    def get_json(self):
        return {
            "bucket": "firemai",
            "file_path": "firemai_data/hotspot_properties_20250407161400.json"
        }

def main(event, context):
    print("🚀 Pub/Sub triggered")
    try:
        # Decode the incoming message
        message = json.loads(base64.b64decode(event['data']).decode("utf-8"))
        print("📦 Message received:", message)

        # Perform tasks (fetch and upload)
        data = fetch_all_properties()
        if not data:
            print("⚠️ No data to save.")
            return

        timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        local_filename = f"hotspot_{timestamp}.json"
        with open(local_filename, "w", encoding="utf-8") as f:
            for record in data:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

        gcs_filename = f"{SUBFOLDER}/hotspot_{timestamp}.json" if SUBFOLDER else f"hotspot_{timestamp}.json"
        upload_to_gcs(local_filename, gcs_filename)

    except Exception as e:
        print(f"❌ Error processing the request: {e}")
        return "Error", 500  # Handle errors appropriately


if __name__ == "__main__":
    main(FakeRequest(), None)  # Run locally with mock data