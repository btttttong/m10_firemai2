import os, time, json, requests
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://disaster.gistda.or.th/api/1.0/documents/fire/hotspot/modis/7days"
API_KEY = os.getenv("GISTDA_API_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
SUBFOLDER = os.getenv("SUBFOLDER")

def fetch_all_properties():
    headers = {"API-Key": API_KEY}
    all_properties, offset = [], 0
    while True:
        params = {"limit": 1000, "offset": offset, "ct_tn": "ราชอาณาจักรไทย"}
        r = requests.get(API_URL, headers=headers, params=params)
        r.raise_for_status()
        features = r.json().get("features", [])
        if not features:
            break
        all_properties.extend([f["properties"] for f in features])
        offset += 1000
        time.sleep(0.5)
    return all_properties

def upload_to_gcs(filename, gcs_path):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(filename, content_type="application/json")
    print(f"✅ Uploaded to GCS: gs://{BUCKET_NAME}/{gcs_path}")

def main(request):
    timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    output_file = f"hotspot_properties_{timestamp}.json"
    gcs_path = f"{SUBFOLDER}/{output_file}"
    data = fetch_all_properties()

    with open(output_file, "w", encoding="utf-8") as out:
        for row in data:
            json.dump(row, out, ensure_ascii=False)
            out.write("\n")
    upload_to_gcs(output_file, gcs_path)
    return f"✅ Uploaded {len(data)} records to GCS", 200

if __name__ == "__main__":
    class FakeRequest: pass
    print(main(FakeRequest()))