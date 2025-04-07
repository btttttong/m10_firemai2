import os, json, base64
from google.cloud import bigquery, storage
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
TEMP_TABLE = os.getenv("TEMP_TABLE")

def load_json_from_gcs(bucket_name, file_path):
    client = bigquery.Client(project=PROJECT_ID)
    uri = f"gs://{bucket_name}/{file_path}"
    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    client.load_table_from_uri(uri, f"{PROJECT_ID}.{DATASET}.{TEMP_TABLE}", job_config=config).result()
    print(f"✅ Loaded: {file_path} to {TEMP_TABLE}")

def delete_file_from_gcs(bucket_name, file_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    blob.delete()
    print(f"🗑️ Deleted file from GCS: gs://{bucket_name}/{file_path}")

def main(request):
    envelope = request.get_json()
    if not envelope or 'message' not in envelope:
        return "❌ Invalid Pub/Sub format", 400
    try:
        message = json.loads(base64.b64decode(envelope['message']['data']).decode("utf-8"))
        bucket = message["bucket"]
        file_path = message["file_path"]

        load_json_from_gcs(bucket, file_path)
        delete_file_from_gcs(bucket, file_path)

        return "✅ GCS → BQ complete + file deleted", 200
    except Exception as e:
        print(f"❌ ERROR: {e}")
        return f"Error: {e}", 500
    

if __name__ == "__main__":
    fake_data = {
        "bucket": "firemai",
        "file_path": "firemai_data/hotspot_properties_20250407.json"
    }
    fake_event = {
        "message": {
            "data": base64.b64encode(json.dumps(fake_data).encode("utf-8")).decode("utf-8")
        }
    }
    class FakeRequest:
        def get_json(self): return fake_event
    print(main(FakeRequest()))