import base64
import json
import os
from flask import Flask, request
from dotenv import load_dotenv
from google.cloud import bigquery, storage

# === Load .env for local dev ===
load_dotenv()

# === Flask App ===
app = Flask(__name__)

# === ENV CONFIG ===
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
TEMP_TABLE = os.getenv("TEMP_TABLE")

def load_json_from_gcs(bucket_name, file_path):
    # Load into BigQuery
    bq_client = bigquery.Client(project=PROJECT_ID)
    uri = f"gs://{bucket_name}/{file_path}"
    print(f"📥 Loading from {uri}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = bq_client.load_table_from_uri(
        uri, f"{PROJECT_ID}.{DATASET}.{TEMP_TABLE}", job_config=job_config
    )
    load_job.result()
    print(f"✅ Loaded into BigQuery: {TEMP_TABLE}")

    # Delete file from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    blob.delete()
    print(f"🗑️ Deleted GCS file: gs://{bucket_name}/{file_path}")

@app.route("/", methods=["POST"])
def pubsub_handler():
    envelope = request.get_json()
    if not envelope or 'message' not in envelope:
        return "❌ Invalid Pub/Sub format", 400

    try:
        message = json.loads(base64.b64decode(envelope['message']['data']).decode("utf-8"))
        print("📦 Pub/Sub Message:", message)
        load_json_from_gcs(message["bucket"], message["file_path"])
        return "✅ Done", 200
    except Exception as e:
        print("❌ ERROR:", str(e))
        return f"Error: {e}", 500