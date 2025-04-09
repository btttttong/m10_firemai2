import base64
import json
import os
from google.cloud import bigquery, storage
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = os.getenv("BQ_DATASET")
TEMP_TABLE = os.getenv("TEMP_TABLE")

def load_json_from_gcs(bucket_name, file_path):
    try:
        client = bigquery.Client(project=PROJECT_ID)
        uri = f"gs://{bucket_name}/{file_path}"
        config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        client.load_table_from_uri(uri, f"{PROJECT_ID}.{DATASET}.{TEMP_TABLE}", job_config=config).result()
        print(f"✅ Loaded: {file_path} to {TEMP_TABLE}")
    except Exception as e:
        print(f"❌ Error loading to BigQuery: {e}")

def delete_file_from_gcs(bucket_name, file_path):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        blob.delete()
        print(f"🗑️ Deleted file from GCS: gs://{bucket_name}/{file_path}")
    except Exception as e:
        print(f"❌ Error deleting file from GCS: {e}")

def main(request):
    envelope = request.get_json()
    if not envelope or 'message' not in envelope:
        print("❌ Invalid Pub/Sub message")
        return ("OK", 200)

    pubsub_message = envelope['message']
    try:
        data = base64.b64decode(pubsub_message['data']).decode('utf-8')
        print(f"🟢 Received message: {data}")

        attributes = pubsub_message.get("attributes", {})
        bucket_name = attributes.get("bucketId")
        file_path = attributes.get("objectId")

        if not bucket_name or not file_path:
            print("⚠️ Missing bucket or object path")
            return ("OK", 200)

        load_json_from_gcs(bucket_name, file_path)
        delete_file_from_gcs(bucket_name, file_path)

    except Exception as e:
        print(f"❌ Unexpected error: {e}")

    return ("OK", 200)