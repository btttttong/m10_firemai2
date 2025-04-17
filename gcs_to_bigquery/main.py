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
    try:
        payload = request.get_json()
        print("📦 Incoming payload:", json.dumps(payload, indent=2))

        # Case: unwrapped
        if "bucket" in payload and "name" in payload:
            bucket = payload["bucket"]
            name = payload["name"]

        # Case: wrapped
        elif "message" in payload and "data" in payload["message"]:
            decoded = base64.b64decode(payload["message"]["data"]).decode("utf-8")
            message = json.loads(decoded)
            bucket = message.get("bucket")
            name = message.get("name")
        else:
            print("⚠️ Unrecognized message format")
            return "OK", 200

        if not bucket or not name:
            print("⚠️ Missing bucket or file name")
            return "OK", 200

        # ดำเนินการโหลดเข้า BigQuery + ลบไฟล์
        load_json_from_gcs(bucket, name)
        delete_file_from_gcs(bucket, name)

    except Exception as e:
        print(f"❌ Error (but no retry): {e}")

    return "OK", 200  # ✅ ไม่ว่าเกิดอะไร ก็จบแบบไม่ retry