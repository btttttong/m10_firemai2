
# 🔥 Firemai: End-to-End Data Pipeline on GCP

## 🎯 Objective

Build a complete data pipeline that ingests fire hotspot data from GISTDA, stores it in a data lake (GCS), loads it into BigQuery for transformation, enriches it with nearby POIs, and visualizes the insights via Looker Studio dashboard.

---

## 📡 Architecture Overview

```
Cloud Scheduler
      |
      v
gistda-to-gcs (Cloud Run) ----------------------+
                                                |
                     Push to GCS (firemai_data/*.json)
                                                |
                     Pub/Sub trigger (firemai-gcs-bq-trigger)
                                                |
      +-----------------------------------------+
      |
      v
gcs-to-bigquery (Cloud Run)
      |
      v
BigQuery: fire_hotspot_temp  --[Scheduled Query]--> fire_hotspot_main
      |
      +--> poi_from_centroid (Cloud Run)
               |
               v
        BigQuery: fire_poi_grouped

fire_hotspot_main + fire_poi_grouped
      |
      v
View: hotspot_with_nearest_tag
      |
      v
Looker Studio Dashboard
```

---

## ⚙️ Components

### 1. `gistda-to-gcs`:
- Scheduled via **Cloud Scheduler** (every 8 hours).
- Calls GISTDA Fire API to fetch fire hotspot data.
- Extracts only the `properties` field and saves to newline-delimited JSON.
- Uploads JSON to `gs://firemai/firemai_data/`.

### 2. `gcs-to-bigquery`:
- Triggered by **Pub/Sub** event from GCS object finalize.
- Loads the JSON from GCS into `fire_hotspot_temp` table.
- Deletes the file from GCS after ingestion to save space.

### 3. `fire_hotspot_main`:
- Created via **Scheduled Query** from `fire_hotspot_temp` with deduplication and clean schema.

### 4. `poi_from_centroid`:  
- Enriches fire centroids with nearby POIs (e.g., villages, roads, waterways) from GISTDA POI API.  
- Writes enriched data to `fire_poi_grouped`, which is used to identify locations closest to fire hotspots.

### 5. `hotspot_with_nearest_tag` View:
- Combines `fire_hotspot_main` and `fire_poi_grouped` to find the nearest POI tag to each hotspot.

---

## 📊 Dashboard

Built using **Looker Studio**  
🔗 [View Dashboard](https://lookerstudio.google.com/reporting/69cca1e4-e109-4e93-8c74-3455aeaf2a49/page/OKGF)

---

## 🚀 Deployment Instructions

1. Clone the repo and set up the following folders:
    ```
    /gistda_to_gcs
    /gcs_to_bigquery
    /poi_from_centroid
    ```

2. Deploy each folder to **Cloud Run** using GitHub integration:
    - Each folder must include:
        - `main.py` (with Functions Framework)
        - `Dockerfile`
        - `requirements.txt`

3. Set environment variables:
    - `.env` (or set via Cloud Run UI):
      ```
      PROJECT_ID=hs-new-project-456106
      BUCKET_NAME=firemai
      SUBFOLDER=firemai_data
      BQ_DATASET=Firemai
      TEMP_TABLE=fire_hotspot_temp
      GISTDA_API_KEY=...
      POI_API_KEY=...
      ```

4. Enable Cloud Scheduler to POST to `gistda-to-gcs`.

5. Ensure `firemai-gcs-bq-trigger` Pub/Sub is connected to `gcs-to-bigquery` Cloud Run via **Push Subscription** with:
    - Enable Payload Unwrapping
    - Allow Unauthenticated Invocations

6. Confirm that scheduled query runs and creates `fire_hotspot_main`.

7. Deploy enrichment job (`poi_from_centroid`) to run on-demand or via Scheduler.

---

## 🛡️ Roles and Security

- Cloud Run services use service accounts with minimal IAM roles:
    - BigQuery Data Editor
    - Storage Object Admin
    - Pub/Sub Publisher
- Authentication is disabled for Pub/Sub push triggers (but enabled for Scheduler).

---

## ✅ Summary

- ✅ Real GISTDA API used
- ✅ Batch ingestion using GCS + Pub/Sub
- ✅ Modular Cloud Run services
- ✅ POI enrichment logic
- ✅ Final Looker dashboard
- ✅ Re-runnable and secure
