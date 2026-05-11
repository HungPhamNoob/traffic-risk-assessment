#!/bin/bash
# ============================================================
# upload_data_to_gcs.sh
# Upload offline training & pipeline replay data to GCS
# ============================================================
# Run once to seed the data lake.
# Usage: bash scripts/upload_data_to_gcs.sh
# ============================================================

set -e

# ============================================================
# Configuration - match your .env.cloud
# ============================================================
GCS_BUCKET="${GCS_BUCKET:-big-data-group-4-bronze}"
LOCAL_DATA_DIR="${LOCAL_DATA_DIR:-data/split}"

# ============================================================
# Files to upload
# ============================================================
TRAIN_FILE="${LOCAL_DATA_DIR}/us_train_offline_before_2020.csv"
PIPELINE_FILE="${LOCAL_DATA_DIR}/us_pipeline_from_2020.csv"

# ============================================================
# Upload
# ============================================================
echo "============================================================"
echo "Uploading offline data to GCS"
echo "============================================================"
echo "Bucket:  gs://${GCS_BUCKET}"
echo "Source:  ${LOCAL_DATA_DIR}/"
echo ""

# Upload training file
if [ -f "$TRAIN_FILE" ]; then
    echo "Uploading $TRAIN_FILE..."
    gcloud storage cp "$TRAIN_FILE" "gs://${GCS_BUCKET}/process/us_train_offline_before_2020.csv"
    echo "SUCCESS: $TRAIN_FILE uploaded."
else
    echo "WARNING: $TRAIN_FILE not found. Skipping."
fi

# Upload pipeline replay file
if [ -f "$PIPELINE_FILE" ]; then
    echo "Uploading $PIPELINE_FILE..."
    gcloud storage cp "$PIPELINE_FILE" "gs://${GCS_BUCKET}/process/us_pipeline_from_2020.csv"
    echo "SUCCESS: $PIPELINE_FILE uploaded."
else
    echo "WARNING: $PIPELINE_FILE not found. Skipping."
fi

echo ""
echo "============================================================"
echo "Upload complete."
echo "============================================================"