#!/bin/bash
# scripts/maintenance/backup_checkpoints.sh - daily backup helper for Airflow or manual runs.

set -e

GCS_BACKUP_BUCKET="${GCS_BUCKET_BACKUPS:-big-data-group-4-backups}"
DATE=$(date +%Y%m%d)
RETENTION_DAYS=7

echo "Starting backup at $(date)"

# 1. Backup Postgres
echo "Backing up Postgres..."
docker compose exec -T postgres pg_dump -U ${POSTGRES_USER} ${POSTGRES_DB} | gzip > /tmp/backup_${DATE}.sql.gz
gsutil cp /tmp/backup_${DATE}.sql.gz gs://${GCS_BACKUP_BUCKET}/postgres/backup_${DATE}.sql.gz
rm /tmp/backup_${DATE}.sql.gz
echo "Postgres backup uploaded"

# 2. Sync Spark/Flink checkpoint metadata only.
echo "Syncing checkpoint metadata..."
if [ -d "./data/checkpoints" ]; then
  gsutil -m rsync -r -d ./data/checkpoints/metadata/ gs://${GCS_BACKUP_BUCKET}/checkpoints/metadata/
  echo "Checkpoint metadata synced"
fi

# 3. Backup Airflow DAGs + configs
echo "Backing up Airflow configs..."
gsutil -m rsync -r -d ./orchestration/dags/ gs://${GCS_BACKUP_BUCKET}/airflow/dags/
gsutil cp .env gs://${GCS_BACKUP_BUCKET}/configs/.env_${DATE} 2>/dev/null || true
echo "Airflow configs backed up"

# 4. Cleanup old backups (>7 days)
echo "Cleaning up backups older than ${RETENTION_DAYS} days..."
gsutil ls gs://${GCS_BACKUP_BUCKET}/** | grep -E "[0-9]{8}" | while read path; do
  file_date=$(echo $path | grep -oE "[0-9]{8}")
  if [[ -n "$file_date" ]]; then
    file_timestamp=$(date -d "${file_date:0:4}-${file_date:4:2}-${file_date:6:2}" +%s 2>/dev/null || echo 0)
    now_timestamp=$(date +%s)
    age_days=$(( (now_timestamp - file_timestamp) / 86400 ))
    if [[ $age_days -gt $RETENTION_DAYS ]]; then
      gsutil rm "$path"
      echo "Deleted: $path (${age_days} days old)"
    fi
  fi
done

echo "Backup completed at $(date)"
