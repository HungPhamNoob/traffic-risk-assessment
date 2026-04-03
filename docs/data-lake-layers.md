# Data Lake Layers

```text
gs://road-accident-data-{project_id}/
├── bronze/raw/
│   ├── us_accidents/
│   ├── uk_accidents/
│   └── tomtom_incidents/date=YYYY-MM-DD/
├── silver/enriched/
├── gold/
├── checkpoints/
├── mlflow-artifacts/
└── simulation_results/
```

## Layer rules

- Bronze: du lieu goc, immutable.
- Silver: cleaned + validated + standardized schema.
- Gold: feature tables, risk score, hotspot outputs.

## Partition goi y

- Theo ngay: `date=YYYY-MM-DD`.
- Co the bo sung `source=us|uk|tomtom` cho truy van nhanh.

## Spot VM recovery

- Flink/Spark state luu o `checkpoints/` de khoi phuc sau preemption.

