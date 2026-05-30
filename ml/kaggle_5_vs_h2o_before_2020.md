# Kaggle 5 vs H2O Before 2020

This note compares the Kaggle 5 classical baseline family against the local
H2O before-2020 reference on the same processed feature contract.

## Run inputs

- Kaggle 5 comparison dataset: `data/process/us_train_offline_before_2020.csv`
- H2O reference log: `data/simulation/h2o_before_2020_classsampling.log`
- Kaggle 5 runner: `ml/training/kaggle_5_before_2020.py`

## Feature engineering differences

### Kaggle 5 notebook

- Starts from raw US Accidents columns.
- Derives `Hour`, `Weekday`, and `Time_Duration(min)`.
- Keeps many categorical fields such as `City`, `County`, `State`,
  `Timezone`, `Wind_Direction`, and `Weather_Condition`.
- Uses `pd.get_dummies()` heavily.
- Originally restricts to Pennsylvania / Montgomery County to stay tractable.

### H2O production contract

- Uses the shared `processing/feature_engineering.py` path.
- Collapses raw columns into 20 numeric, online-safe features.
- Encodes weather text into `weather_code`.
- Encodes street text into `road_type_code`.
- Uses boolean road-context flags such as `is_junction`,
  `has_traffic_signal`, `is_crossing`, `is_roundabout`, `is_stop`,
  `is_station`, `is_railway`.
- Avoids high-cardinality location columns and end-time duration so the same
  schema works for Flink, Spark, and offline training.

### Fairness choice in this run

The comparison run keeps the Kaggle 5 model family but feeds it the H2O
processed feature file. That makes the metric comparison fairer for the
capstone pipeline, even though it is not a raw-notebook rerun of every Kaggle
feature step.

## Local resource policy used

- CPU jobs capped at `2`
- CPU thread env cap at `2`
- Soft GPU budget: `4.0 GiB`
- GPU detected: `NVIDIA GeForce RTX 5060 Laptop GPU`
- GPU not used in training because this baseline stays on sklearn classical
  models
- Train rows used: `400,000 / 2,380,669`
- Test rows used: `100,000 / 595,168`

## Kaggle 5 results from the resource-aware run

| Model | Accuracy | Macro F1 | Weighted F1 | Logloss | Mean per-class error |
| --- | ---: | ---: | ---: | ---: | ---: |
| Random Forest | 0.762280 | 0.397744 | 0.748997 | 0.613518 | 0.616684 |
| Decision Tree (entropy) | 0.721050 | 0.351360 | 0.705847 | 0.618614 | 0.653085 |
| Decision Tree (gini) | 0.724220 | 0.345258 | 0.712312 | 0.622129 | 0.649684 |

Skipped in the local weak-machine policy:

- Logistic Regression: too slow above `250,000` train rows
- KNN: too expensive above `20,000 / 5,000` train/test rows
- Random Forest (selected features): skipped because selector would retrain an
  extra forest above `250,000` rows

## H2O reference

| Model | Accuracy | Macro F1 | Weighted F1 | Logloss | Mean per-class error |
| --- | ---: | ---: | ---: | ---: | ---: |
| H2O XGBoost | 0.795512 | 0.418518 | 0.785149 | 0.557677 | 0.591127 |

## Best-vs-best takeaway

- Best Kaggle 5 local run: `Random Forest`
- Best H2O reference: `XGBoost_1_AutoML_1_20260511_202816`
- H2O still leads on all core shared metrics in this comparison:
  - Accuracy: `0.795512` vs `0.762280`
  - Macro F1: `0.418518` vs `0.397744`
  - Weighted F1: `0.785149` vs `0.748997`
  - Logloss: `0.557677` vs `0.613518`
  - Mean per-class error: `0.591127` vs `0.616684`

## Rare-class note

The Kaggle 5 Random Forest still fails severity `1` and remains weak on
severity `4`, similar to the H2O story:

| Class | Kaggle RF precision | Kaggle RF recall | H2O precision | H2O recall |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| 2 | 0.804454 | 0.875744 | 0.842256 | 0.877560 |
| 3 | 0.647819 | 0.578838 | 0.687546 | 0.687949 |
| 4 | 0.677778 | 0.078684 | 0.672897 | 0.069982 |

## Artifacts

- `data/simulation/kaggle_5_before_2020/summary_metrics.csv`
- `data/simulation/kaggle_5_before_2020/best_model_comparison.csv`
- `data/simulation/kaggle_5_before_2020/random_forest_classification_report.csv`
- `data/simulation/kaggle_5_before_2020/random_forest_confusion_matrix.csv`
- `data/simulation/kaggle_5_before_2020/h2o_best_classification_report.csv`
- `data/simulation/kaggle_5_before_2020/resource_metadata.json`
- `data/simulation/kaggle_5_before_2020/feature_engineering_comparison.md`
