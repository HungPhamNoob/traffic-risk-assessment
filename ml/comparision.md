# H2O Model Comparison Against Paper Baselines

This document compares the current Traffic Risk Assessment model against the
baseline papers stored in `paper/`. The comparison focuses on model quality,
data handling, leakage control, and production readiness because the capstone
project is an end-to-end Big Data and MLOps system, not only an offline
classification notebook.

## Current Project Result

The current full-data local run trained on the before-2020 processed feature
file:

```text
data/process/us_train_offline_before_2020.csv
```

The training script was:

```text
ml/training/h2o_before_2020.py
```

The latest run used H2O AutoML with class balancing and explicit bounded class
sampling factors enabled. The sampling factors were computed from the
before-2020 label distribution in severity order `[1, 2, 3, 4]`:

```text
[100.0, 1.0, 2.246568, 21.599554]
```

The model selector uses macro F1 first, with logloss as a tie-breaker. MLflow
stored model artifacts, leaderboard information, complete sklearn metrics,
per-class precision, recall, F1, support, and confusion matrices.

| Item | Value |
| --- | ---: |
| Training rows | 2,380,970 |
| Test rows | 594,867 |
| Feature columns | 20 |
| Label column | `true_severity` |
| Best model | H2O XGBoost |
| Accuracy | 0.795512 |
| Macro precision | 0.550675 |
| Macro recall | 0.408873 |
| Macro F1 | 0.418518 |
| Weighted precision | 0.790505 |
| Weighted recall | 0.795512 |
| Weighted F1 | 0.785149 |
| Logloss | 0.557677 |
| Mean per-class error | 0.591127 |

Per-class result for the best H2O XGBoost model:

| Severity | Precision | Recall | F1 | Support |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 0.000000 | 0.000000 | 0.000000 | 219 |
| 2 | 0.842256 | 0.877560 | 0.859545 | 398,610 |
| 3 | 0.687546 | 0.687949 | 0.687747 | 177,519 |
| 4 | 0.672897 | 0.069982 | 0.126779 | 18,519 |

Interpretation:

The model is useful for majority traffic risk classes and gives a realistic
production baseline, but it is not yet strong for rare classes. Severity 1 has
only 969 rows in the full before-2020 training file and 219 rows in the test
split, so the current feature set does not provide enough signal for that class.
The new class sampling run improved macro F1, weighted F1, logloss, severity 3
recall, and severity 4 recall compared with the previous local H2O run, but
severity 1 remains unsolved. Macro F1, weighted F1, class-level recall, and
confusion matrices are mandatory report metrics.

## Baseline Paper Summary

| Baseline | Data / Scope | Method | Reported Metrics | Notes |
| --- | --- | --- | --- | --- |
| Baseline 1 | U.S. accident severity data with an 80/20 split and large test support. | Random Forest, XGBoost, and soft voting. | Random Forest accuracy 0.921, XGBoost accuracy 0.917, Random Forest macro F1 0.72, weighted F1 0.92. | Strong offline classifier baseline. It focuses on model metrics and feature importance, but does not provide Kafka/Flink/Spark/MLflow/PostgreSQL serving infrastructure. |
| Baseline 2 | 499,315 U.S. accidents from 2016-2023 with clear class imbalance. | Weighted XGBoost with randomized search cross-validation and class weighting. | Accuracy 78.2%; severity 2 precision/recall 0.87; severity 3 recall 0.49; severity 4 recall 0.13. | Closest metric profile to this project because it uses weighted XGBoost and discusses rare severity failure modes. |
| Baseline 3 | Multi-year accident severity dataset with leakage-aware train/test separation. | WCFR feature selection, mRMR, CMIM, ReliefF, tree importance, Logistic Regression, Random Forest, and XGBoost. | Accuracy up to approximately 0.84 and macro F1 up to approximately 0.55. | Stronger feature-selection study than this project. It is valuable for future work because WCFR targets feature relevance and redundancy. |
| Baseline 4 | Large accident dataset with 45 features, reduced to 1% for modeling to reduce overfitting risk. | Random Forest, XGBoost, LightGBM, Gradient Boosting, Logistic Regression, Extra Trees, Decision Tree, MLP, SVM, Naive Bayes, QDA. | Best reported model is XGBoost at approximately 85% accuracy; LightGBM 84%; Gradient Boosting 83%. | Broad model comparison with many classical algorithms, but it is still mainly an offline machine-learning workflow. |

## Metric Comparison

| Project / Paper | Best model | Accuracy | Macro F1 | Weighted F1 | Rare-class behavior |
| --- | --- | ---: | ---: | ---: | --- |
| Current project | H2O XGBoost | 0.7955 | 0.4185 | 0.7851 | Severity 1 recall 0.0000; severity 4 recall 0.0700. |
| Baseline 1 | Random Forest | 0.9210 | 0.7200 | 0.9200 | Better rare-class metrics, including severity 4 F1 0.36. |
| Baseline 2 | Weighted XGBoost | 0.7820 | Not reported in the visible text | Not reported in the visible text | Severity 4 recall 0.13; similar imbalance problem. |
| Baseline 3 | WCFR + ML classifiers | ~0.8400 | ~0.5500 | Not reported in the visible text | Better macro-F1 through feature selection. |
| Baseline 4 | XGBoost | ~0.8500 | Not reported in the visible text | Not reported in the visible text | Detailed rare-class result is not available in the visible text. |

## Architecture Comparison

| Capability | Current Project | Paper Baselines |
| --- | --- | --- |
| Offline pretraining before 2020 | Yes. `h2o_before_2020.py` trains only on before-2020 processed features. | Yes for offline modeling, but temporal split rules vary by paper. |
| Realtime simulation after 2020 | Yes. Kafka/Flink and local replay use after-2020 data. | No production replay layer. |
| Streaming engine | Kafka with 3 brokers, 1 topic, 1 partition, and 3 producers; Flink consumes the stream. | Not covered. |
| Batch engine | Spark writes Gold retraining data as both CSV and Parquet. | Not covered. |
| Model registry | MLflow tracks metrics, artifacts, and model versions. | Not covered. |
| Serving database | PostgreSQL/PostGIS table for prediction serving. | Not covered. |
| API backend | FastAPI exposes overview, map predictions, hotspots, scenario simulation, analytics, system status, and docs-compatible aliases. | Not covered. |
| Monitoring | Prometheus and Grafana are configured for local and node-1 cloud monitoring. | Not covered. |
| Leakage control | Training uses before-2020 data; after-2020 data is replay/retraining simulation. EDA separates before and after 2020 and treats after-2020 as observational only. | Baseline 3 is strongest on leakage-aware feature selection; other baselines use standard train/test splits. |

## Key Lessons

1. The current project is stronger than the paper baselines as a deployable Big
   Data system because it includes data ingestion, streaming inference, batch
   retraining, MLflow registration, PostgreSQL serving, FastAPI, and monitoring.
2. The current offline H2O model is weaker than the strongest paper baselines on
   macro F1. The main issue is rare-class recall, especially severity 1 and
   severity 4.
3. Baseline 2 is the fairest direct model comparison because its weighted XGBoost
   accuracy is close to this project, and it reports the same rare severity
   weakness.
4. Baseline 3 suggests the most valuable future improvement: add leakage-safe
   feature selection or richer engineered features before AutoML training.
5. Accuracy alone is not acceptable for this project. Macro F1, weighted F1,
   per-class recall, and confusion matrices must be shown in the report and
   MLflow.

## Recommended Next Improvements

| Priority | Improvement | Expected effect |
| ---: | --- | --- |
| 1 | Add leakage-safe feature selection inspired by WCFR or tree-based feature importance. | Improve macro F1 and reduce weak features. |
| 2 | Add stronger rare-class handling through controlled sampling or class weights. | Improve severity 1 and severity 4 recall. |
| 3 | Add richer location features such as state, city, road segment, or spatial grid. | Improve severity separation beyond simple lat/lon. |
| 4 | Increase H2O runtime on GCP and allow more candidate models. | Improve leaderboard depth and reduce local laptop limitations. |
| 5 | Track class-level thresholds and risk calibration in MLflow. | Make production alerts more reliable than raw severity predictions. |
