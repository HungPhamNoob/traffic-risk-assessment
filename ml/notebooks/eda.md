# Traffic Risk Assessment EDA Summary

This document summarizes the current EDA notebook in `ml/notebooks/eda.ipynb`. The notebook compares before-2020 offline pretraining data and from-2020 realtime replay data side by side, while using only the before-2020 findings for model-selection decisions.

## Dataset Scope

| Period | Source file | Valid feature rows | Role |
|---|---|---:|---|
| before_2020_pretrain | `data/process/us_train_offline_before_2020.csv` | 2,975,837 | Offline H2O pretraining |
| from_2020_realtime | `data/split/us_pipeline_from_2020.csv` | 3,733,887 | Realtime replay and online retraining simulation |

The raw replay scan skipped 0 invalid rows after applying the shared `build_features()` contract.

## Temporal Split

|   event_year |   before_count |   after_count |
|-------------:|---------------:|--------------:|
|         2016 |         410821 |             0 |
|         2017 |         717290 |             0 |
|         2018 |         893423 |             0 |
|         2019 |         954303 |             0 |
|         2020 |              0 |       1141630 |
|         2021 |              0 |       1341177 |
|         2022 |              0 |       1115901 |
|         2023 |              0 |        135179 |

The current local data matches the intended temporal separation: offline pretraining uses records before 2020, while replay data starts from 2020 onward. The after-2020 period should not be used to choose initial modeling decisions because it represents future/realtime data.

## Severity Distribution And Imbalance

|   severity |     before_count |   before_share |      after_count |   after_share |
|-----------:|-----------------:|---------------:|-----------------:|--------------:|
|          1 |    969           |       0.000326 |  66389           |      0.01778  |
|          2 |      1.99465e+06 |       0.670283 |      3.17092e+06 |      0.849227 |
|          3 | 887867           |       0.298359 | 411380           |      0.110175 |
|          4 |  92347           |       0.031032 |  85201           |      0.022818 |

| period               |    rows |   majority_class |   majority_count |   minority_class |   minority_count |   majority_to_minority_ratio |
|:---------------------|--------:|-----------------:|-----------------:|-----------------:|-----------------:|-----------------------------:|
| before_2020_pretrain | 2975837 |                2 |          1994654 |                1 |              969 |                      2058.47 |
| from_2020_realtime   | 3733887 |                2 |          3170917 |                1 |            66389 |                        47.76 |

Severity is strongly imbalanced in both periods. The before-2020 split is dominated by severity 2, and severity 4 is rare. H2O training therefore uses `balance_classes=True`, bounded `class_sampling_factors`, `sort_metric="mean_per_class_error"`, and report metrics beyond accuracy: macro F1, weighted F1, per-class precision, per-class recall, and confusion matrix.

## Engineered Schema Quality

Top engineered missing rates:

| feature            |   before_missing_count |   before_missing_share |   after_sample_missing_count |   after_sample_missing_share |
|:-------------------|-----------------------:|-----------------------:|-----------------------------:|-----------------------------:|
| event_id           |                      0 |                      0 |                            0 |                            0 |
| event_year         |                      0 |                      0 |                            0 |                            0 |
| event_time         |                      0 |                      0 |                            0 |                            0 |
| true_severity      |                      0 |                      0 |                            0 |                            0 |
| lat                |                      0 |                      0 |                            0 |                            0 |
| lon                |                      0 |                      0 |                            0 |                            0 |
| hour               |                      0 |                      0 |                            0 |                            0 |
| day_of_week        |                      0 |                      0 |                            0 |                            0 |
| is_weekend         |                      0 |                      0 |                            0 |                            0 |
| is_rush_hour       |                      0 |                      0 |                            0 |                            0 |
| weather_code       |                      0 |                      0 |                            0 |                            0 |
| temperature_f      |                      0 |                      0 |                            0 |                            0 |
| humidity           |                      0 |                      0 |                            0 |                            0 |
| wind_speed_mph     |                      0 |                      0 |                            0 |                            0 |
| visibility_mi      |                      0 |                      0 |                            0 |                            0 |
| road_type_code     |                      0 |                      0 |                            0 |                            0 |
| is_junction        |                      0 |                      0 |                            0 |                            0 |
| has_traffic_signal |                      0 |                      0 |                            0 |                            0 |
| is_crossing        |                      0 |                      0 |                            0 |                            0 |
| is_roundabout      |                      0 |                      0 |                            0 |                            0 |

The engineered schema is now consistent across both periods. This fixes the earlier risk where raw columns and engineered columns were mixed together, causing apparent 50% missingness after concatenation. Training and serving should consume only the unified feature schema.

## Numeric Feature Ranges

| feature        |   count_before |   mean_before |   std_before |   min_before |   25%_before |   50%_before |   75%_before |   max_before |   count_after_sample |   mean_after_sample |   std_after_sample |   min_after_sample |   25%_after_sample |   50%_after_sample |   75%_after_sample |   max_after_sample |
|:---------------|---------------:|--------------:|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|---------------------:|--------------------:|-------------------:|-------------------:|-------------------:|-------------------:|-------------------:|-------------------:|
| temperature_f  |    2.97584e+06 |        62.11  |       18.687 |      -40     |       50     |       64     |       75.9   |      130     |               300000 |              60.88  |             21.082 |            -37     |             46     |             62     |             75     |            130     |
| humidity       |    2.97584e+06 |        65.102 |       22.435 |        1     |       49     |       67     |       84     |      100     |               300000 |              66.884 |             23.044 |              1     |             50     |             70     |             87     |            100     |
| wind_speed_mph |    2.97584e+06 |         7.065 |        5.356 |        0     |        3.5   |        6.9   |       10.4   |      100     |               300000 |               8.873 |             13.639 |              0     |              3     |              7     |             10     |            100     |
| visibility_mi  |    2.97584e+06 |         9.07  |        2.259 |        0     |       10     |       10     |       10     |       10     |               300000 |               9.073 |              2.314 |              0     |             10     |             10     |             10     |             10     |
| lat            |    2.97584e+06 |        36.495 |        4.919 |       24.555 |       33.551 |       35.851 |       40.378 |       49.002 |               300000 |              36.01  |              4.735 |             24.555 |             33.215 |             35.284 |             39.946 |             48.998 |
| lon            |    2.97584e+06 |       -95.419 |       17.217 |     -124.624 |     -117.289 |      -90.24  |      -80.917 |      -67.113 |               300000 |             -91.734 |             15.835 |           -124.497 |            -97.675 |            -85.659 |            -80.794 |            -68.161 |

Weather outliers are handled in `processing.feature_engineering` by clipping to defensible bounds:

| Feature | Bound |
|---|---|
| temperature_f | -40 to 130 |
| humidity | 0 to 100 |
| wind_speed_mph | 0 to 100 |
| visibility_mi | 0 to 10 |

This keeps abnormal sensor or weather-station values from dominating H2O tree splits while preserving valid accident records.

## Hourly Pattern

|   hour |   before_count |   before_avg_severity |   after_sample_count |   after_sample_avg_severity |
|-------:|---------------:|----------------------:|---------------------:|----------------------------:|
|      0 |          23181 |                2.6355 |                 1274 |                      2.4882 |
|      1 |          17979 |                2.6024 |                 1112 |                      2.5576 |
|      2 |          18943 |                2.6207 |                 1867 |                      2.286  |
|      3 |          18606 |                2.6411 |                 2016 |                      2.3135 |
|      4 |          53613 |                2.4424 |                12978 |                      2.0606 |
|      5 |          83929 |                2.4068 |                14234 |                      2.0668 |
|      6 |         169213 |                2.3492 |                27036 |                      2.054  |
|      7 |         273753 |                2.2959 |                40018 |                      2.0127 |
|      8 |         284490 |                2.2864 |                38197 |                      1.995  |
|      9 |         177627 |                2.323  |                13456 |                      2.2209 |
|     10 |         157935 |                2.319  |                11406 |                      2.2839 |
|     11 |         155403 |                2.3183 |                10434 |                      2.3485 |
|     12 |         139220 |                2.3521 |                 8673 |                      2.5331 |
|     13 |         142056 |                2.3631 |                 9624 |                      2.5072 |
|     14 |         150269 |                2.3678 |                11032 |                      2.4928 |
|     15 |         177683 |                2.3663 |                14999 |                      2.4292 |
|     16 |         212684 |                2.3595 |                21108 |                      2.2771 |
|     17 |         221759 |                2.3603 |                20906 |                      2.2682 |
|     18 |         167452 |                2.3768 |                16757 |                      2.31   |
|     19 |         115470 |                2.3872 |                 9834 |                      2.3319 |
|     20 |          81772 |                2.4243 |                 4834 |                      2.5362 |
|     21 |          57739 |                2.4662 |                 3883 |                      2.5856 |
|     22 |          48328 |                2.4916 |                 2934 |                      2.6714 |
|     23 |          26733 |                2.5694 |                 1388 |                      2.5821 |

The hourly profile supports keeping `hour`, `is_rush_hour`, `is_weekend`, and `is_night`. Rush-hour windows and night context provide interpretable operational features for both dashboard explanations and scenario simulation.

## Correlation With Severity

Top before-2020 correlations:

| feature            |   before_corr_with_true_severity |
|:-------------------|---------------------------------:|
| road_type_code     |                        -0.215736 |
| has_traffic_signal |                        -0.209038 |
| is_crossing        |                        -0.17637  |
| is_weekend         |                         0.130781 |
| is_stop            |                        -0.07809  |
| is_junction        |                         0.068347 |
| is_night           |                         0.067376 |
| is_station         |                        -0.062818 |
| lon                |                         0.055519 |
| is_rush_hour       |                        -0.053057 |
| lat                |                         0.045368 |
| temperature_f      |                        -0.029362 |

Top after-2020 sample correlations:

| feature            |   after_sample_corr_with_true_severity |
|:-------------------|---------------------------------------:|
| road_type_code     |                              -0.333452 |
| has_traffic_signal |                              -0.26074  |
| is_weekend         |                               0.224895 |
| is_crossing        |                              -0.209912 |
| hour               |                               0.205579 |
| is_rush_hour       |                              -0.126706 |
| humidity           |                              -0.10266  |
| temperature_f      |                              -0.098993 |
| is_stop            |                              -0.098809 |
| is_junction        |                               0.085461 |
| is_station         |                              -0.080751 |
| lat                |                               0.070881 |

Linear correlations are generally weak, so a linear-only model is not a good primary model. H2O GBM, XGBoost, Random Forest, and stacked ensembles are more appropriate because severity risk is likely driven by nonlinear interactions between road context, time, location, and weather.

## Vendor Notebook Lessons Used

| source                         | reused_lessons                                                                                                                           |
|:-------------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------|
| Vendor Kaggle notebooks        | Temporal feature extraction, missing-value review, outlier handling, severity imbalance review, and tree-based classification baselines. |
| Project feature_engineering.py | Single schema for offline H2O training, Flink inference, and Spark Gold retraining.                                                      |
| Before-2020 EDA only           | Model choices use the offline period only to avoid leakage from realtime replay data.                                                    |

The project keeps the useful EDA ideas from the vendor notebooks but does not copy their notebook-only preprocessing directly. Production preprocessing is centralized in `processing.feature_engineering.build_features()` so Flink, Spark, H2O, and FastAPI stay aligned.

## Modeling Decisions

| Decision | Reason |
|---|---|
| Use `true_severity` as the only label | It is generated directly from US Accidents `Severity` and is present in both offline and replay features. |
| Exclude `event_id`, `event_year`, and `event_time` from features | They are metadata or split keys, not stable model inputs. |
| Keep `lat` and `lon` | The accident distribution is spatial, and location is useful for map risk serving. |
| Keep weather and road flags | Individual correlations are modest, but tree models can learn interactions. |
| Use class balancing and sampling factors | Severity 4 is rare and must not be ignored by the model. |
| Evaluate macro F1 and class recall | Accuracy can look good while minority-class recall fails. |

## Conclusion

The data is suitable for the requested MLOps pipeline after enforcing the unified feature schema and clipped numeric ranges. The main risk is not schema anymore; it is class imbalance. The training scripts now expose explicit class-sampling factors and log complete MLflow metrics so the capstone report can defend the model beyond accuracy.
