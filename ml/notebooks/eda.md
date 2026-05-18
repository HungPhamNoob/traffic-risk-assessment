# Traffic Risk Assessment EDA

This document rewrites the EDA summary from the end-to-end project notes in [`docs/notion.txt`](../../docs/notion.txt). It focuses on the production dataset actually used by the pipeline: **US Accidents**, with a strict temporal split between offline training and realtime replay.

The EDA is not only descriptive. It is used to justify:

- the pre-2020 vs post-2020 split
- the feature schema shared by Flink, Spark, and H2O
- class-balancing choices in training
- which signals should remain in the final model
- why post-2020 data is treated as replay/retraining input instead of offline feature-discovery input

## Data Views Compared

| Symbol | Meaning |
| --- | --- |
| `before_2020_raw` | raw records before 2020, used for offline training |
| `from_2020_raw` | raw records from 2020 onward, used for streaming replay |
| `before_2020_featured` | pre-2020 data after feature engineering |

The engineered schema used in the pipeline contains 26 columns. The original raw source has `7,728,394` rows; after normalization and quality filtering, `6,763,340` valid rows remain for EDA on the shared schema.

## 1. Data Overview

| Dataset | Rows | Columns | Start Year | End Year | Role |
| --- | ---: | ---: | ---: | ---: | --- |
| `before_2020_raw` | 2,976,413 | 26 | 2016 | 2019 | offline raw data |
| `from_2020_raw` | 3,786,927 | 26 | 2020 | 2023 | replay data |
| `before_2020_featured` | 2,975,837 | 26 | 2016 | 2019 | engineered training data |

Interpretation:

- `before_2020_raw` and `before_2020_featured` are almost identical in size, so feature engineering does not discard much data.
- `from_2020_raw` is larger than the pre-2020 split, which makes it suitable for long-running replay and incremental retraining.
- The split is aligned with the real pipeline design:

```text
before 2020  -> offline pretraining
from 2020    -> streaming replay / online simulation / retraining source
```

## 2. Accident Count by Year

Pre-2020 counts:

| Year | Accident Count |
| --- | ---: |
| 2016 | 410,821 |
| 2017 | 717,868 |
| 2018 | 893,423 |
| 2019 | 954,301 |

Post-2020 counts:

| Year | Accident Count |
| --- | ---: |
| 2020 | 1,145,516 |
| 2021 | 1,268,272 |
| 2022 | 1,209,705 |
| 2023 | 163,434 |

Interpretation:

- 2016 to 2019 grows steadily, so the offline training window is large enough and temporally coherent.
- 2020 to 2022 remains very large, which is useful for replay throughput and retraining.
- 2023 drops sharply. This should not be interpreted as a real traffic improvement without verifying data completeness. The more defensible explanation is partial-year coverage.

Conclusion:

- the temporal split is meaningful
- it reduces leakage risk
- it matches the intended offline/online separation in the architecture

## 3. Severity Distribution and Class Imbalance

| Severity | `before_2020` Count | `before_2020` Share | `from_2020` Count | `from_2020` Share |
| --- | ---: | ---: | ---: | ---: |
| 1 | 969 | 0.03% | 66,389 | 1.78% |
| 2 | 1,994,654 | 67.03% | 3,170,917 | 84.92% |
| 3 | 887,867 | 29.84% | 411,380 | 11.02% |
| 4 | 92,347 | 3.10% | 85,201 | 2.28% |

Imbalance summary:

| Period | Majority Class | Minority Class | Ratio |
| --- | ---: | ---: | ---: |
| `before_2020` | 2 | 1 | 2058.47 : 1 |
| `from_2020` | 2 | 1 | 47.76 : 1 |

Interpretation:

- the dataset is strongly imbalanced, especially before 2020
- a naive model can achieve misleading accuracy by overpredicting class `2`
- severity `1` is extremely underrepresented in the offline split

Modeling implications:

- use `balance_classes=True`
- bound class sampling factors
- evaluate `macro_f1`, weighted F1, per-class recall, and confusion matrices
- avoid defending the model with accuracy alone

## 4. Missing Values and Schema Quality

After moving to a unified engineered schema, the critical features used by training and serving are consistently populated:

- `event_id`
- `event_year`
- `event_time`
- `true_severity`
- `lat`, `lon`
- `hour`, `day_of_week`
- weather features
- road-context flags

Interpretation:

- the earlier risk of mixed raw and engineered schemas is removed
- the shared `build_features()` contract is now suitable for Flink realtime inference, Spark Silver-to-Gold processing, and H2O AutoML training

This is one of the strongest results of the EDA because it validates the production preprocessing path, not just a notebook-only workflow.

## 5. Weather Features

Main engineered weather fields:

- `temperature_f`
- `humidity`
- `wind_speed_mph`
- `visibility_mi`

Observed behavior:

- values mostly fall in realistic ranges
- visibility is concentrated near 10 miles, but low-visibility cases still exist and remain useful
- wind speed has a long tail and contains outliers
- temperature and humidity are broad enough to preserve seasonal variation

Production treatment in feature engineering:

| Feature | Clipping Bound |
| --- | --- |
| `temperature_f` | `-40` to `130` |
| `humidity` | `0` to `100` |
| `wind_speed_mph` | `0` to `100` |
| `visibility_mi` | `0` to `10` |

Interpretation:

- clipping avoids extreme values dominating tree splits
- weather features remain informative without allowing obvious sensor noise to distort training

## 6. Time-of-Day Patterns

Accident counts rise strongly during daytime and commute windows. Severity trends do not perfectly follow volume, which means frequency and severity should be analyzed separately.

Important retained features:

- `hour`
- `day_of_week`
- `is_weekend`
- `is_rush_hour`
- `is_night`

Interpretation:

- morning and evening commute periods explain traffic volume concentration
- night and weekend context carry additional severity signal
- these features are simple, interpretable, and operationally useful for scenario simulation

## 7. Weather Code and Road Context

The EDA indicates that severity changes under different:

- `weather_code`
- `road_type_code`
- `is_junction`
- `has_traffic_signal`
- `is_crossing`
- `is_roundabout`
- `is_stop`
- `is_station`
- `is_railway`
- `is_night`

These are not individually strong linear predictors, but they provide meaningful structured context. In practice, the model should learn them through interactions rather than as isolated one-feature effects.

This supports the choice of tree-based models over linear models.

## 8. Correlation With Severity

Top correlations with `true_severity` before 2020:

| Feature | Correlation |
| --- | ---: |
| `road_type_code` | -0.2157 |
| `has_traffic_signal` | -0.2090 |
| `is_crossing` | -0.1764 |
| `is_weekend` | 0.1308 |
| `is_stop` | -0.0781 |
| `is_junction` | 0.0683 |
| `is_night` | 0.0674 |
| `lon` | 0.0555 |
| `is_rush_hour` | -0.0531 |
| `lat` | 0.0454 |

Interpretation:

- linear correlations are generally modest
- no single feature dominates the label
- severity is likely produced by nonlinear combinations of road context, time, weather, and geography

Modeling implication:

- linear-only baselines are insufficient
- H2O AutoML with GBM, XGBoost, Random Forest, and stacked ensembles is a better fit

## 9. Spatial Distribution

The spatial scatter view confirms that accidents are not uniformly distributed. Geographic clustering is expected and meaningful, so retaining `lat` and `lon` is justified for both severity prediction and downstream map-based dashboard analysis.

Interpretation:

- location is not just visualization metadata
- it is part of the predictive structure

## 10. Data Drift: Before 2020 vs From 2020

The strongest drift appears in the label distribution:

- class `2` becomes much more dominant after 2020
- class `3` decreases substantially in share
- class `1` appears more often post-2020 than in the offline split, but is still minor

Interpretation:

- the replay period is not i.i.d. with the offline training period
- blind merging would hide temporal change and weaken the experimental design
- retraining is justified as an explicit architecture component, not as optional polish

Operational implication:

```text
before 2020: offline pretraining
from 2020:   replay + incremental Gold data + scheduled retraining
```

## 11. Feature Engineering Assessment

Strengths:

| Observation | Why it matters |
| --- | --- |
| Pre- and post-processing row counts are nearly identical for pre-2020 data | the pipeline is stable |
| Shared engineered schema is used across Flink, Spark, and H2O | training-serving skew is reduced |
| Weather ranges are clipped consistently | outliers are controlled |
| Time, location, and road context are preserved | the model keeps interpretable signals |

Current limitations:

| Issue | Impact |
| --- | --- |
| extreme class imbalance | severity `1` remains difficult to learn |
| noticeable post-2020 drift | offline metrics may not transfer cleanly |
| weak linear correlations | simpler models will underfit interactions |

## 12. Final Conclusions

The EDA supports the current end-to-end design.

1. The temporal split is correct and should remain unchanged.
2. The production feature schema is stable enough for shared use across streaming, batch, and training.
3. The major modeling risk is class imbalance, not missing values or schema inconsistency.
4. Weather, time, road, and spatial features all deserve to stay in the model.
5. Post-2020 data should be used for replay and retraining, not for initial feature-discovery decisions, to avoid leakage.

In short, the EDA validates the architecture choice:

- **offline pretraining on pre-2020 data**
- **realtime replay from 2020 onward**
- **Spark-built Gold data for scheduled H2O retraining**
