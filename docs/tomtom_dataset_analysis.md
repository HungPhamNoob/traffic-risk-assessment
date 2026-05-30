# TomTom Dataset Analysis

This document analyzes `data/process/tomtom_pipeline_features.csv`, which is the
TomTom feature-engineered dataset used by the project pipeline.

## 1. What this dataset is

`tomtom_pipeline_features.csv` is not the raw TomTom API response. It is the
post-enrichment, post-feature-engineering output that has already been projected
into the same model feature contract used by the H2O training pipeline:

- raw source: TomTom Traffic Incident Details API
- producer: `ingestion/kafka/tomtom_producer.py`
- streaming enrichment: `processing/streaming_enrichment.py`
- shared feature engineering: `processing/feature_engineering.py`

That means the file is already model-ready and much closer to a silver/gold
feature table than to a bronze raw incident log.

## 2. Dataset size and schema

### Shape

- Rows: `449`
- Columns: `24`
- Duplicate `event_id`: `0`
- Missing values: `0` in every column

### Columns

| Group | Columns |
| --- | --- |
| Identity/time | `event_id`, `event_year`, `event_time` |
| Label | `true_severity` |
| Geometry | `lat`, `lon` |
| Time features | `hour`, `day_of_week`, `is_weekend`, `is_rush_hour`, `is_night` |
| Weather features | `weather_code`, `temperature_f`, `humidity`, `wind_speed_mph`, `visibility_mi` |
| Road/context features | `road_type_code`, `is_junction`, `has_traffic_signal`, `is_crossing`, `is_roundabout`, `is_stop`, `is_station`, `is_railway` |

### Data types

- `event_id`, `event_time`: string
- most calendar/context fields: integer
- coordinates/weather fields: float
- `true_severity`: integer class label in `1..4`

## 3. Provenance and transformation logic

### 3.1 Raw TomTom source

The upstream TomTom source is incident-oriented, not accident-history-oriented.
Important raw fields include:

- `iconCategory`
- `magnitudeOfDelay`
- `delay`
- `length`
- `from`, `to`, `roadNumbers`
- `startTime`, `lastReportTime`
- GeoJSON geometry

The producer normalizes each API incident into a streaming event with fields
such as `event_id`, `latitude`, `longitude`, `icon_category`,
`delay_magnitude`, `delay_seconds`, and `incident_description`.

### 3.2 Severity labeling

`true_severity` here is not a human-annotated accident severity label like the
US Accidents dataset. It is synthesized from TomTom incident signals:

- `magnitudeOfDelay >= 4` -> severity `4`
- `magnitudeOfDelay == 3` -> severity `3`
- `magnitudeOfDelay == 2` -> severity `2`
- `magnitudeOfDelay == 0/1/null` -> severity `1`
- `iconCategory == 8` pushes severity to at least `4`
- `iconCategory == 1` pushes severity to at least `3`
- `iconCategory == 9` pushes severity to at least `2`

So this label is a **derived operational severity**, not a ground-truth injury
or damage severity.

### 3.3 Weather enrichment

TomTom Incident Details does not provide the weather fields required by the
shared model contract. The pipeline enriches weather via Open-Meteo using event
time and coordinates, then maps the weather text into project weather codes.

Weather code meaning:

| Code | Meaning |
| ---: | --- |
| `0` | clear / unknown / normal |
| `1` | rain |
| `2` | snow / ice / sleet |
| `3` | fog / haze / mist |
| `4` | thunder / storm |
| `5` | cloudy / overcast |
| `6` | windy |

### 3.4 Road type encoding

`road_type_code` is inferred from road/street text, not from a dedicated TomTom
road-class field.

| Code | Meaning |
| ---: | --- |
| `0` | unknown / local road |
| `1` | interstate / freeway / highway |
| `2` | route / state route / US route |
| `3` | street |
| `4` | avenue |
| `5` | boulevard |
| `6` | drive |
| `7` | road |

## 4. Temporal coverage

### Time range

- Earliest event: `2022-03-22T17:04:01+00:00`
- Latest event: `2026-05-26T10:31:06+00:00`

### Year distribution

| Year | Rows |
| ---: | ---: |
| 2022 | 5 |
| 2023 | 5 |
| 2024 | 19 |
| 2025 | 24 |
| 2026 | 396 |

### Key observation

This dataset is heavily concentrated in `2026` and looks like a **small
operational sample captured across a handful of pipeline runs**, not a stable
multi-year historical training corpus. The single busiest date is:

- `2026-05-26`: `232` rows

That makes this file useful for pipeline validation and feature inspection, but
not strong enough by itself for robust offline model training.

## 5. Geographic coverage

### Coordinate bounds

- Latitude: `40.524526` to `40.919861`
- Longitude: `-74.264476` to `-73.625094`

These bounds are consistent with the configured New York metro bbox used by the
TomTom producer defaults in this repo. So despite the TomTom docs mentioning
multi-region support, this particular file looks like a **New York-focused
sample** rather than a broad US/UK dataset.

## 6. Target label distribution

### Severity counts

| Severity | Rows | Share |
| ---: | ---: | ---: |
| 1 | 164 | 36.53% |
| 2 | 79 | 17.59% |
| 3 | 99 | 22.05% |
| 4 | 107 | 23.83% |

### Interpretation

This is much more balanced than the US Accidents before-2020 training data. In
the TomTom file:

- severity `1` is the largest class, but not overwhelmingly dominant
- severity `4` is also common
- class balance is relatively friendly for demo modeling

That balance is a consequence of the TomTom severity normalization logic rather
than a natural accident-outcome distribution.

## 7. Feature distribution

### 7.1 Time features

- mean hour: `11.95`
- median hour: `10`
- rush-hour rows: `126 / 449` (`28.06%`)
- weekend rows: `35 / 449` (`7.80%`)
- night rows: `28 / 449` (`6.24%`)

Hour distribution is strongly concentrated around:

- `10:00` -> `212` rows
- `17:00` -> `38` rows
- `09:00` -> `33` rows

This suggests the file reflects polling snapshots or incident collection bursts,
not a naturally smooth 24-hour stream.

### 7.2 Weather features

Summary:

- mean temperature: `58.20 F`
- mean humidity: `74.83%`
- mean wind speed: `5.31 mph`
- visibility: always `10.0 mi`

Weather code distribution:

| Weather code | Meaning | Rows |
| ---: | --- | ---: |
| 0 | clear / unknown | 250 |
| 1 | rain | 46 |
| 2 | snow / ice | 2 |
| 5 | cloudy / overcast | 151 |

Important note:

- `visibility_mi` is constant at `10.0` for every row, so it adds no signal in
  this sample.
- the weather feature space is narrow; codes `3`, `4`, and `6` do not appear.

### 7.3 Road/context features

`road_type_code` distribution:

| Road type code | Meaning | Rows |
| ---: | --- | ---: |
| 0 | unknown/local | 80 |
| 1 | interstate/highway | 63 |
| 2 | route | 18 |
| 3 | street | 122 |
| 4 | avenue | 125 |
| 5 | boulevard | 20 |
| 6 | drive | 3 |
| 7 | road | 18 |

Binary context flags:

- `is_junction`: all `0`
- `has_traffic_signal`: all `0`
- `is_crossing`: all `0`
- `is_roundabout`: all `0`
- `is_stop`: all `0`
- `is_station`: all `0`
- `is_railway`: all `0`

This is one of the biggest limitations of the dataset. A large chunk of the
shared feature contract exists structurally, but TomTom does not naturally fill
those fields, so they collapse to constants here.

## 8. Relationships worth noting

### Severity vs rush hour

Distribution by `is_rush_hour`:

- non-rush rows: more mixed (`1: 30.3%`, `2: 19.5%`, `3: 27.6%`, `4: 22.6%`)
- rush-hour rows: more polarized toward severity `1` and `4`
  (`1: 52.4%`, `4: 27.0%`)

### Severity vs night

- non-night rows: fairly spread
- night rows: strongly concentrated in severity `4` (`71.4%`)

But the night sample is only `28` rows, so this is directional, not conclusive.

### Severity vs weather

- rain (`weather_code=1`) shows a relatively high share of severity `4`
- snow (`weather_code=2`) has only `2` rows, so it cannot support inference

## 9. Data quality assessment

### Strengths

- no missing values
- no duplicate `event_id`
- clean schema aligned with the model contract
- balanced severity labels for experimentation
- coordinates and timestamps look valid

### Weaknesses

- dataset is very small (`449` rows)
- highly concentrated in one recent year and one dominant date
- many engineered binary road-context features are constant zeros
- visibility is constant
- severity is derived, not human-labeled ground truth
- weather is externally enriched, not native to TomTom
- sample appears geographically narrow

## 10. Modeling implications

### Good use cases

- validating the TomTom ingestion -> enrichment -> feature-engineering pipeline
- demoing real-time scoring on incident-like streaming data
- sanity-checking whether the H2O contract can accept non-US-Accidents sources
- lightweight exploratory modeling

### Weak use cases

- training a production-grade standalone TomTom risk model from scratch
- comparing directly against the US Accidents offline corpus as if labels were
  equivalent
- drawing strong causal conclusions about crash severity

## 11. Bottom line

`tomtom_pipeline_features.csv` is best understood as a **small, clean,
feature-engineered operational sample of TomTom traffic incidents**, not as a
full historical accident dataset.

Its main value in this repo is:

1. proving that TomTom incident data can be projected into the shared pipeline
   schema
2. supporting streaming demos and feature-contract validation
3. showing what information is lost when incident APIs are forced into an
   accident-risk feature space

Its main limitation is also clear:

the shared H2O feature contract contains many fields that are natural for US
Accidents data but weak or synthetic for TomTom, so this dataset is structurally
compatible with the pipeline while still being semantically different from the
offline training corpus.
