#!/usr/bin/env python3
"""
H2O AutoML Training - US Traffic Accident Risk Prediction

Purpose:
    Load offline feature CSV, train H2O AutoML classification model,
    log comprehensive metrics and model artifact to MLflow,
    and register the best model in MLflow Registry.

Input:
    data/process/us_train_offline_before_2020.csv

Output:
    MLflow experiment run with metrics, model artifact, and registry entry.

Example command:
    python ml/training/h2o_before_2020.py
"""

import logging
import os
import csv
import sys
import tempfile
from pathlib import Path
import h2o
from h2o.automl import H2OAutoML
import mlflow
import mlflow.h2o
import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_recall_fscore_support,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from processing.feature_engineering import build_features  # noqa: E402

# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("h2o-training")


# ============================================================
# Configuration
# ============================================================

DATA_PATH = os.getenv(
    "US_TRAIN_OFFLINE_PATH", "data/process/us_train_offline_before_2020.csv"
)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MLFLOW_EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "traffic-risk-assessment")

MODEL_NAME = os.getenv("ML_MODEL_NAME", "traffic-risk-model")
MAX_RUNTIME_SECS = int(os.getenv("H2O_MAX_RUNTIME", "3600"))
H2O_MAX_MEM = os.getenv("H2O_MAX_MEM", "2G")
H2O_NTHREADS = int(os.getenv("H2O_NTHREADS", "2"))
USE_CLASS_SAMPLING_FACTORS = (
    os.getenv("H2O_USE_CLASS_SAMPLING_FACTORS", "true").lower() == "true"
)
MAX_CLASS_SAMPLING_FACTOR = float(os.getenv("H2O_MAX_CLASS_SAMPLING_FACTOR", "100.0"))

# Label column in the CSV
LABEL_COLUMN = "true_severity"

# Columns to exclude from feature vector (metadata, identifiers, label)
EXCLUDED_COLUMNS = {
    "event_id",  # unique identifier, not a feature
    "event_year",  # temporal split key, not a feature
    "event_time",  # timestamp metadata; derived time fields are used instead
    "true_severity",  # the label we want to predict
    "ingestion_time_epoch",  # pipeline metric, not a traffic-risk feature
    "processed_time_epoch",  # pipeline metric, not a traffic-risk feature
    "end_to_end_latency_ms",  # observability metric, not a model feature
}

FEATURE_COLUMNS = [
    "event_id",
    "event_year",
    "event_time",
    "true_severity",
    "lat",
    "lon",
    "hour",
    "day_of_week",
    "is_weekend",
    "is_rush_hour",
    "weather_code",
    "temperature_f",
    "humidity",
    "wind_speed_mph",
    "visibility_mi",
    "road_type_code",
    "is_junction",
    "has_traffic_signal",
    "is_crossing",
    "is_roundabout",
    "is_stop",
    "is_station",
    "is_railway",
    "is_night",
]

RAW_REQUIRED_COLUMNS = {"ID", "Severity", "Start_Time", "Start_Lat", "Start_Lng"}

# Random seed for reproducibility
SEED = int(os.getenv("H2O_SEED", "42"))


# ============================================================
# Helper: Safely log metrics that may not be available
# ============================================================


def log_metric_if_exists(perf, metric_name, mlflow_name=None, log_prefix="  "):
    """
    Safely extract a metric from H2O model performance and log to MLflow + console.

    Handles missing metrics, NaN values, and multi-class metrics gracefully.
    """
    if mlflow_name is None:
        mlflow_name = metric_name

    try:
        # For metrics that return a list/tuple (e.g., F1, precision, recall)
        if metric_name in ("F1", "precision", "recall", "specificity", "sensitivity"):
            if hasattr(perf, metric_name):
                values = getattr(perf, metric_name)()
                for row in values:
                    class_label = row[0]
                    val = float(row[1])
                    mlflow.log_metric(f"{mlflow_name}_class_{class_label}", val)
                    logger.info(
                        "%s%s (class %s):          %.6f",
                        log_prefix,
                        metric_name.capitalize(),
                        class_label,
                        val,
                    )
                return True

        # For single-value metrics
        elif hasattr(perf, metric_name):
            val = float(getattr(perf, metric_name)())
            if not (val != val):  # NaN check
                mlflow.log_metric(mlflow_name, val)
                logger.info(
                    "%s%s:                   %.6f",
                    log_prefix,
                    metric_name.capitalize(),
                    val,
                )
                return True
    except (KeyError, AttributeError, TypeError, ValueError):
        pass

    logger.info("%s%s:                   N/A", log_prefix, metric_name.capitalize())
    return False


def evaluate_classifier_with_sklearn(
    model,
    test_frame,
    feature_columns: list[str],
    label_column: str,
) -> tuple[dict[str, float], pd.DataFrame, pd.DataFrame]:
    """
    Compute classification metrics that H2O does not always expose for multiclass models.

    H2O's native performance object may omit accuracy, weighted F1, macro F1,
    precision, or recall depending on the model family. The capstone report and
    MLflow comparison need these metrics consistently, so this helper converts
    the validation labels and predictions into pandas series and evaluates them
    with scikit-learn.
    """
    prediction_frame = model.predict(test_frame[feature_columns])
    prediction_df = prediction_frame.as_data_frame()
    y_pred = prediction_df["predict"].astype(str)
    y_true = test_frame[label_column].as_data_frame()[label_column].astype(str)
    labels = sorted(
        set(y_true.tolist()) | set(y_pred.tolist()), key=lambda value: int(float(value))
    )

    precision, recall, f1, support = precision_recall_fscore_support(
        y_true,
        y_pred,
        labels=labels,
        zero_division=0,
    )

    metrics = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "macro_precision": float(
            precision_recall_fscore_support(
                y_true, y_pred, labels=labels, average="macro", zero_division=0
            )[0]
        ),
        "macro_recall": float(
            precision_recall_fscore_support(
                y_true, y_pred, labels=labels, average="macro", zero_division=0
            )[1]
        ),
        "macro_f1": float(
            f1_score(y_true, y_pred, labels=labels, average="macro", zero_division=0)
        ),
        "weighted_precision": float(
            precision_recall_fscore_support(
                y_true, y_pred, labels=labels, average="weighted", zero_division=0
            )[0]
        ),
        "weighted_recall": float(
            precision_recall_fscore_support(
                y_true, y_pred, labels=labels, average="weighted", zero_division=0
            )[1]
        ),
        "weighted_f1": float(
            f1_score(y_true, y_pred, labels=labels, average="weighted", zero_division=0)
        ),
    }

    report_df = pd.DataFrame(
        {
            "class_label": labels,
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "support": support,
        }
    )

    matrix = confusion_matrix(y_true, y_pred, labels=labels)
    confusion_df = pd.DataFrame(
        matrix,
        index=[f"actual_{label}" for label in labels],
        columns=[f"predicted_{label}" for label in labels],
    )

    return metrics, report_df, confusion_df


def log_classifier_metrics(
    metrics: dict[str, float],
    report_df: pd.DataFrame,
    confusion_df: pd.DataFrame,
    artifact_prefix: str,
) -> None:
    """
    Log complete classification metrics and readable CSV artifacts to MLflow.

    Metrics are also printed to the console so training logs remain useful even
    when the MLflow UI is not available during local development.
    """
    for metric_name, metric_value in metrics.items():
        mlflow.log_metric(metric_name, metric_value)
        logger.info("  %-24s %.6f", metric_name + ":", metric_value)

    for _, row in report_df.iterrows():
        class_label = str(row["class_label"])
        mlflow.log_metric(f"class_{class_label}_precision", float(row["precision"]))
        mlflow.log_metric(f"class_{class_label}_recall", float(row["recall"]))
        mlflow.log_metric(f"class_{class_label}_f1", float(row["f1"]))
        mlflow.log_metric(f"class_{class_label}_support", float(row["support"]))

    logger.info("  Per-class classification report:\n%s", report_df)
    logger.info("  Confusion matrix table:\n%s", confusion_df)

    with tempfile.TemporaryDirectory() as tmpdir:
        report_path = os.path.join(
            tmpdir, f"{artifact_prefix}_classification_report.csv"
        )
        confusion_path = os.path.join(tmpdir, f"{artifact_prefix}_confusion_matrix.csv")
        report_df.to_csv(report_path, index=False)
        confusion_df.to_csv(confusion_path)
        try:
            mlflow.log_artifact(report_path, artifact_path="metrics")
            mlflow.log_artifact(confusion_path, artifact_path="metrics")
        except Exception:
            logger.exception(
                "Metric CSV artifact upload failed. Metrics are already logged, "
                "so training will continue to model registration."
            )


def build_class_sampling_factors(label_distribution) -> tuple[list[float] | None, dict]:
    """
    Compute bounded H2O class sampling factors from the observed label distribution.

    The US accident severity label is highly imbalanced. H2O's `balance_classes`
    option helps, but explicit sampling factors make the rare-class strategy
    auditable in training logs and MLflow. Factors are ordered by numeric class
    label because H2O expects the list to follow the response domain order.
    """
    if not USE_CLASS_SAMPLING_FACTORS:
        return None, {}

    distribution_df = label_distribution.as_data_frame()
    label_column = distribution_df.columns[0]
    count_column = "Count"
    counts = {
        str(int(float(row[label_column]))): float(row[count_column])
        for _, row in distribution_df.iterrows()
    }
    if not counts:
        return None, {}

    max_count = max(counts.values())
    ordered_labels = sorted(counts.keys(), key=lambda value: int(float(value)))
    factors = [
        round(min(max_count / counts[label], MAX_CLASS_SAMPLING_FACTOR), 6)
        for label in ordered_labels
    ]
    metadata = {
        "ordered_labels": ordered_labels,
        "class_counts": counts,
        "class_sampling_factors": factors,
        "max_class_sampling_factor": MAX_CLASS_SAMPLING_FACTOR,
    }
    return factors, metadata


def materialize_training_csv(data_path: str) -> str:
    """
    Return a local CSV path that H2O can import reliably.

    H2O can import local files consistently in both laptops and GCP VMs. When
    the configured path is a GCS URI, this helper downloads it to /tmp first and
    leaves the original object unchanged.
    """
    if not data_path.startswith("gs://"):
        if not os.path.exists(data_path):
            raise FileNotFoundError(
                f"Training data not found: {data_path}. "
                "Run data splitting and offline feature engineering first."
            )
        return ensure_feature_training_csv(data_path)

    import gcsfs

    local_path = os.path.join(tempfile.gettempdir(), os.path.basename(data_path))
    logger.info("Downloading GCS training CSV from %s to %s", data_path, local_path)

    fs = gcsfs.GCSFileSystem()
    with fs.open(data_path, "rb") as src, open(local_path, "wb") as dst:
        while True:
            chunk = src.read(16 * 1024 * 1024)
            if not chunk:
                break
            dst.write(chunk)

    return ensure_feature_training_csv(local_path)


def read_csv_header(csv_path: str) -> list[str]:
    """
    Read only the CSV header so the training job can choose the correct schema path.

    Cloud training may receive either an already-engineered feature CSV or a raw
    US Accidents CSV. Inspecting the header avoids loading the multi-gigabyte
    file into pandas just to decide whether feature engineering is required.
    """
    with open(csv_path, "r", encoding="utf-8", newline="") as csv_file:
        reader = csv.reader(csv_file)
        return next(reader)


def ordered_feature_row(feature_row: dict) -> dict:
    """Return one engineered row with stable column order for H2O CSV import."""
    return {column: feature_row.get(column) for column in FEATURE_COLUMNS}


def build_feature_csv_from_raw_csv(raw_csv_path: str) -> str:
    """
    Stream-convert a raw US Accidents CSV into the unified feature schema.

    The GCS cloud object can be the original US Accidents schema with columns
    such as `Severity`, `Start_Time`, and `Start_Lat`. The serving pipeline uses
    engineered fields such as `true_severity`, `hour`, `weather_code`, and road
    flags. This conversion keeps offline training aligned with Flink inference
    without loading the full raw dataset into memory.
    """
    output_path = os.path.join(
        tempfile.gettempdir(),
        f"{Path(raw_csv_path).stem}_features_for_h2o.csv",
    )
    logger.info(
        "Raw US Accidents schema detected. Building feature CSV at %s",
        output_path,
    )

    processed_count = 0
    written_count = 0
    skipped_count = 0
    log_interval = int(os.getenv("OFFLINE_FEATURE_LOG_INTERVAL", "250000"))

    with open(raw_csv_path, "r", encoding="utf-8", newline="") as input_file:
        reader = csv.DictReader(input_file)
        with open(output_path, "w", encoding="utf-8", newline="") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=FEATURE_COLUMNS)
            writer.writeheader()

            for raw_row in reader:
                processed_count += 1
                feature_row = build_features(raw_row)
                if feature_row is None:
                    skipped_count += 1
                else:
                    writer.writerow(ordered_feature_row(feature_row))
                    written_count += 1

                if processed_count % log_interval == 0:
                    logger.info(
                        "Feature conversion progress: processed=%s, written=%s, skipped=%s",
                        f"{processed_count:,}",
                        f"{written_count:,}",
                        f"{skipped_count:,}",
                    )

    if written_count == 0:
        raise ValueError(
            "Feature conversion produced zero rows. Check raw CSV schema and critical fields."
        )

    logger.info(
        "Feature conversion completed: processed=%s, written=%s, skipped=%s",
        f"{processed_count:,}",
        f"{written_count:,}",
        f"{skipped_count:,}",
    )
    return output_path


def ensure_feature_training_csv(csv_path: str) -> str:
    """
    Return an H2O-ready feature CSV regardless of raw or engineered input.

    If the input already contains `true_severity`, it is already in the expected
    training schema. If the input contains the raw US Accidents columns, it is
    converted through the same shared feature engineering function used by the
    streaming and batch jobs.
    """
    header = read_csv_header(csv_path)
    header_set = set(header)

    if LABEL_COLUMN in header_set:
        logger.info("Feature CSV schema detected. Using %s directly.", csv_path)
        return csv_path

    if RAW_REQUIRED_COLUMNS.issubset(header_set):
        return build_feature_csv_from_raw_csv(csv_path)

    raise ValueError(
        "Training CSV does not match the feature schema or raw US Accidents schema. "
        f"Missing label column '{LABEL_COLUMN}' and required raw columns "
        f"{sorted(RAW_REQUIRED_COLUMNS - header_set)}. Header columns: {header}"
    )


# ============================================================
# Main training function
# ============================================================


def main():
    logger.info("=" * 80)
    logger.info("H2O AutoML Training - Traffic Risk Assessment")
    logger.info("=" * 80)
    logger.info("Data path:              %s", DATA_PATH)
    logger.info("MLflow tracking URI:    %s", MLFLOW_TRACKING_URI)
    logger.info("MLflow experiment:      %s", MLFLOW_EXPERIMENT_NAME)
    logger.info("Registered model name:  %s", MODEL_NAME)
    logger.info("Max runtime (seconds):  %s", MAX_RUNTIME_SECS)
    logger.info("H2O max memory:         %s", H2O_MAX_MEM)
    logger.info("H2O threads:            %s", H2O_NTHREADS)
    logger.info("Random seed:            %s", SEED)

    # ---- Step 1: Initialize H2O cluster ----
    logger.info("Step 1: Initializing H2O cluster...")
    h2o.init(
        max_mem_size=H2O_MAX_MEM,
        nthreads=H2O_NTHREADS,
    )
    logger.info("H2O cluster initialized successfully.")

    # ---- Step 2: Configure MLflow ----
    logger.info("Step 2: Configuring MLflow tracking...")
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

    # ---- Step 3: Load data ----
    logger.info("Step 3: Loading training data from CSV...")
    local_data_path = materialize_training_csv(DATA_PATH)
    df_h2o = h2o.import_file(local_data_path)
    logger.info("Loaded %s rows, %s columns", f"{df_h2o.nrows:,}", df_h2o.ncols)

    # Convert label to categorical factor
    df_h2o[LABEL_COLUMN] = df_h2o[LABEL_COLUMN].asfactor()

    # Log label distribution
    label_dist = df_h2o[LABEL_COLUMN].table()
    logger.info("Label distribution:\n%s", label_dist)
    class_sampling_factors, class_sampling_metadata = build_class_sampling_factors(
        label_dist
    )
    if class_sampling_factors:
        logger.info(
            "Class sampling labels:  %s",
            class_sampling_metadata["ordered_labels"],
        )
        logger.info("Class sampling factors: %s", class_sampling_factors)

    # ---- Step 4: Train/test split ----
    logger.info("Step 4: Splitting into train/test (80/20)...")
    train, test = df_h2o.split_frame(ratios=[0.8], seed=SEED)
    logger.info("Training rows:   %s", f"{train.nrows:,}")
    logger.info("Test rows:       %s", f"{test.nrows:,}")

    # ---- Step 5: Define features ----
    feature_columns = [c for c in df_h2o.columns if c not in EXCLUDED_COLUMNS]
    logger.info("Feature columns (%s): %s", len(feature_columns), feature_columns)
    logger.info("Label column:         %s", LABEL_COLUMN)

    # ---- Step 6: Train H2O AutoML ----
    logger.info("Step 6: Starting H2O AutoML training...")
    logger.info("Max runtime: %s seconds", MAX_RUNTIME_SECS)

    with mlflow.start_run(run_name="h2o_automl") as run:
        run_id = run.info.run_id
        logger.info("MLflow run ID: %s", run_id)

        # Log training parameters
        mlflow.log_param("max_runtime_secs", MAX_RUNTIME_SECS)
        mlflow.log_param("seed", SEED)
        mlflow.log_param("n_features", len(feature_columns))
        mlflow.log_param("n_train_rows", train.nrows)
        mlflow.log_param("n_test_rows", test.nrows)
        if class_sampling_factors:
            mlflow.log_param(
                "class_sampling_labels",
                ",".join(class_sampling_metadata["ordered_labels"]),
            )
            mlflow.log_param(
                "class_sampling_factors",
                ",".join(str(value) for value in class_sampling_factors),
            )
            mlflow.log_param(
                "max_class_sampling_factor",
                class_sampling_metadata["max_class_sampling_factor"],
            )

        automl_parameters = {
            "max_runtime_secs": MAX_RUNTIME_SECS,
            "seed": SEED,
            "project_name": "traffic_risk",
            "balance_classes": True,
            "max_after_balance_size": 5.0,
            "sort_metric": "mean_per_class_error",
        }
        if class_sampling_factors:
            automl_parameters["class_sampling_factors"] = class_sampling_factors

        aml = H2OAutoML(**automl_parameters)

        aml.train(
            x=feature_columns,
            y=LABEL_COLUMN,
            training_frame=train,
        )

        logger.info("Training completed.")

        # ---- Step 7: Leaderboard ----
        lb = aml.leaderboard
        lb_df = lb.head(rows=10).as_data_frame()
        logger.info("H2O AutoML leaderboard (top 10):\n%s", lb_df)

        # ---- Step 8: Evaluate and log top models ----
        top_model_ids = lb_df["model_id"].tolist()
        logger.info(
            "Step 8: Evaluating and logging top %s models...", len(top_model_ids)
        )
        logger.info("Model IDs: %s", top_model_ids)

        best_model = None
        best_macro_f1 = -1.0
        best_logloss = float("inf")
        best_metrics = {}

        for rank, model_id in enumerate(top_model_ids, start=1):
            model = h2o.get_model(model_id)
            model_perf = model.model_performance(test)
            sklearn_metrics, report_df, confusion_df = evaluate_classifier_with_sklearn(
                model,
                test,
                feature_columns,
                LABEL_COLUMN,
            )

            # Track the best model using macro F1 first because severity classes
            # are strongly imbalanced. Logloss remains a tie-breaker.
            current_logloss = float(model_perf.logloss())
            current_macro_f1 = sklearn_metrics["macro_f1"]
            if current_macro_f1 > best_macro_f1 or (
                current_macro_f1 == best_macro_f1 and current_logloss < best_logloss
            ):
                best_macro_f1 = current_macro_f1
                best_logloss = current_logloss
                best_model = model
                best_metrics = sklearn_metrics

            logger.info("-" * 60)
            logger.info("Evaluating model rank %d: %s (%s)", rank, model_id, model.algo)

            # Create a nested run for each model
            with mlflow.start_run(
                run_name=f"top{rank}_{model.algo}", nested=True
            ) as child_run:
                child_run_id = child_run.info.run_id
                logger.info("Nested MLflow run ID: %s", child_run_id)

                # Log model info as params
                mlflow.log_param("rank", rank)
                mlflow.log_param("model_id", model_id)
                mlflow.log_param("algo", model.algo)

                # --- Log all available metrics ---
                log_metric_if_exists(model_perf, "logloss")
                log_metric_if_exists(model_perf, "mean_per_class_error")
                log_metric_if_exists(model_perf, "accuracy")
                log_metric_if_exists(model_perf, "rmse")
                log_metric_if_exists(model_perf, "mse")
                log_metric_if_exists(model_perf, "r2")
                log_metric_if_exists(model_perf, "mae")
                log_metric_if_exists(model_perf, "rmsle")
                log_metric_if_exists(model_perf, "auc")
                log_metric_if_exists(model_perf, "gini")

                # Per-class metrics
                log_metric_if_exists(model_perf, "F1")
                log_metric_if_exists(model_perf, "precision")
                log_metric_if_exists(model_perf, "recall")
                log_classifier_metrics(
                    sklearn_metrics,
                    report_df,
                    confusion_df,
                    artifact_prefix=f"rank{rank}_{model.algo}",
                )

                # --- Confusion matrix ---
                try:
                    if hasattr(model_perf, "confusion_matrix"):
                        cm = model_perf.confusion_matrix()
                        logger.info("  Confusion matrix:\n%s", cm)
                except (KeyError, AttributeError):
                    logger.info("  Confusion matrix:           N/A")

                # Log model artifact for this rank
                mlflow.h2o.log_model(model, artifact_path=f"model_rank{rank}")

        # ---- Step 9: Register the best model ----
        logger.info("=" * 60)
        logger.info(
            "Registering the best model (highest macro F1, logloss tie-breaker)..."
        )
        logger.info("Best model ID: %s", best_model.model_id)
        logger.info("Best model algorithm: %s", best_model.algo)
        logger.info("Best model macro F1: %.6f", best_macro_f1)
        logger.info(
            "Best model weighted F1: %.6f", best_metrics.get("weighted_f1", 0.0)
        )
        logger.info("Best model logloss: %.6f", best_logloss)

        # Log best model as a separate artifact
        mlflow.h2o.log_model(best_model, artifact_path="best_model")

        # Register in MLflow Registry
        model_uri = f"runs:/{run_id}/best_model"
        registered_model = mlflow.register_model(model_uri, MODEL_NAME)
        logger.info(
            "Model registered: name=%s, version=%s",
            registered_model.name,
            registered_model.version,
        )

    # ---- Cleanup ----
    logger.info("Shutting down H2O cluster...")
    h2o.shutdown(prompt=False)

    logger.info("=" * 80)
    logger.info("Training complete. Model registered in MLflow.")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
