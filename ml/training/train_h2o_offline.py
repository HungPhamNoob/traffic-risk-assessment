#!/usr/bin/env python3
"""
H2O AutoML Training – US Traffic Accident Risk Prediction

Purpose:
    Load offline feature CSV, train H2O AutoML classification model,
    log comprehensive metrics and model artifact to MLflow,
    and register the best model in MLflow Registry.

Input:
    data/process/us_train_offline_before_2020.csv

Output:
    MLflow experiment run with metrics, model artifact, and registry entry.

Example command:
    python ml/training/train_h2o_offline.py
"""

import logging
import os
import tempfile
import h2o
from h2o.automl import H2OAutoML
import mlflow
import mlflow.h2o

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
H2O_NTHREADS = 2

# Label column in the CSV
LABEL_COLUMN = "true_severity"

# Columns to exclude from feature vector (metadata, identifiers, label)
EXCLUDED_COLUMNS = {
    "event_id",  # unique identifier, not a feature
    "event_year",  # temporal split key, not a feature
    "event_time",  # timestamp metadata; derived time fields are used instead
    "true_severity",  # the label we want to predict
}

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
        return data_path

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

    return local_path


# ============================================================
# Main training function
# ============================================================


def main():
    logger.info("=" * 80)
    logger.info("H2O AutoML Training – Traffic Risk Assessment")
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

        aml = H2OAutoML(
            max_runtime_secs=MAX_RUNTIME_SECS,
            seed=SEED,
            project_name="traffic_risk",
            balance_classes=True,
            class_sampling_factors=[1.0, 1.5, 2.5, 4.0],
            max_after_balance_size=5.0,
        )

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
        best_logloss = float("inf")

        for rank, model_id in enumerate(top_model_ids, start=1):
            model = h2o.get_model(model_id)
            model_perf = model.model_performance(test)

            # Track the best model (lowest logloss)
            current_logloss = float(model_perf.logloss())
            if current_logloss < best_logloss:
                best_logloss = current_logloss
                best_model = model

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
        logger.info("Registering the best model (lowest logloss)...")
        logger.info("Best model ID: %s", best_model.model_id)
        logger.info("Best model algorithm: %s", best_model.algo)
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
