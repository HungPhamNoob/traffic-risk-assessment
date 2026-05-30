#!/usr/bin/env python3
"""
Run the Kaggle 5 classical baselines on the before-2020 H2O feature dataset.

Purpose:
    - Reuse the model families shown in `ml/notebooks/vendor/kaggle_5.ipynb`
      on the same processed dataset used by `h2o_before_2020.py`.
    - Emit a richer metric set so the comparison with H2O is more fair.
    - Save comparable CSV artifacts for summary metrics, per-class metrics,
      confusion matrices, and a direct H2O-vs-Kaggle scorecard.

Notes:
    - This script uses the H2O processed feature file for fairness, not the raw
      Kaggle notebook feature-engineering flow.
    - KNN is skipped automatically on very large splits because the original
      notebook also limited itself to a much smaller county-sized slice.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

os.environ.setdefault("OMP_NUM_THREADS", os.getenv("KAGGLE5_MAX_CPU_THREADS", "2"))
os.environ.setdefault("OPENBLAS_NUM_THREADS", os.getenv("KAGGLE5_MAX_CPU_THREADS", "2"))
os.environ.setdefault("MKL_NUM_THREADS", os.getenv("KAGGLE5_MAX_CPU_THREADS", "2"))
os.environ.setdefault("NUMEXPR_NUM_THREADS", os.getenv("KAGGLE5_MAX_CPU_THREADS", "2"))

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import SelectFromModel
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    balanced_accuracy_score,
    cohen_kappa_score,
    confusion_matrix,
    f1_score,
    log_loss,
    matthews_corrcoef,
    mean_absolute_error,
    mean_squared_error,
    precision_recall_fscore_support,
    r2_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("kaggle-5-before-2020")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_PATH = PROJECT_ROOT / "data/process/us_train_offline_before_2020.csv"
DEFAULT_H2O_LOG_PATH = PROJECT_ROOT / "data/simulation/h2o_before_2020_classsampling.log"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data/simulation/kaggle_5_before_2020"

LABEL_COLUMN = "true_severity"
SEED = int(os.getenv("KAGGLE5_SEED", "42"))
TEST_SIZE = float(os.getenv("KAGGLE5_TEST_SIZE", "0.2"))
MAX_TRAIN_ROWS = int(os.getenv("KAGGLE5_MAX_TRAIN_ROWS", "400000"))
MAX_TEST_ROWS = int(os.getenv("KAGGLE5_MAX_TEST_ROWS", "100000"))
MAX_CPU_JOBS = max(1, int(os.getenv("KAGGLE5_MAX_CPU_JOBS", "2")))
GPU_MEMORY_BUDGET_GB = float(os.getenv("KAGGLE5_GPU_MEMORY_BUDGET_GB", "4.0"))
KNN_MAX_TRAIN_ROWS = int(os.getenv("KAGGLE5_KNN_MAX_TRAIN_ROWS", "20000"))
KNN_MAX_TEST_ROWS = int(os.getenv("KAGGLE5_KNN_MAX_TEST_ROWS", "5000"))
LOGISTIC_MAX_TRAIN_ROWS = int(os.getenv("KAGGLE5_LOGISTIC_MAX_TRAIN_ROWS", "250000"))
RF_SELECTED_MAX_TRAIN_ROWS = int(
    os.getenv("KAGGLE5_RF_SELECTED_MAX_TRAIN_ROWS", "250000")
)
RF_IMPORTANCE_THRESHOLD = float(os.getenv("KAGGLE5_RF_IMPORTANCE_THRESHOLD", "0.03"))

EXCLUDED_COLUMNS = {
    "event_id",
    "event_year",
    "event_time",
    "true_severity",
    "ingestion_time_epoch",
    "processed_time_epoch",
    "end_to_end_latency_ms",
}

FEATURE_COMPARISON_TEXT = """# Kaggle 5 vs H2O Feature Engineering

## Kaggle 5 notebook (`ml/notebooks/vendor/kaggle_5.ipynb`)

- Starts from raw US Accidents columns.
- Creates `Hour`, `Weekday`, and `Time_Duration(min)`.
- Keeps many raw categorical fields such as `City`, `County`, `State`,
  `Timezone`, `Wind_Direction`, and `Weather_Condition`.
- Uses `pd.get_dummies()` to one-hot encode categorical features.
- Originally narrows the data to Pennsylvania / Montgomery County because the
  raw design becomes expensive on large data.

## H2O production feature contract (`processing/feature_engineering.py`)

- Starts from the same raw accident feed but compresses it into 20 numeric,
  production-safe features shared by Spark, Flink, and offline training.
- Encodes weather text into `weather_code`.
- Encodes street text into `road_type_code`.
- Derives `hour`, `day_of_week`, `is_weekend`, `is_rush_hour`, and `is_night`.
- Converts road context booleans into numeric flags:
  `is_junction`, `has_traffic_signal`, `is_crossing`, `is_roundabout`,
  `is_stop`, `is_station`, `is_railway`.
- Avoids end-time derived duration and high-cardinality location categories so
  the same features are available online at inference time.

## What this comparison run does

- Uses the H2O processed file `data/process/us_train_offline_before_2020.csv`
  so Kaggle-style classical models and H2O see the same input table.
- This is the fairest metric comparison for the capstone pipeline.
- It is not a byte-for-byte rerun of the original notebook feature engineering,
  because that notebook depends on raw columns that are intentionally removed by
  the production feature contract.
"""


@dataclass
class ModelRunResult:
    model_name: str
    status: str
    metrics: dict[str, Any]
    report_df: pd.DataFrame | None
    confusion_df: pd.DataFrame | None
    extra: dict[str, Any]


@dataclass
class ResourceMetadata:
    cpu_jobs: int
    cpu_thread_cap: str
    gpu_detected: bool
    gpu_name: str | None
    gpu_total_memory_mib: int | None
    gpu_used_memory_mib: int | None
    gpu_budget_gb: float
    gpu_used_for_training: bool
    train_rows_full: int
    train_rows_used: int
    test_rows_full: int
    test_rows_used: int
    sampling_applied: bool
    notes: list[str]


def _label_sort_key(value: Any) -> Any:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return str(value)


def _ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def detect_gpu_metadata() -> tuple[bool, str | None, int | None, int | None]:
    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=name,memory.total,memory.used",
                "--format=csv,noheader,nounits",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        first_line = result.stdout.strip().splitlines()[0]
        name, total_mib, used_mib = [part.strip() for part in first_line.split(",")]
        return True, name, int(total_mib), int(used_mib)
    except Exception:
        return False, None, None, None


def load_dataset(path: Path) -> tuple[pd.DataFrame, pd.Series]:
    if not path.exists():
        raise FileNotFoundError(f"Missing dataset: {path}")

    df = pd.read_csv(path)
    if LABEL_COLUMN not in df.columns:
        if "Severity" in df.columns:
            label_column = "Severity"
        else:
            raise ValueError("Label column not found: expected true_severity or Severity")
    else:
        label_column = LABEL_COLUMN

    df = df.dropna(subset=[label_column]).copy()
    feature_columns = [c for c in df.columns if c not in EXCLUDED_COLUMNS and c != label_column]
    if not feature_columns:
        raise ValueError("No feature columns found after exclusions.")

    x = df[feature_columns].copy()
    y = df[label_column].astype(str)

    non_numeric = x.select_dtypes(exclude=[np.number]).columns.tolist()
    if non_numeric:
        logger.info("One-hot encoding %d non-numeric columns: %s", len(non_numeric), non_numeric)
        x = pd.get_dummies(x, columns=non_numeric, drop_first=False)

    logger.info("Loaded %s rows with %s usable feature columns.", len(df), x.shape[1])
    return x, y


def cap_split_for_local_resources(
    x_train: pd.DataFrame,
    x_test: pd.DataFrame,
    y_train: pd.Series,
    y_test: pd.Series,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series, ResourceMetadata]:
    train_full = len(x_train)
    test_full = len(x_test)
    notes: list[str] = []
    sampling_applied = False

    if train_full > MAX_TRAIN_ROWS:
        sampling_applied = True
        notes.append(
            "Train split capped for local hardware from "
            f"{train_full:,} to {MAX_TRAIN_ROWS:,} rows."
        )
        x_train, _, y_train, _ = train_test_split(
            x_train,
            y_train,
            train_size=MAX_TRAIN_ROWS,
            random_state=SEED,
            stratify=y_train,
        )

    if test_full > MAX_TEST_ROWS:
        sampling_applied = True
        notes.append(
            "Test split capped for local hardware from "
            f"{test_full:,} to {MAX_TEST_ROWS:,} rows."
        )
        x_test, _, y_test, _ = train_test_split(
            x_test,
            y_test,
            train_size=MAX_TEST_ROWS,
            random_state=SEED,
            stratify=y_test,
        )

    gpu_detected, gpu_name, gpu_total_mib, gpu_used_mib = detect_gpu_metadata()
    if gpu_detected:
        notes.append(
            "GPU detected but not used in this run because the Kaggle 5 baseline "
            "here stays on sklearn classical models. Keeping that model family "
            "is more faithful than swapping in a GPU-only library."
        )
        if gpu_total_mib is not None and gpu_total_mib > int(GPU_MEMORY_BUDGET_GB * 1024):
            notes.append(
                f"Soft GPU budget set to {GPU_MEMORY_BUDGET_GB:.1f} GiB while card has "
                f"{gpu_total_mib / 1024:.1f} GiB total."
            )

    metadata = ResourceMetadata(
        cpu_jobs=MAX_CPU_JOBS,
        cpu_thread_cap=os.getenv("KAGGLE5_MAX_CPU_THREADS", "2"),
        gpu_detected=gpu_detected,
        gpu_name=gpu_name,
        gpu_total_memory_mib=gpu_total_mib,
        gpu_used_memory_mib=gpu_used_mib,
        gpu_budget_gb=GPU_MEMORY_BUDGET_GB,
        gpu_used_for_training=False,
        train_rows_full=train_full,
        train_rows_used=len(x_train),
        test_rows_full=test_full,
        test_rows_used=len(x_test),
        sampling_applied=sampling_applied,
        notes=notes,
    )

    return x_train, x_test, y_train, y_test, metadata


def compute_specificity_by_class(cm: np.ndarray) -> np.ndarray:
    total = cm.sum()
    specificities = []
    for idx in range(cm.shape[0]):
        tp = cm[idx, idx]
        fn = cm[idx, :].sum() - tp
        fp = cm[:, idx].sum() - tp
        tn = total - tp - fn - fp
        denominator = tn + fp
        specificities.append(float(tn / denominator) if denominator else 0.0)
    return np.asarray(specificities, dtype=float)


def compute_probabilistic_metrics(
    y_true: pd.Series,
    y_pred: pd.Series,
    labels: list[str],
    proba: np.ndarray | None,
) -> dict[str, Any]:
    metrics: dict[str, Any] = {}

    y_true_int = np.asarray([int(float(v)) for v in y_true], dtype=float)
    y_pred_int = np.asarray([int(float(v)) for v in y_pred], dtype=float)

    metrics["mse"] = float(mean_squared_error(y_true_int, y_pred_int))
    metrics["rmse"] = float(math.sqrt(metrics["mse"]))
    metrics["mae"] = float(mean_absolute_error(y_true_int, y_pred_int))
    metrics["r2"] = float(r2_score(y_true_int, y_pred_int))
    metrics["rmsle"] = float(
        math.sqrt(mean_squared_error(np.log1p(y_true_int), np.log1p(y_pred_int)))
    )

    if proba is None:
        metrics["logloss"] = None
        metrics["auc_macro_ovr"] = None
        metrics["auc_weighted_ovr"] = None
        metrics["gini_macro_ovr"] = None
        metrics["gini_weighted_ovr"] = None
        return metrics

    metrics["logloss"] = float(log_loss(y_true, proba, labels=labels))
    if len(labels) > 2:
        try:
            auc_macro = roc_auc_score(
                y_true,
                proba,
                labels=labels,
                multi_class="ovr",
                average="macro",
            )
            auc_weighted = roc_auc_score(
                y_true,
                proba,
                labels=labels,
                multi_class="ovr",
                average="weighted",
            )
            metrics["auc_macro_ovr"] = float(auc_macro)
            metrics["auc_weighted_ovr"] = float(auc_weighted)
            metrics["gini_macro_ovr"] = float(2 * auc_macro - 1)
            metrics["gini_weighted_ovr"] = float(2 * auc_weighted - 1)
        except ValueError:
            metrics["auc_macro_ovr"] = None
            metrics["auc_weighted_ovr"] = None
            metrics["gini_macro_ovr"] = None
            metrics["gini_weighted_ovr"] = None
    else:
        metrics["auc_macro_ovr"] = None
        metrics["auc_weighted_ovr"] = None
        metrics["gini_macro_ovr"] = None
        metrics["gini_weighted_ovr"] = None

    return metrics


def evaluate_predictions(
    model_name: str,
    y_true: pd.Series,
    y_pred: pd.Series,
    labels: list[str],
    proba: np.ndarray | None,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    precision, recall, f1, support = precision_recall_fscore_support(
        y_true,
        y_pred,
        labels=labels,
        zero_division=0,
    )
    macro = precision_recall_fscore_support(
        y_true,
        y_pred,
        labels=labels,
        average="macro",
        zero_division=0,
    )
    weighted = precision_recall_fscore_support(
        y_true,
        y_pred,
        labels=labels,
        average="weighted",
        zero_division=0,
    )

    cm = confusion_matrix(y_true, y_pred, labels=labels)
    row_sums = cm.sum(axis=1)
    per_class_acc = np.divide(
        np.diag(cm),
        row_sums,
        out=np.zeros_like(row_sums, dtype=float),
        where=row_sums != 0,
    )
    specificity = compute_specificity_by_class(cm)

    metrics: dict[str, Any] = {
        "accuracy": float(accuracy_score(y_true, y_pred)),
        "balanced_accuracy": float(balanced_accuracy_score(y_true, y_pred)),
        "cohen_kappa": float(cohen_kappa_score(y_true, y_pred)),
        "matthews_corrcoef": float(matthews_corrcoef(y_true, y_pred)),
        "macro_precision": float(macro[0]),
        "macro_recall": float(macro[1]),
        "macro_f1": float(
            f1_score(y_true, y_pred, labels=labels, average="macro", zero_division=0)
        ),
        "weighted_precision": float(weighted[0]),
        "weighted_recall": float(weighted[1]),
        "weighted_f1": float(
            f1_score(y_true, y_pred, labels=labels, average="weighted", zero_division=0)
        ),
        "macro_specificity": float(np.mean(specificity)),
        "weighted_specificity": float(np.average(specificity, weights=support)),
        "mean_per_class_error": float(1.0 - np.mean(per_class_acc)),
    }
    metrics.update(compute_probabilistic_metrics(y_true, y_pred, labels, proba))

    report_df = pd.DataFrame(
        {
            "class_label": labels,
            "precision": precision,
            "recall": recall,
            "sensitivity": recall,
            "specificity": specificity,
            "f1": f1,
            "support": support,
        }
    )
    confusion_df = pd.DataFrame(
        cm,
        index=[f"actual_{label}" for label in labels],
        columns=[f"predicted_{label}" for label in labels],
    )

    logger.info(
        "[%s] accuracy=%.6f macro_f1=%.6f weighted_f1=%.6f logloss=%s",
        model_name,
        metrics["accuracy"],
        metrics["macro_f1"],
        metrics["weighted_f1"],
        "N/A" if metrics["logloss"] is None else f"{metrics['logloss']:.6f}",
    )
    return metrics, report_df, confusion_df


def align_probabilities(model: Any, x_eval: pd.DataFrame, labels: list[str]) -> np.ndarray | None:
    if not hasattr(model, "predict_proba"):
        return None
    try:
        proba = model.predict_proba(x_eval)
        class_order = [str(c) for c in model.classes_]
        index_map = [class_order.index(label) for label in labels]
        return proba[:, index_map]
    except Exception:
        logger.exception("Probability extraction failed for %s", model.__class__.__name__)
        return None


def fit_and_evaluate_model(
    model_name: str,
    model: Any,
    x_train: pd.DataFrame,
    x_test: pd.DataFrame,
    y_train: pd.Series,
    y_test: pd.Series,
) -> ModelRunResult:
    try:
        model.fit(x_train, y_train)
        y_pred = pd.Series(model.predict(x_test), index=y_test.index, dtype=str)
        labels = sorted(set(y_test) | set(y_pred), key=_label_sort_key)
        proba = align_probabilities(model, x_test, labels)
        metrics, report_df, confusion_df = evaluate_predictions(
            model_name=model_name,
            y_true=y_test,
            y_pred=y_pred,
            labels=labels,
            proba=proba,
        )
        return ModelRunResult(
            model_name=model_name,
            status="ok",
            metrics=metrics,
            report_df=report_df,
            confusion_df=confusion_df,
            extra={},
        )
    except Exception as exc:
        logger.exception("Model %s failed", model_name)
        return ModelRunResult(
            model_name=model_name,
            status="failed",
            metrics={"error": str(exc)},
            report_df=None,
            confusion_df=None,
            extra={},
        )


def maybe_run_knn(
    x_train: pd.DataFrame,
    x_test: pd.DataFrame,
    y_train: pd.Series,
    y_test: pd.Series,
) -> ModelRunResult:
    if len(x_train) > KNN_MAX_TRAIN_ROWS or len(x_test) > KNN_MAX_TEST_ROWS:
        reason = (
            "Skipped on full before-2020 split because KNN scales poorly at "
            f"{len(x_train):,} train rows and {len(x_test):,} test rows. "
            f"Thresholds: train<={KNN_MAX_TRAIN_ROWS:,}, test<={KNN_MAX_TEST_ROWS:,}."
        )
        logger.warning(reason)
        return ModelRunResult(
            model_name="KNN (k=6)",
            status="skipped",
            metrics={"skip_reason": reason},
            report_df=None,
            confusion_df=None,
            extra={},
        )

    knn = KNeighborsClassifier(n_neighbors=6)
    return fit_and_evaluate_model("KNN (k=6)", knn, x_train, x_test, y_train, y_test)


def maybe_run_logistic_regression(
    x_train: pd.DataFrame,
    x_test: pd.DataFrame,
    y_train: pd.Series,
    y_test: pd.Series,
) -> ModelRunResult:
    if len(x_train) > LOGISTIC_MAX_TRAIN_ROWS:
        reason = (
            "Skipped on full before-2020 split because Logistic Regression on "
            f"{len(x_train):,} rows is too slow for this local baseline pass. "
            f"Threshold: train<={LOGISTIC_MAX_TRAIN_ROWS:,}."
        )
        logger.warning(reason)
        return ModelRunResult(
            model_name="Logistic Regression",
            status="skipped",
            metrics={"skip_reason": reason},
            report_df=None,
            confusion_df=None,
            extra={},
        )

    lr = LogisticRegression(
        random_state=SEED,
        max_iter=1000,
        solver="lbfgs",
        n_jobs=None,
    )
    return fit_and_evaluate_model("Logistic Regression", lr, x_train, x_test, y_train, y_test)


def run_random_forest_selected_features(
    x_train: pd.DataFrame,
    x_test: pd.DataFrame,
    y_train: pd.Series,
    y_test: pd.Series,
) -> ModelRunResult:
    if len(x_train) > RF_SELECTED_MAX_TRAIN_ROWS:
        reason = (
            "Skipped selected-feature Random Forest on full before-2020 split "
            f"because the selector pass would retrain another forest on "
            f"{len(x_train):,} rows. Threshold: train<={RF_SELECTED_MAX_TRAIN_ROWS:,}."
        )
        logger.warning(reason)
        return ModelRunResult(
            model_name="Random Forest (selected features)",
            status="skipped",
            metrics={"skip_reason": reason},
            report_df=None,
            confusion_df=None,
            extra={},
        )

    try:
        selector_model = RandomForestClassifier(
            n_estimators=100,
            random_state=SEED,
            n_jobs=MAX_CPU_JOBS,
        )
        selector_model.fit(x_train, y_train)
        selector = SelectFromModel(selector_model, threshold=RF_IMPORTANCE_THRESHOLD, prefit=True)
        support_idx = selector.get_support(indices=True)
        if len(support_idx) == 0:
            reason = (
                "No feature passed the importance threshold "
                f"{RF_IMPORTANCE_THRESHOLD:.4f} for selected-feature RF."
            )
            logger.warning(reason)
            return ModelRunResult(
                model_name="Random Forest (selected features)",
                status="skipped",
                metrics={"skip_reason": reason},
                report_df=None,
                confusion_df=None,
                extra={},
            )

        selected_features = x_train.columns[support_idx].tolist()
        logger.info(
            "Random Forest selected %d features above threshold %.4f: %s",
            len(selected_features),
            RF_IMPORTANCE_THRESHOLD,
            selected_features,
        )
        x_train_sel = selector.transform(x_train)
        x_test_sel = selector.transform(x_test)
        rf_selected = RandomForestClassifier(
            n_estimators=100,
            random_state=SEED,
            n_jobs=MAX_CPU_JOBS,
        )
        result = fit_and_evaluate_model(
            "Random Forest (selected features)",
            rf_selected,
            x_train_sel,
            x_test_sel,
            y_train,
            y_test,
        )
        result.extra["selected_features"] = selected_features
        return result
    except Exception as exc:
        logger.exception("Selected-feature random forest failed")
        return ModelRunResult(
            model_name="Random Forest (selected features)",
            status="failed",
            metrics={"error": str(exc)},
            report_df=None,
            confusion_df=None,
            extra={},
        )


def parse_h2o_reference(log_path: Path) -> tuple[dict[str, Any], pd.DataFrame | None]:
    if not log_path.exists():
        logger.warning("H2O reference log not found: %s", log_path)
        return {}, None

    lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()

    best_model_id = None
    for line in lines:
        match = re.search(r"Best model ID:\s+(.+)$", line)
        if match:
            best_model_id = match.group(1).strip()
            break

    if not best_model_id:
        logger.warning("Could not find best H2O model ID in %s", log_path)
        return {}, None

    start_idx = None
    for idx, line in enumerate(lines):
        if f"Evaluating model rank" in line and best_model_id in line:
            start_idx = idx
            break

    if start_idx is None:
        logger.warning("Could not find evaluation block for best H2O model %s", best_model_id)
        return {}, None

    end_idx = len(lines)
    for idx in range(start_idx + 1, len(lines)):
        if "Evaluating model rank" in lines[idx]:
            end_idx = idx
            break

    block = lines[start_idx:end_idx]
    metrics: dict[str, Any] = {"model": f"H2O {best_model_id}"}
    scalar_patterns = {
        "logloss": r"Logloss:\s+([0-9.]+|N/A)",
        "mean_per_class_error": r"Mean_per_class_error:\s+([0-9.]+|N/A)",
        "accuracy": r"accuracy:\s+([0-9.]+|N/A)",
        "macro_precision": r"macro_precision:\s+([0-9.]+|N/A)",
        "macro_recall": r"macro_recall:\s+([0-9.]+|N/A)",
        "macro_f1": r"macro_f1:\s+([0-9.]+|N/A)",
        "weighted_precision": r"weighted_precision:\s+([0-9.]+|N/A)",
        "weighted_recall": r"weighted_recall:\s+([0-9.]+|N/A)",
        "weighted_f1": r"weighted_f1:\s+([0-9.]+|N/A)",
        "rmse": r"Rmse:\s+([0-9.]+|N/A)",
        "mse": r"Mse:\s+([0-9.]+|N/A)",
        "r2": r"R2:\s+([0-9.]+|N/A)",
    }
    joined_block = "\n".join(block)
    for key, pattern in scalar_patterns.items():
        match = re.search(pattern, joined_block)
        if not match:
            continue
        value = match.group(1)
        metrics[key] = None if value == "N/A" else float(value)

    report_rows: list[dict[str, Any]] = []
    in_report = False
    for line in block:
        if "Per-class classification report:" in line:
            in_report = True
            continue
        if in_report and "Confusion matrix table:" in line:
            break
        if not in_report:
            continue
        match = re.search(
            r"^\s*\d+\s+(\S+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+(\d+)\s*$",
            line,
        )
        if match:
            report_rows.append(
                {
                    "class_label": match.group(1),
                    "precision": float(match.group(2)),
                    "recall": float(match.group(3)),
                    "f1": float(match.group(4)),
                    "support": int(match.group(5)),
                }
            )

    report_df = pd.DataFrame(report_rows) if report_rows else None
    return metrics, report_df


def build_summary_dataframe(results: list[ModelRunResult]) -> pd.DataFrame:
    rows = []
    for result in results:
        row = {"model": result.model_name, "status": result.status}
        row.update(result.metrics)
        if result.extra.get("selected_features"):
            row["selected_feature_count"] = len(result.extra["selected_features"])
        rows.append(row)
    summary_df = pd.DataFrame(rows)
    if not summary_df.empty and "status" in summary_df.columns:
        summary_df["status"] = pd.Categorical(
            summary_df["status"],
            categories=["ok", "skipped", "failed"],
            ordered=True,
        )
    if "macro_f1" in summary_df.columns:
        summary_df = summary_df.sort_values(
            by=["status", "macro_f1"],
            ascending=[True, False],
            na_position="last",
        )
    return summary_df


def save_results(
    output_dir: Path,
    summary_df: pd.DataFrame,
    results: list[ModelRunResult],
    h2o_metrics: dict[str, Any],
    h2o_report_df: pd.DataFrame | None,
    resource_metadata: ResourceMetadata,
) -> None:
    _ensure_output_dir(output_dir)

    summary_path = output_dir / "summary_metrics.csv"
    summary_df.to_csv(summary_path, index=False)
    logger.info("Saved summary metrics to %s", summary_path)

    resource_path = output_dir / "resource_metadata.json"
    resource_path.write_text(
        json.dumps(resource_metadata.__dict__, indent=2),
        encoding="utf-8",
    )
    logger.info("Saved resource metadata to %s", resource_path)

    for result in results:
        slug = (
            result.model_name.lower()
            .replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .replace("/", "_")
        )
        if result.report_df is not None:
            report_path = output_dir / f"{slug}_classification_report.csv"
            result.report_df.to_csv(report_path, index=False)
        if result.confusion_df is not None:
            confusion_path = output_dir / f"{slug}_confusion_matrix.csv"
            result.confusion_df.to_csv(confusion_path)
        if result.extra.get("selected_features"):
            features_path = output_dir / f"{slug}_selected_features.json"
            features_path.write_text(
                json.dumps(result.extra["selected_features"], indent=2),
                encoding="utf-8",
            )

    if h2o_metrics:
        best_kaggle = summary_df.loc[
            (summary_df["status"] == "ok") & summary_df["macro_f1"].notna()
        ].head(1)
        if not best_kaggle.empty:
            compare_rows = [
                {"model": best_kaggle.iloc[0]["model"], **best_kaggle.iloc[0].to_dict()},
                {"model": "H2O best reference", **h2o_metrics},
            ]
            comparison_df = pd.DataFrame(compare_rows)
            comparison_path = output_dir / "best_model_comparison.csv"
            comparison_df.to_csv(comparison_path, index=False)
            logger.info("Saved H2O comparison to %s", comparison_path)

    if h2o_report_df is not None and not h2o_report_df.empty:
        h2o_report_path = output_dir / "h2o_best_classification_report.csv"
        h2o_report_df.to_csv(h2o_report_path, index=False)

    feature_doc_path = output_dir / "feature_engineering_comparison.md"
    feature_doc_path.write_text(
        FEATURE_COMPARISON_TEXT
        + "\n\n## Local resource policy for this run\n\n"
        + f"- CPU jobs capped at `{resource_metadata.cpu_jobs}`.\n"
        + f"- CPU thread env cap set to `{resource_metadata.cpu_thread_cap}`.\n"
        + f"- Train rows used: `{resource_metadata.train_rows_used:,}`"
        + f" / full `{resource_metadata.train_rows_full:,}`.\n"
        + f"- Test rows used: `{resource_metadata.test_rows_used:,}`"
        + f" / full `{resource_metadata.test_rows_full:,}`.\n"
        + f"- GPU detected: `{resource_metadata.gpu_detected}`.\n"
        + f"- GPU training used: `{resource_metadata.gpu_used_for_training}`.\n"
        + f"- Soft GPU memory budget: `{resource_metadata.gpu_budget_gb:.1f} GiB`.\n"
        + "".join(f"- {note}\n" for note in resource_metadata.notes),
        encoding="utf-8",
    )


def main() -> None:
    data_path = Path(os.getenv("KAGGLE5_DATA_PATH", str(DEFAULT_DATA_PATH)))
    h2o_log_path = Path(os.getenv("KAGGLE5_H2O_LOG_PATH", str(DEFAULT_H2O_LOG_PATH)))
    output_dir = Path(os.getenv("KAGGLE5_OUTPUT_DIR", str(DEFAULT_OUTPUT_DIR)))

    x, y = load_dataset(data_path)
    x_train, x_test, y_train, y_test = train_test_split(
        x,
        y,
        test_size=TEST_SIZE,
        random_state=SEED,
        shuffle=True,
    )
    logger.info(
        "Train/test split complete: train=%s rows, test=%s rows",
        f"{len(x_train):,}",
        f"{len(x_test):,}",
    )
    x_train, x_test, y_train, y_test, resource_metadata = cap_split_for_local_resources(
        x_train,
        x_test,
        y_train,
        y_test,
    )
    logger.info(
        "Resource-aware split in use: train=%s rows, test=%s rows, cpu_jobs=%s",
        f"{len(x_train):,}",
        f"{len(x_test):,}",
        MAX_CPU_JOBS,
    )
    for note in resource_metadata.notes:
        logger.info("Resource note: %s", note)

    results: list[ModelRunResult] = []

    dt_entropy = DecisionTreeClassifier(
        max_depth=8,
        criterion="entropy",
        random_state=SEED,
    )
    results.append(
        fit_and_evaluate_model(
            "Decision Tree (entropy)",
            dt_entropy,
            x_train,
            x_test,
            y_train,
            y_test,
        )
    )

    dt_gini = DecisionTreeClassifier(
        max_depth=8,
        criterion="gini",
        random_state=SEED,
    )
    results.append(
        fit_and_evaluate_model(
            "Decision Tree (gini)",
            dt_gini,
            x_train,
            x_test,
            y_train,
            y_test,
        )
    )

    rf = RandomForestClassifier(
        n_estimators=100,
        random_state=SEED,
        n_jobs=MAX_CPU_JOBS,
    )
    results.append(fit_and_evaluate_model("Random Forest", rf, x_train, x_test, y_train, y_test))

    results.append(maybe_run_logistic_regression(x_train, x_test, y_train, y_test))
    results.append(maybe_run_knn(x_train, x_test, y_train, y_test))
    results.append(run_random_forest_selected_features(x_train, x_test, y_train, y_test))

    summary_df = build_summary_dataframe(results)
    h2o_metrics, h2o_report_df = parse_h2o_reference(h2o_log_path)
    save_results(
        output_dir,
        summary_df,
        results,
        h2o_metrics,
        h2o_report_df,
        resource_metadata,
    )

    logger.info("Top result rows:\n%s", summary_df.head(10).to_string(index=False))
    if h2o_metrics:
        logger.info("H2O best reference metrics: %s", h2o_metrics)


if __name__ == "__main__":
    main()
