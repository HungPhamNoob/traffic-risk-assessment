# GCP Operations Guide

This guide explains how the shell scripts under `scripts/gcp/` are intended to be used for the three-node cloud deployment.

The deployment model is:

- `node1-control`: PostgreSQL/PostGIS, Airflow, MLflow, FastAPI, Prometheus, Grafana.
- `node2-streaming`: Kafka, Flink, Redis, replay producer.
- `node3-batch`: Spark batch processing and online retraining inputs.

The most important operating rule is:

`node2-streaming` and `node3-batch` must be treated as one synchronized pair when replay state, checkpoints, or recovery are involved.

## 1. Script Inventory

### One-time bootstrap scripts

- `setup_gcp.sh`
  Creates the GCP service account, buckets, VMs, and Linux startup-script bindings.
- `setup_gcp.ps1`
  Windows PowerShell variant for creating the initial environment.
- `startup-node1.sh`
  VM startup script automatically executed by Compute Engine for `node1-control`.
- `startup-node2.sh`
  VM startup script automatically executed by Compute Engine for `node2-streaming`.
- `startup-node3.sh`
  VM startup script automatically executed by Compute Engine for `node3-batch`.
- `upload_data_to_gcs.sh`
  One-time helper that uploads the offline pretraining CSV and replay CSV to GCS Bronze.

### Regular runtime scripts

- `run-node1.sh`
  Starts Node 1 services and runs the offline bootstrap model flow when needed.
- `run-node2.sh`
  Starts the streaming stack on Node 2.
- `run-node3.sh`
  Starts the batch stack on Node 3, synchronizes Silver/Gold data, and runs online retraining once.

### Synchronized pair lifecycle scripts

- `node23-lifecycle.sh`
  The primary controller for the Node 2 and Node 3 pair.
  Supports `start`, `stop`, `restart`, and `reset`.
- `start-node23-synced.sh`
  Thin wrapper around `node23-lifecycle.sh start`.
- `stop-node23-synced.sh`
  Thin wrapper around `node23-lifecycle.sh stop`.
- `reset-realtime.sh`
  Deletes replay-generated state, including Flink checkpoints, Spark checkpoints, Silver replay outputs, and Gold retrain outputs.

### Maintenance scripts

- `manage-nodes.sh`
  Operator-facing helper to start, stop, and inspect GCP VMs.
  For Node 2 and Node 3 it intentionally acts on the synchronized pair.
- `backup-checkpoints.sh`
  Creates operational backups for PostgreSQL dumps, Airflow DAGs, and checkpoint metadata.

## 2. Which Scripts Matter Most

If you only remember four entrypoints, remember these:

1. `scripts/gcp/setup_gcp.sh`
   Use only for fresh infrastructure bootstrap.
2. `scripts/gcp/run-node1.sh`
   Use to bring up the control plane.
3. `scripts/gcp/node23-lifecycle.sh`
   Use for all replay-pair lifecycle actions.
4. `scripts/gcp/manage-nodes.sh`
   Use for quick operator actions such as status, start, and stop.

## 3. Recommended Execution Order

### Workflow A: First cloud setup

Run this from the repository root on the operator machine:

```bash
bash scripts/gcp/setup_gcp.sh
bash scripts/gcp/upload_data_to_gcs.sh
```

Wait until the VM startup scripts finish installing Docker and host dependencies.

Then bring up the platform in this order:

```bash
bash scripts/gcp/run-node1.sh
bash scripts/gcp/node23-lifecycle.sh start
```

Why this order is required:

- Node 1 provides PostgreSQL, MLflow, FastAPI, and Airflow.
- Node 2 writes predictions to Node 1 services.
- Node 3 depends on Gold/Silver paths and MLflow tracking that conceptually belong to the control plane.

### Workflow B: Normal daily restart

For a clean operator restart of the replay system:

```bash
bash scripts/gcp/run-node1.sh
bash scripts/gcp/node23-lifecycle.sh restart
```

Do not restart Node 2 alone after replay has started.
Do not restart Node 3 alone after replay has started.

Restarting only one branch can create drift between:

- Kafka replay progress
- Flink checkpoint lineage
- Spark checkpoint lineage
- Silver replay features
- Gold retraining inputs

### Workflow C: Replay reset from the beginning

Use this only when you intentionally want to delete replay progress and restart from the beginning:

```bash
bash scripts/gcp/node23-lifecycle.sh reset
bash scripts/gcp/node23-lifecycle.sh restart
```

What `reset` removes:

- Flink checkpoints
- Spark checkpoints
- Silver replay outputs
- Gold retrain outputs

This reset is intentionally destructive to replay state.

### Workflow D: VM-level operations

For VM administration:

```bash
bash scripts/gcp/manage-nodes.sh status all
bash scripts/gcp/manage-nodes.sh start node1-control
bash scripts/gcp/manage-nodes.sh start pair
bash scripts/gcp/manage-nodes.sh stop pair
```

`pair` means the synchronized Node 2 and Node 3 branch.

## 4. What Each Runtime Script Actually Does

### `run-node1.sh`

Responsibilities:

- Loads `.env.cloud`
- Ensures Java and `python3-venv` exist for H2O bootstrap
- Starts Node 1 Docker Compose services
- Waits for MLflow tracking
- Checks whether a registered model already exists
- Runs offline pre-2020 model bootstrap only if needed
- Restarts MLflow serving and FastAPI after bootstrap

Use this script when:

- Bringing up the control plane on a fresh VM
- Recovering Node 1 services
- Re-deploying the backend and control-plane services

### `run-node2.sh`

Responsibilities:

- Loads `.env.cloud`
- Starts Kafka, topic init, Flink JobManager, Flink TaskManager, Redis, and the Python streaming job
- Preserves checkpoints if they already exist

Use this script when:

- Starting the replay streaming branch
- Recovering the streaming branch

### `run-node3.sh`

Responsibilities:

- Loads `.env.cloud`
- Starts Spark services
- Synchronizes Silver features from GCS to local disk
- Rebuilds local Gold output directories
- Runs the Spark Silver-to-Gold job
- Synchronizes Gold outputs back to GCS
- Creates a dedicated Python virtual environment for retraining
- Runs `ml/training/h2o_after_2020.py`

Use this script when:

- Starting the batch branch
- Rebuilding the latest retraining dataset
- Running one online retraining cycle

## 5. The Correct Recovery Model

The project now follows this recovery policy:

- If Node 2 fails, recover Node 2 and Node 3 together.
- If Node 3 fails, recover Node 2 and Node 3 together.
- If replay checkpoints are deleted, delete both Flink and Spark checkpoints together.
- If replay is restarted from scratch, restart both Node 2 and Node 3 together.

This policy is implemented through:

- `scripts/gcp/node23-lifecycle.sh`
- `orchestration/dags/dag_stream_replay_monitor.py`
- `orchestration/dags/dag_ml_pipeline.py`

## 6. Airflow Interaction

Two DAGs matter for operations:

### `orchestration/dags/dag_ml_pipeline.py`

Purpose:

- Runs the hourly batch branch on Node 3
- If the batch branch fails, it now calls the synchronized pair recovery entrypoint

Operational implication:

- Batch failure is no longer treated as an isolated Node 3 problem
- Airflow now assumes replay consistency is more important than partial uptime

### `orchestration/dags/dag_stream_replay_monitor.py`

Purpose:

- Checks Kafka, Flink, active Flink jobs, and Spark availability
- If any required health check fails, it now restarts the synchronized pair

Operational implication:

- Streaming health failures now trigger a real recovery path
- The health DAG is no longer a warning-only report for the main checks

## 7. When Not To Use A Script

Do not use these patterns during normal replay operations:

- Do not run `run-node2.sh` alone after a replay-state issue.
- Do not run `run-node3.sh` alone after a replay-state issue.
- Do not manually delete only Flink checkpoints.
- Do not manually delete only Spark checkpoints.

If the issue affects replay continuity, use:

```bash
bash scripts/gcp/node23-lifecycle.sh restart
```

If the issue affects replay state integrity, use:

```bash
bash scripts/gcp/node23-lifecycle.sh reset
bash scripts/gcp/node23-lifecycle.sh restart
```

## 8. Minimal Runbook

### Fresh demo day bring-up

```bash
bash scripts/gcp/run-node1.sh
bash scripts/gcp/node23-lifecycle.sh start
```

### Check overall status

```bash
bash scripts/gcp/manage-nodes.sh status all
```

### Restart the replay pair

```bash
bash scripts/gcp/node23-lifecycle.sh restart
```

### Reset replay and start again

```bash
bash scripts/gcp/node23-lifecycle.sh reset
bash scripts/gcp/node23-lifecycle.sh restart
```

### Back up critical operational state

```bash
bash scripts/gcp/backup-checkpoints.sh
```

## 9. Should Any Script Be Deleted

At the current stage, no script is obviously dead.

The files fall into four valid groups:

- initial bootstrap
- runtime startup
- synchronized recovery
- maintenance

The only scripts that are intentionally thin wrappers are:

- `start-node23-synced.sh`
- `stop-node23-synced.sh`

They are still worth keeping because:

- existing Makefile targets and operator habits can continue to work
- they provide stable entrypoints even if the lifecycle implementation changes again

If you want stricter cleanup later, the best candidate for relocation is not deletion but documentation-driven demotion:

- move `setup_gcp.ps1` into a Windows-specific onboarding section if the team is fully Linux-based

For now, keep it.
