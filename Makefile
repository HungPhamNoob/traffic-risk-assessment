SHELL := /bin/bash

PROJECT_ID ?= big-data-group-4
ZONE ?= us-central1-a
ENV_FILE ?= .env
CLOUD_ENV_FILE ?= .env.cloud
NODE1 ?= node1-control
NODE2 ?= node2-streaming
NODE3 ?= node3-batch

.PHONY: help setup format lint test compile validate clean-local \
	local-up local-down local-logs local-pipeline local-reset \
	cloud-node1 cloud-node2 cloud-node3 cloud-reset cloud-start-node23 \
	ssh-node1 ssh-node2 ssh-node3 gcp-list push-branch

help:
	@echo "Traffic Risk Assessment - available commands"
	@echo "  make setup              Prepare local folders and copy .env.example if needed"
	@echo "  make validate           Run formatting, linting, tests, compile checks, and compose validation"
	@echo "  make local-up           Start local core services from docker-compose.yaml"
	@echo "  make local-pipeline     Run bounded local pipeline and write API JSON outputs"
	@echo "  make local-reset        Remove local simulation outputs and Docker volumes"
	@echo "  make cloud-node1        Start Node 1 services through IAP SSH"
	@echo "  make cloud-node2        Start Node 2 services through IAP SSH"
	@echo "  make cloud-node3        Start Node 3 services through IAP SSH"
	@echo "  make cloud-start-node23 Restart Node 2 and Node 3 together through IAP SSH"
	@echo "  make ssh-node1          Open IAP SSH to Node 1"
	@echo "  make ssh-node2          Open IAP SSH to Node 2"
	@echo "  make ssh-node3          Open IAP SSH to Node 3"

setup:
	@mkdir -p data/raw data/split data/process data/simulation orchestration/logs ml/training
	@test -f .env || cp .env.example .env
	@echo "Local project folders are ready."

format:
	.venv/bin/python -m black . --exclude='(\.git|\.venv|venv|vendor|data|ml/notebooks)'

lint:
	.venv/bin/python -m black --check . --exclude='(\.git|\.venv|venv|vendor|data|ml/notebooks)'
	.venv/bin/python -m flake8 . --exclude=venv,.venv,vendor,data,ml/notebooks --max-line-length=120 --extend-ignore=E203,W503,W605

test:
	.venv/bin/python -m pytest tests/ -q

compile:
	python3 -m py_compile \
		processing/feature_engineering.py \
		processing/flink_streaming.py \
		processing/spark_batch.py \
		ingestion/kafka/us_producer.py \
		ml/training/train_before_2020.py \
		ml/training/train_after_2020.py \
		data/split_data.py
	python3 -m py_compile dashboard/backend/app/app.py

compose-check:
	docker compose --env-file $(ENV_FILE) -f docker-compose.yaml config --quiet
	docker compose --env-file $(CLOUD_ENV_FILE) -f deployment/node1-control/docker-compose.yaml config --quiet
	docker compose --env-file $(CLOUD_ENV_FILE) -f deployment/node2-streaming/docker-compose.yaml config --quiet
	docker compose --env-file $(CLOUD_ENV_FILE) -f deployment/node3-batch/docker-compose.yaml config --quiet

validate: lint test compile compose-check
	@echo "Validation completed successfully."

local-up:
	docker compose --env-file $(ENV_FILE) -f docker-compose.yaml up -d postgres redis kafka-topic-init mlflow fastapi

local-down:
	docker compose --env-file $(ENV_FILE) -f docker-compose.yaml down --remove-orphans

local-logs:
	docker compose --env-file $(ENV_FILE) -f docker-compose.yaml logs -f --tail=200

local-pipeline:
	bash scripts/local/run_pipeline.sh

local-reset:
	docker compose --env-file $(ENV_FILE) -f docker-compose.yaml down --volumes --remove-orphans
	rm -rf data/simulation
	mkdir -p data/simulation
	@echo "Local simulation state has been reset."

clean-local:
	rm -rf .pytest_cache **/__pycache__ data/simulation
	@echo "Local generated files have been removed."

ssh-node1:
	gcloud compute ssh $(NODE1) --project=$(PROJECT_ID) --zone=$(ZONE) --tunnel-through-iap

ssh-node2:
	gcloud compute ssh $(NODE2) --project=$(PROJECT_ID) --zone=$(ZONE) --tunnel-through-iap

ssh-node3:
	gcloud compute ssh $(NODE3) --project=$(PROJECT_ID) --zone=$(ZONE) --tunnel-through-iap

cloud-node1:
	gcloud compute ssh $(NODE1) --project=$(PROJECT_ID) --zone=$(ZONE) --tunnel-through-iap --quiet \
		--command='cd /opt/traffic && bash scripts/gcp/run-node1.sh'

cloud-node2:
	gcloud compute ssh $(NODE2) --project=$(PROJECT_ID) --zone=$(ZONE) --tunnel-through-iap --quiet \
		--command='cd /opt/traffic && bash scripts/gcp/run-node2.sh'

cloud-node3:
	gcloud compute ssh $(NODE3) --project=$(PROJECT_ID) --zone=$(ZONE) --tunnel-through-iap --quiet \
		--command='cd /opt/traffic && bash scripts/gcp/run-node3.sh'

cloud-reset:
	gcloud compute ssh $(NODE1) --project=$(PROJECT_ID) --zone=$(ZONE) --tunnel-through-iap --quiet \
		--command='cd /opt/traffic && bash scripts/gcp/reset-realtime.sh'

cloud-start-node23:
	gcloud compute ssh $(NODE1) --project=$(PROJECT_ID) --zone=$(ZONE) --tunnel-through-iap --quiet \
		--command='cd /opt/traffic && bash scripts/gcp/start-node23-synced.sh'

gcp-list:
	gcloud compute instances list --project=$(PROJECT_ID) \
		--filter='name~node' \
		--format='table(name,zone,status,networkInterfaces[0].networkIP)'

push-branch:
	git push -u origin hung1
