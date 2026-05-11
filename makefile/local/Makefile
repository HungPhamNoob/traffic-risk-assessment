SHELL := /bin/bash

PROJECT_ROOT ?= $(shell git rev-parse --show-toplevel 2>/dev/null || pwd)
ENV_FILE ?= $(PROJECT_ROOT)/.env
COMPOSE_FILE ?= $(PROJECT_ROOT)/docker-compose.yaml
COMPOSE := docker compose --env-file "$(ENV_FILE)" --file "$(COMPOSE_FILE)" --project-name traffic-local
COMPOSE_ALL := $(COMPOSE) --profile batch --profile orchestration
PYTHON := $(PROJECT_ROOT)/.venv/bin/python
GITHUB_REMOTE ?= https://github.com/HungPhamNoob/traffic-risk-assessment.git
GIT_BRANCH ?= hung1
LOCAL_MAKEFILE := $(lastword $(MAKEFILE_LIST))

.PHONY: help setup validate format lint test compile compose-check \
	up up-batch up-orchestration up-monitoring down stop ps logs \
	reset reset-realtime reset-hard pipeline full-pipeline train-before train-after \
	spark-submit api-check metrics git-push-hung1 docker-recover kill-local-orphans

help:
	@echo "Local Makefile for the Traffic Risk Assessment platform"
	@echo "  make -f makefile/local/Makefile setup            Prepare local folders and .env"
	@echo "  make -f makefile/local/Makefile up               Start core local services"
	@echo "  make -f makefile/local/Makefile up-batch         Start Spark master and 3 workers"
	@echo "  make -f makefile/local/Makefile up-monitoring    Start Prometheus, Grafana, blackbox exporter"
	@echo "  make -f makefile/local/Makefile pipeline         Run local end-to-end pipeline"
	@echo "  make -f makefile/local/Makefile full-pipeline    Run local pipeline with before-2020 H2O training"
	@echo "  make -f makefile/local/Makefile train-before     Train H2O on before-2020 features"
	@echo "  make -f makefile/local/Makefile train-after      Retrain H2O from Gold after-2020 data"
	@echo "  make -f makefile/local/Makefile validate         Run lint, tests, compile, and compose checks"
	@echo "  make -f makefile/local/Makefile git-push-hung1   Commit and push the current workspace to hung1"
	@echo "  make -f makefile/local/Makefile docker-recover   Restart Docker daemon when containers cannot stop"

setup:
	@mkdir -p \
		"$(PROJECT_ROOT)/data/raw" \
		"$(PROJECT_ROOT)/data/split" \
		"$(PROJECT_ROOT)/data/process" \
		"$(PROJECT_ROOT)/data/simulation" \
		"$(PROJECT_ROOT)/orchestration/logs" \
		"$(PROJECT_ROOT)/ml/training"
	@test -f "$(ENV_FILE)" || cp "$(PROJECT_ROOT)/.env.example" "$(ENV_FILE)"
	@echo "Local folders and environment file are ready."

format:
	cd "$(PROJECT_ROOT)" && .venv/bin/python -m black . --exclude='(\.git|\.venv|venv|vendor|data|ml/notebooks)'

lint:
	cd "$(PROJECT_ROOT)" && .venv/bin/python -m black --check . --exclude='(\.git|\.venv|venv|vendor|data|ml/notebooks)'
	cd "$(PROJECT_ROOT)" && .venv/bin/python -m flake8 . --exclude=venv,.venv,vendor,data,ml/notebooks --max-line-length=120 --extend-ignore=E203,W503,W605

test:
	cd "$(PROJECT_ROOT)" && .venv/bin/python -m pytest tests/ -q

compile:
	cd "$(PROJECT_ROOT)" && python3 -m py_compile \
		processing/feature_engineering.py \
		processing/flink_streaming.py \
		processing/spark_batch.py \
		ingestion/kafka/us_producer.py \
		ml/dataset/dataset_offline.py \
		ml/training/h2o_before_2020.py \
		ml/training/h2o_after_2020.py \
		dashboard/backend/app/app.py

compose-check:
	$(COMPOSE) config --quiet

validate: lint test compile compose-check
	@echo "Local validation completed successfully."

up:
	$(COMPOSE) up -d zookeeper kafka-topic-init postgres redis mlflow fastapi prometheus grafana

up-batch:
	$(COMPOSE) --profile batch up -d spark-master spark-worker-1 spark-worker-2 spark-worker-3

up-orchestration:
	$(COMPOSE) --profile orchestration up -d airflow-db airflow

up-monitoring:
	$(COMPOSE) up -d blackbox-exporter prometheus grafana

stop:
	$(COMPOSE_ALL) stop --timeout 60 || ($(MAKE) -f "$(LOCAL_MAKEFILE)" docker-recover && $(COMPOSE_ALL) stop --timeout 60)

down:
	$(COMPOSE_ALL) down --remove-orphans || ($(MAKE) -f "$(LOCAL_MAKEFILE)" docker-recover && $(COMPOSE_ALL) down --remove-orphans)

ps:
	$(COMPOSE_ALL) ps

logs:
	$(COMPOSE_ALL) logs -f --tail=200

reset-realtime:
	rm -rf "$(PROJECT_ROOT)/data/simulation/checkpoints" \
		"$(PROJECT_ROOT)/data/simulation/silver" \
		"$(PROJECT_ROOT)/data/simulation/gold" \
		"$(PROJECT_ROOT)/data/simulation/api" \
		"$(PROJECT_ROOT)/data/simulation/performance"
	mkdir -p "$(PROJECT_ROOT)/data/simulation"
	@echo "Local realtime checkpoints and generated simulation layers were reset."

reset:
	$(COMPOSE_ALL) down --volumes --remove-orphans || ($(MAKE) -f "$(LOCAL_MAKEFILE)" docker-recover && $(COMPOSE_ALL) down --volumes --remove-orphans)
	rm -rf "$(PROJECT_ROOT)/data/simulation"
	mkdir -p "$(PROJECT_ROOT)/data/simulation"
	@echo "Local Docker volumes and simulation state were reset."

reset-hard: docker-recover reset

pipeline:
	cd "$(PROJECT_ROOT)" && bash scripts/local/run_pipeline.sh

full-pipeline:
	cd "$(PROJECT_ROOT)" && LOCAL_SAMPLE_ROWS=0 LOCAL_RUN_TRAINING=true bash scripts/local/run_pipeline.sh

train-before:
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" ml/training/h2o_before_2020.py

train-after:
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" ml/training/h2o_after_2020.py

spark-submit:
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" -m pyspark.sql.shell >/dev/null 2>&1 || true
	cd "$(PROJECT_ROOT)" && .venv/bin/spark-submit --master local[*] processing/spark_batch.py

api-check:
	curl -fsS http://localhost:8000/health
	curl -fsS http://localhost:8000/api/v1/system/status
	curl -fsS http://localhost:8000/api/v1/overview/summary

metrics:
	curl -fsS http://localhost:8000/metrics | head -40

git-push-hung1:
	cd "$(PROJECT_ROOT)" && git init
	cd "$(PROJECT_ROOT)" && git config user.name "HungPhamNoob"
	cd "$(PROJECT_ROOT)" && git config user.email "hungvp711@gmail.com"
	cd "$(PROJECT_ROOT)" && git add .
	cd "$(PROJECT_ROOT)" && git commit -m "$${GIT_COMMIT_MESSAGE:-commit}" || true
	cd "$(PROJECT_ROOT)" && git remote get-url origin >/dev/null 2>&1 || git remote add origin $(GITHUB_REMOTE)
	cd "$(PROJECT_ROOT)" && if [ -n "$${GITHUB_TOKEN:-}" ]; then \
		git remote set-url origin "https://$${GITHUB_TOKEN}@github.com/HungPhamNoob/traffic-risk-assessment.git"; \
	else \
		git remote set-url origin "$(GITHUB_REMOTE)"; \
	fi
	cd "$(PROJECT_ROOT)" && git branch -M $(GIT_BRANCH)
	cd "$(PROJECT_ROOT)" && git push -u origin $(GIT_BRANCH)

kill-local-orphans:
	@echo "Removing stale local traffic containers that Docker may leave after a Snap/AppArmor failure."
	@sudo pkill -f 'kafka.Kafka /etc/kafka/kafka.properties' 2>/dev/null || true
	@sudo pkill -f 'uvicorn app.app:app' 2>/dev/null || true
	@sudo pkill -f 'gunicorn -b 0.0.0.0:5000' 2>/dev/null || true
	@sudo pkill -x postgres 2>/dev/null || true

docker-recover:
	@echo "Restarting Docker can clear permission-denied stop errors from stale containers."
	@if docker info --format '{{.DockerRootDir}}' 2>/dev/null | grep -q '/var/snap/docker'; then \
		tmp_profile=$$(mktemp); \
		printf 'profile docker-default {\n}\n' > $$tmp_profile; \
		sudo apparmor_parser -R $$tmp_profile 2>/dev/null || true; \
		rm -f $$tmp_profile; \
		sudo snap restart docker; \
	else \
		sudo systemctl restart docker 2>/dev/null || systemctl --user restart docker 2>/dev/null || true; \
	fi
	@sleep 5
	@docker ps >/dev/null
	@echo "Docker daemon is reachable."
