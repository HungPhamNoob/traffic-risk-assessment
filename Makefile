.PHONY: help up down logs restart setup test lint format

# ==================== DOCKER ====================
help:
	@echo "📚 Capstone Team 4 - Available commands:"
	@echo "  make up              - Start all services (Node 1)"
	@echo "  make up-node2        - Start streaming services (Node 2)"
	@echo "  make up-node3        - Start batch services (Node 3)"
	@echo "  make down            - Stop all services"
	@echo "  make logs [service]  - View logs of a service"
	@echo "  make restart [service] - Restart a service"

up:
	cd deployment/node1-control && docker-compose up -d

up-node2:
	cd deployment/node2-streaming && docker-compose up -d

up-node3:
	cd deployment/node3-batch && docker-compose up -d

down:
	cd deployment/node1-control && docker-compose down
	cd deployment/node2-streaming && docker-compose down 2>/dev/null || true
	cd deployment/node3-batch && docker-compose down 2>/dev/null || true

logs:
ifndef service
	$(error Please specify service: make logs service=airflow)
endif
	cd deployment/node1-control && docker-compose logs -f $(service)

restart:
ifndef service
	$(error Please specify service: make restart service=airflow)
endif
	cd deployment/node1-control && docker-compose restart $(service)

# ==================== SETUP ====================
setup:
	@echo "🔄 Initializing project..."
	@cp .env.example .env 2>/dev/null || true
	@mkdir -p data/{raw,processed,checkpoints}
	@mkdir -p orchestration/{dags,logs,plugins}
	@mkdir -p ml/{mlruns,models}
	@echo "✅ Setup complete! Edit .env before starting."

setup-gcp:
	@echo "🔐 Running GCP setup script..."
	bash scripts/setup_gcp.sh

# ==================== TESTING ====================
test:
	@echo "🧪 Running tests..."
	pytest tests/ -v --tb=short

test-etl:
	@echo "🧪 Running ETL tests..."
	pytest tests/test_etl.py -v

test-flink:
	@echo "🧪 Running Flink tests..."
	pytest tests/test_flink_scorer.py -v

test-api:
	@echo "🧪 Running API tests..."
	pytest tests/test_api.py -v

# ==================== CODE QUALITY ====================
lint:
	@echo "🔍 Running linter..."
	flake8 ingestion/ processing/ ml/ dashboard/ --exclude=venv,.venv
	black --check ingestion/ processing/ ml/ dashboard/ --exclude=venv

format:
	@echo "✨ Formatting code..."
	black ingestion/ processing/ ml/ dashboard/ --exclude=venv,.venv
	isort ingestion/ processing/ ml/ dashboard/ --skip=venv --skip=.venv

# ==================== GCP MANAGEMENT ====================
start-nodes:
	@echo "🚀 Starting GCP nodes..."
	bash scripts/manage-nodes.sh start

stop-nodes:
	@echo "🛑 Stopping GCP nodes..."
	bash scripts/manage-nodes.sh stop

status-nodes:
	@echo "📊 Checking node status..."
	bash scripts/manage-nodes.sh status

# ==================== BACKUP ====================
backup:
	@echo "💾 Running backup..."
	bash scripts/backup_checkpoints.sh

# ==================== DOCS ====================
docs:
	@echo "📖 Generating docs..."
	pdoc --html --output-dir docs/api ml/ dashboard/backend/ 2>/dev/null || echo "Install pdoc: pip install pdoc3"