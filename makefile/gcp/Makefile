SHELL := /bin/bash

PROJECT_ID ?= big-data-group-4
ZONE ?= us-central1-a
BRANCH ?= hung1
NODE1 ?= node1-control
NODE2 ?= node2-streaming
NODE3 ?= node3-batch
PROJECT_ROOT ?= /opt/traffic

SSH_BASE := gcloud compute ssh --project=$(PROJECT_ID) --zone=$(ZONE) --tunnel-through-iap --quiet

.PHONY: help list ssh-node1 ssh-node2 ssh-node3 sync-env \
	node1 node2 node3 start-node23 reset-realtime status logs-node1 logs-node2 logs-node3 \
	validate push deploy-all full-reset-run collect-metrics backup-checkpoints kafka-topic-check docker-recover-node1 docker-recover-node2 docker-recover-node3

help:
	@echo "GCP Makefile for the Traffic Risk Assessment platform"
	@echo "  make -f makefile/gcp/Makefile list             List GCP traffic VMs"
	@echo "  make -f makefile/gcp/Makefile node1            Start Node 1 control services"
	@echo "  make -f makefile/gcp/Makefile node2            Start Node 2 streaming services"
	@echo "  make -f makefile/gcp/Makefile node3            Start Node 3 Spark batch services"
	@echo "  make -f makefile/gcp/Makefile start-node23     Start Node 2 and Node 3 together"
	@echo "  make -f makefile/gcp/Makefile reset-realtime   Reset Flink/Spark checkpoints together"
	@echo "  make -f makefile/gcp/Makefile status           Print Docker status on all nodes"
	@echo "  make -f makefile/gcp/Makefile kafka-topic-check Verify the single Kafka topic on Node 2"
	@echo "  make -f makefile/gcp/Makefile backup-checkpoints Back up GCP checkpoint directories"
	@echo "  make -f makefile/gcp/Makefile deploy-all       Pull branch and start all node services"
	@echo "  make -f makefile/gcp/Makefile full-reset-run  Reset and run the full cloud pipeline"
	@echo "  make -f makefile/gcp/Makefile collect-metrics Collect measured cloud evidence"

list:
	gcloud compute instances list --project=$(PROJECT_ID) \
		--filter='name~node' \
		--format='table(name,zone,status,networkInterfaces[0].networkIP)'

ssh-node1:
	$(SSH_BASE) $(NODE1)

ssh-node2:
	$(SSH_BASE) $(NODE2)

ssh-node3:
	$(SSH_BASE) $(NODE3)

sync-env:
	gcloud storage cp .env.cloud gs://big-data-group-4-bronze/env/.env.cloud

node1:
	$(SSH_BASE) $(NODE1) --command='cd $(PROJECT_ROOT) && git fetch origin && git checkout $(BRANCH) && git pull origin $(BRANCH) && bash scripts/gcp/run-node1.sh'

node2:
	$(SSH_BASE) $(NODE2) --command='cd $(PROJECT_ROOT) && git fetch origin && git checkout $(BRANCH) && git pull origin $(BRANCH) && bash scripts/gcp/run-node2.sh'

node3:
	$(SSH_BASE) $(NODE3) --command='cd $(PROJECT_ROOT) && git fetch origin && git checkout $(BRANCH) && git pull origin $(BRANCH) && bash scripts/gcp/run-node3.sh'

start-node23:
	$(SSH_BASE) $(NODE1) --command='cd $(PROJECT_ROOT) && bash scripts/gcp/start-node23-synced.sh'

reset-realtime:
	$(SSH_BASE) $(NODE1) --command='cd $(PROJECT_ROOT) && RESET_LOCAL_COMPOSE=false RESET_POSTGRES=true RESET_GCS=true bash scripts/gcp/reset-realtime.sh'
	$(SSH_BASE) $(NODE2) --command='cd $(PROJECT_ROOT) && RESET_LOCAL_COMPOSE=true RESET_POSTGRES=false RESET_GCS=false bash scripts/gcp/reset-realtime.sh'
	$(SSH_BASE) $(NODE3) --command='cd $(PROJECT_ROOT) && RESET_LOCAL_COMPOSE=true RESET_POSTGRES=false RESET_GCS=false bash scripts/gcp/reset-realtime.sh'

status:
	$(SSH_BASE) $(NODE1) --command='cd $(PROJECT_ROOT)/deployment/node1-control && docker compose --env-file $(PROJECT_ROOT)/.env.cloud ps'
	$(SSH_BASE) $(NODE2) --command='cd $(PROJECT_ROOT)/deployment/node2-streaming && docker compose --env-file $(PROJECT_ROOT)/.env.cloud ps'
	$(SSH_BASE) $(NODE3) --command='cd $(PROJECT_ROOT)/deployment/node3-batch && docker compose --env-file $(PROJECT_ROOT)/.env.cloud ps'

logs-node1:
	$(SSH_BASE) $(NODE1) --command='cd $(PROJECT_ROOT)/deployment/node1-control && docker compose --env-file $(PROJECT_ROOT)/.env.cloud logs --tail=200'

logs-node2:
	$(SSH_BASE) $(NODE2) --command='cd $(PROJECT_ROOT)/deployment/node2-streaming && docker compose --env-file $(PROJECT_ROOT)/.env.cloud logs --tail=200'

logs-node3:
	$(SSH_BASE) $(NODE3) --command='cd $(PROJECT_ROOT)/deployment/node3-batch && docker compose --env-file $(PROJECT_ROOT)/.env.cloud logs --tail=200'

validate:
	docker compose --env-file .env.cloud -f deployment/node1-control/docker-compose.yaml config --quiet
	docker compose --env-file .env.cloud -f deployment/node2-streaming/docker-compose.yaml config --quiet
	docker compose --env-file .env.cloud -f deployment/node3-batch/docker-compose.yaml config --quiet

push:
	git push -u origin $(BRANCH)

deploy-all: sync-env node1 start-node23

full-reset-run:
	BRANCH=$(BRANCH) GCP_PROJECT_ID=$(PROJECT_ID) GCP_ZONE=$(ZONE) bash scripts/gcp/full-cloud-reset-run.sh

collect-metrics:
	GCP_PROJECT_ID=$(PROJECT_ID) GCP_ZONE=$(ZONE) bash scripts/gcp/collect-cloud-metrics.sh

backup-checkpoints:
	$(SSH_BASE) $(NODE1) --command='cd $(PROJECT_ROOT) && bash scripts/gcp/backup-checkpoints.sh'

kafka-topic-check:
	$(SSH_BASE) $(NODE2) --command='docker exec node2-kafka-1 kafka-topics --bootstrap-server kafka-1:29092,kafka-2:29092,kafka-3:29092 --describe --topic "$${KAFKA_TOPIC_RAW:-traffic.us.raw}"'

docker-recover-node1:
	$(SSH_BASE) $(NODE1) --command='sudo systemctl restart docker || true'

docker-recover-node2:
	$(SSH_BASE) $(NODE2) --command='sudo systemctl restart docker || true'

docker-recover-node3:
	$(SSH_BASE) $(NODE3) --command='sudo systemctl restart docker || true'
