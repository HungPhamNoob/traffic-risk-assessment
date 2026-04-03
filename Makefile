.PHONY: ingest train deploy-node1 deploy-node2 deploy-node3 local-dev test clean

ingest:
	python ingestion/kafka/producers/us_producer.py

train:
	python ml/training/train_h2o.py

deploy-node1:
	bash deployment/node1-control/setup.sh

deploy-node2:
	bash deployment/node2-streaming/setup.sh

deploy-node3:
	bash deployment/node3-batch/setup.sh

local-dev:
	docker-compose up -d

test:
	pytest tests/

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
