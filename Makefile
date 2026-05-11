PYTHON := .venv/bin/python
PIP    := .venv/bin/pip
NPM    := npm --prefix frontend-react
DATA_DIR ?= data

.PHONY: venv install install-prod install-frontend api frontend train score score-prod monitor monitor-loop poller poller-once test verify deploy-check bootstrap-model build-frontend run-all clean

venv:
	python3 -m venv .venv

install: venv
	$(PIP) install -r requirements.txt
	$(PIP) install -e . --no-deps --quiet

install-prod: venv
	$(PIP) install -r requirements.deploy.txt
	$(PIP) install -e . --no-deps --quiet

install-frontend:
	$(NPM) install

api:
	$(PYTHON) -m uvicorn backend.api.main:app --reload --port 8000

frontend:
	$(NPM) run dev -- --host 0.0.0.0

train:
	$(PYTHON) scripts/run_training.py --version v1 --days 90

score:
	$(PYTHON) scripts/run_scoring.py

score-prod:
	$(PYTHON) scripts/run_scoring.py

monitor:
	$(PYTHON) scripts/run_monitoring.py

monitor-loop:
	mkdir -p $(DATA_DIR)/logs
	MARKETPLACE_DATA_DIR=$(DATA_DIR) nohup $(PYTHON) scripts/run_monitoring.py --loop --interval 300 > $(DATA_DIR)/logs/monitor.log 2>&1 &
	@echo "Monitor loop started. Tail logs with: tail -f $(DATA_DIR)/logs/monitor.log"

poller:
	mkdir -p $(DATA_DIR)/logs
	MARKETPLACE_DATA_DIR=$(DATA_DIR) nohup $(PYTHON) -m backend.ingestion.lta_poller > $(DATA_DIR)/logs/poller.log 2>&1 &
	@echo "Poller started in background. Tail logs with: tail -f $(DATA_DIR)/logs/poller.log"

poller-once:
	$(PYTHON) -m backend.ingestion.lta_poller --once

download-data:
	$(PYTHON) scripts/download_data.py --all

download-zones:
	$(PYTHON) scripts/download_data.py --source zones

download-holidays:
	$(PYTHON) scripts/download_data.py --source holidays

download-lta:
	$(PYTHON) scripts/download_data.py --source lta

download-grab:
	$(PYTHON) scripts/download_data.py --source grab

test:
	$(PYTHON) -m pytest tests/ -v

build-frontend:
	$(NPM) run build

verify:
	$(PYTHON) -m pytest tests/ -q
	$(NPM) run build
	$(NPM) run lint

bootstrap-model:
	$(PYTHON) scripts/bootstrap_model.py

deploy-check:
	$(PYTHON) scripts/deploy_check.py --require-frontend-build

run-all:
	make api & make frontend

clean:
	rm -rf $(DATA_DIR)/outputs/* $(DATA_DIR)/processed/* $(DATA_DIR)/registry/models/* $(DATA_DIR)/registry/registry.json
