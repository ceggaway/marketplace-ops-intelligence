PYTHON := .venv/bin/python
PIP    := .venv/bin/pip
NPM    := npm --prefix frontend-react

.PHONY: venv install install-frontend api frontend train score monitor monitor-loop poller poller-once test run-all clean

venv:
	python3 -m venv .venv

install: venv
	$(PIP) install -r requirements.txt
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

monitor:
	$(PYTHON) scripts/run_monitoring.py

monitor-loop:
	mkdir -p data/logs
	nohup $(PYTHON) scripts/run_monitoring.py --loop --interval 300 > data/logs/monitor.log 2>&1 &
	@echo "Monitor loop started. Tail logs with: tail -f data/logs/monitor.log"

poller:
	mkdir -p data/logs
	nohup $(PYTHON) -m backend.ingestion.lta_poller > data/logs/poller.log 2>&1 &
	@echo "Poller started in background. Tail logs with: tail -f data/logs/poller.log"

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

run-all:
	make api & make frontend

clean:
	rm -rf data/outputs/* data/processed/* data/registry/models/* data/registry/registry.json
