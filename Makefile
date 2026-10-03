export PATH := $(shell pwd)/.venv/bin:$(PATH)

.PHONY: setup test data stats

setup:
	pip install -r requirements.txt && pip install -e .

test:
	pytest -q

data:
	python -m kairos.harness.trace_replay --generate --out data/processed/

stats:
	python -m kairos.harness.trace_replay --stats --trace data/processed/synthetic.parquet
