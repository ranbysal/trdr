VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(PYTHON) -m pip
PYTEST := $(VENV)/bin/pytest

.PHONY: venv deps test

venv:
	python3 -m venv --system-site-packages $(VENV)

deps: venv
	$(PIP) install --upgrade pip setuptools wheel
	$(PIP) install -r requirements-dev.txt || $(PYTHON) -c "import numpy, pandas, yaml, pytest"

test:
	$(PYTEST) -q
