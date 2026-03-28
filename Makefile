PYTHON  ?= python3
BOROUGH ?= barnet
VENV    := .venv
PY      := $(VENV)/bin/python
LDM     := $(VENV)/bin/ldm
PYTEST  := $(VENV)/bin/pytest

.PHONY: venv install-dev test run-schools run-london run-borough clean

venv:
	$(PYTHON) -m venv $(VENV)
	$(PY) -m pip install --upgrade pip -q

install-dev: venv
	$(PY) -m pip install -e .[dev]

test:
	$(PYTEST)

# KT19 pilot area (sample mode)
run-schools:
	$(LDM) schools run --area KT19

# All 33 London boroughs (official mode)
run-london:
	$(LDM) schools run --area london --input-mode official

# Single borough (official mode). Override with: make run-borough BOROUGH=camden
run-borough:
	$(LDM) schools run --area $(BOROUGH) --input-mode official

clean:
	rm -rf $(VENV) data/marts/* data/manifests/*
