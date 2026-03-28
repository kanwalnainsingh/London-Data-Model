PYTHON ?= python3
BOROUGH ?= barnet

.PHONY: install-dev test run-schools run-london run-borough

install-dev:
	$(PYTHON) -m pip install -e .[dev]

test:
	pytest

# KT19 pilot area (sample mode)
run-schools:
	ldm schools run --area KT19

# All 33 London boroughs (official mode)
run-london:
	ldm schools run --area london --input-mode official

# Single borough (official mode). Override with: make run-borough BOROUGH=camden
run-borough:
	ldm schools run --area $(BOROUGH) --input-mode official
