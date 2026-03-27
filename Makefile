PYTHON ?= python3

.PHONY: install-dev test run-schools

install-dev:
	$(PYTHON) -m pip install -e .[dev]

test:
	pytest

run-schools:
	ldm schools run --area KT19
