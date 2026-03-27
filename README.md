# London Data Model

London Data Model is a GitHub-first Python project for building local-data pipelines from official UK sources.

The first pipeline focuses on school discovery for `KT19`. It is designed to:

- find nearby state-funded mainstream schools
- compute distance from a configured search point
- assign simple accessibility bands
- attach official Ofsted context
- surface data quality issues instead of hiding them

The project follows the execution plan in [docs/PLAN.md](docs/PLAN.md).

## Current Status

The repository currently contains the initial scaffold:

- Python package under `src/`
- CLI entry point: `ldm`
- school pipeline module structure
- config folders for areas, pipeline settings, and thresholds
- local data output folders
- smoke tests

Business logic and real data extraction are not implemented yet.

## Project Layout

```text
configs/
  areas/
  pipeline/
  thresholds/

data/
  raw/
  staging/
  marts/
  manifests/

docs/
  PLAN.md

src/
  london_data_model/
    cli.py
    pipelines/
      schools/

tests/
```

## Getting Started

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install the project:

```bash
pip install -e .[dev]
```

Run the CLI stub:

```bash
ldm schools run --area KT19
```

Run tests:

```bash
python3 -m unittest discover -s tests -p 'test_*.py' -v
```

## Development Principles

- official data only
- no admissions prediction in V1
- no silent record dropping
- no database in V1
- prefer simple, traceable outputs

## Next Steps

The next implementation target is the school pipeline skeleton:

- define stage interfaces more fully
- wire placeholder data contracts
- keep logging and manifests explicit
- avoid real source integration until the interfaces are stable
