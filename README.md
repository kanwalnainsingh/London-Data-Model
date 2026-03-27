# London Data Model

London Data Model is a GitHub-first Python project for building local-data pipelines from official UK sources.

The first pipeline focuses on school discovery for `KT19`. It is designed to:

- find nearby state-funded mainstream schools
- compute distance from a configured search point
- assign simple accessibility bands
- attach official Ofsted context
- surface data quality issues instead of hiding them

The project follows the execution plan in [docs/PLAN.md](docs/PLAN.md).
Official source notes are in [docs/SOURCES.md](docs/SOURCES.md).

## Current Status

The repository currently contains the initial scaffold:

- Python package under `src/`
- CLI entry point: `ldm`
- school pipeline module structure
- config folders for areas, pipeline settings, and thresholds
- local data output folders
- smoke tests

Business logic is partially implemented for pipeline structure, distance, quality rules, and local official-source integration.

The remaining gap for a meaningful real-data run is not pipeline wiring. It is local official input data plus the final search-point choice for the target area.

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

Run the checked-in KT19 sample pipeline:

```bash
PYTHONPATH=src python3 -m london_data_model.cli schools run --area KT19
```

This sample run uses placeholder JSON input files in `data/raw/` so the pipeline can execute end to end without inventing official school records.

To switch to local official source files, update `input_mode` in `configs/pipeline/schools.yml` from `sample` to `official` and place the configured files under `data/raw/`.

You can also override the configured mode directly from the CLI:

```bash
PYTHONPATH=src python3 -m london_data_model.cli schools run --area KT19 --input-mode official
```

The current KT19 area config now uses an explicit user-supplied search point in [`configs/areas/kt19.yml`](configs/areas/kt19.yml). This is a provisional district proxy for MVP testing, not a claimed official centroid.

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

## Issue And Commit Linkage

All new implementation work should start from a GitHub issue.

Expected workflow:

- create the issue first
- put the goal, scope, constraints, acceptance criteria, and plan in the issue
- reference the issue number in the commit or PR description
- push the work before closing the issue
- close the issue with the verification commands and commit reference

Published history should not be rewritten just to add issue references. If older pushed commits need traceability, backfill the relationship in the relevant issue comments instead.

## Sample Inputs

The checked-in sample input files are:

- `data/raw/kt19_schools_sample.json`
- `data/raw/kt19_ofsted_sample.json`

They are deliberately empty placeholders. They exist to make the KT19 pipeline run reproducible while preserving the project rule that published outputs should be based on official data.

## Official Input Mode

Real pipeline inputs should come from:

- DfE Get Information about Schools establishments data
- Ofsted state-funded schools inspection data

The current integration is file-based and local. See [docs/SOURCES.md](docs/SOURCES.md) for the source references and caveats.

### Real KT19 Run Checklist

To perform the first meaningful KT19 test with real data:

1. Download the current official GIAS establishments file and place it at the path configured by `official_input.schools_path`.
2. Download or export the current official Ofsted state-funded schools inspection file and place it at the path configured by `official_input.ofsted_path`.
3. Confirm the real file headers match the configured column maps in [`configs/pipeline/schools.yml`](configs/pipeline/schools.yml).
4. Switch `input_mode` from `sample` to `official`.
5. Run:

```bash
PYTHONPATH=src python3 -m london_data_model.cli schools run --area KT19 --input-mode official
```

If the files or headers do not match, the pipeline should fail fast with an explicit official-source configuration error.
