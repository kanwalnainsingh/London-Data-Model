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

The repository also contains a static GitHub Pages view under `docs/`. That page should consume committed public artifacts from `docs/data/`, which are generated from the pipeline summary and manifest outputs. This keeps the public page inside the agreed MVP scope without publishing invented school rows.

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

**Requires Python 3.8+.** Check with `python3 --version`.

### Quickstart (recommended)

The Makefile handles everything — no manual venv activation needed:

```bash
make install-dev   # creates .venv and installs the package + dev deps
make test          # runs the full test suite (28 tests)
make run-schools   # runs the KT19 sample pipeline
```

### Manual setup (alternative)

If you prefer to manage the venv yourself:

```bash
python3 -m venv .venv
source .venv/bin/activate          # macOS / Linux
# .venv\Scripts\activate           # Windows

pip install -e .[dev]

ldm schools run --area KT19        # run the pipeline
pytest                             # run tests
```

> All `ldm` and `pytest` commands require either the venv to be activated or using the `make` targets above (which use `.venv/bin/` paths directly).

### What the sample run does

`make run-schools` uses placeholder JSON files in `data/raw/` so the pipeline runs end-to-end without real school records. Outputs go to `data/marts/` and `data/manifests/`.

To switch to real data, update `input_mode` in `configs/pipeline/schools.yml` from `sample` to `official`, place the official source files under `data/raw/`, then:

```bash
ldm schools run --area KT19 --input-mode official
```

The KT19 area config uses explicit configured coordinates in [`configs/areas/kt19.yml`](configs/areas/kt19.yml). It is intentionally labeled as configured user-supplied coordinates, not an asserted official centroid.

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

## GitHub Pages

The repository includes a GitHub Pages-compatible status page:

- `docs/index.html`
- `docs/data/kt19-status.json`
- `docs/data/kt19-summary.json`
- `docs/data/kt19-manifest.json`

These `docs/data/` artifacts are a public mirror of the current published summary and manifest for the KT19 pipeline. They are intended for static page consumption only.

The public manifest is intentionally sanitized for GitHub Pages. Internal local filesystem paths remain in `data/manifests/`, not in `docs/data/`.

If Pages is enabled for the `main` branch `docs/` folder, the public URL should be:

```text
https://kanwalnainsingh.github.io/London-Data-Model/
```

## GitHub Actions

The repository now has two GitHub Actions workflows:

- `CI`: runs `pytest -q` on pushes and pull requests on Python `3.8` and `3.11`
- `Deploy Pages`: publishes the `docs/` site to GitHub Pages from GitHub Actions
- `Refresh Public Data`: runs on a daily schedule and manual dispatch, executes the London official pipeline, commits refreshed `docs/data/` artifacts when outputs change, and deploys the updated site in the same workflow

Current automation gap to be aware of:

- the scheduled refresh can auto-fetch GIAS and Ofsted because those sources are already wired into the pipeline fetch stage
- KS4 and KS5 inputs are still disabled by config, so the refresh workflow will not publish performance metrics until those source URLs and mappings are configured
- GitHub Pages must be configured with `build_type: workflow` in repository settings or via the Pages API for the deploy workflows to be authoritative
