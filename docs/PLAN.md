London Data Model - Full Project Plan
====================================

## 1. Purpose

Build an open-source, GitHub-first data pipeline that helps users explore local areas using official UK data.

The first use case is school discovery for `KT19`.

The second use case, to be prepared for but not implemented in V1, is admissions and catchment relevance.

This document is the source of truth for:

- Product scope
- Data contracts
- Pipeline architecture
- Multi-agent delivery boundaries
- Delivery phases
- Guardrails against guesswork and hallucination

## 2. Product Vision

The product should help a user answer:

- What state-funded mainstream schools are near a search area such as `KT19`?
- How far are they from the search point?
- How accessible are they using simple distance-based labels?
- What official Ofsted context is available?
- What is missing or uncertain in the data?

The product should not attempt to answer:

- Whether a child will get an offer
- Whether a school is definitively in catchment in V1
- Subjective judgments about school quality beyond official source fields

## 3. V1 Outcome

V1 is an end-to-end school discovery pipeline for `KT19`.

Input:

- A configured search area, initially `KT19`

Processing:

- Resolve a search point
- Extract school and Ofsted source data
- Filter to in-scope schools
- Standardise records
- Compute distance and accessibility
- Attach Ofsted context
- Assign data quality flags
- Publish outputs and a manifest

Output:

- A clean tabular dataset
- A machine-readable structured dataset
- A summary JSON
- A manifest with traceability metadata

Success means the pipeline runs locally, produces deterministic outputs, and never hides data quality issues.

## 4. V2 Outcome

V2 is admissions and catchment relevance, not prediction.

Future work may include:

- Admissions policy links
- Distance criterion extraction
- Historical offer distance where official data exists
- Catchment indicators where official sources publish them

V2 must remain separate from V1. V1 should be designed so these fields can be added later without breaking core output structure.

## 5. Non-Negotiables

- Official data only
- No hallucinated insights
- No AI-generated school strengths or summaries
- No admission guarantees
- No silent record dropping
- Full traceability from outputs back to source files
- No database in V1
- Keep the initial implementation simple and local-file based
- Multi-agent work is allowed only as a delivery method, not as a runtime product feature

## 6. Scope

### 6.1 Include in V1

- State-funded mainstream primary schools
- State-funded mainstream secondary schools
- State-funded all-through schools
- Open establishments only, subject to source status mapping
- Distance from a single resolved search point
- Latest official Ofsted context where available

### 6.2 Exclude in V1

- Private schools
- Special schools
- Colleges and sixth-form colleges
- Independent nursery-only settings
- Admissions prediction
- Subjective ranking logic
- Web frontend
- Database storage

## 7. Primary User Need

A user wants trustworthy, structured local school discovery data for a defined search area.

The product must optimise for:

- Honesty
- Consistency
- Traceability
- Ease of local execution

The product must not optimise for:

- Fancy scoring systems with unclear meaning
- Premature platform complexity
- Broad national coverage before the `KT19` flow is correct

## 8. Core Assumptions

These assumptions are acceptable for scaffolding and early implementation, but must remain configurable:

- The pipeline will be written in Python.
- Execution will happen locally through a CLI.
- Output artifacts will live under `data/`.
- Search areas will initially be configured rather than entered through a UI.
- Official school and inspection data will be stored under `data/raw/` before transformation.
- Delivery may be split across specialised agents, but runtime execution remains one local CLI, one repository, and one linear pipeline.

## 9. Open Decisions That Must Be Controlled Explicitly

These are the main ambiguity points. Code must not bury these assumptions in business logic.

### 9.1 Search Point Resolution

The plan says "postcode or district (`KT19`)", but these are not the same thing.

The implementation must explicitly support one of these modes:

- Postcode centroid
- District centroid
- User-supplied latitude/longitude

For V1, keep this as configuration and record the chosen method in the manifest.

### 9.2 Official Source Selection

The implementation must document and pin the official source used for:

- School establishment records
- Ofsted inspection context
- Search area geospatial resolution if needed

Until source selection is final, modules should expose interfaces that keep extraction replaceable.

### 9.3 Proximity Score Definition

`proximity_score` is required by the plan but not fully defined.

V1 implementation rule:

- Keep the field in the schema
- Make the scoring method deterministic and simple
- Define the exact formula in config or a dedicated rule section before implementation

### 9.4 All-Through Handling

All-through schools must not be handled by ad hoc conditional logic.

V1 rule:

- Map all-through explicitly to a threshold profile in configuration
- Record the mapping in code comments or config description

### 9.5 Open Status Mapping

`is_open` depends on source values. The mapping from source status to boolean must be explicit and documented.

## 10. Data Sources Strategy

V1 will only use official UK sources.

The source strategy should follow this pattern:

- `schools_source`: official establishment data
- `ofsted_source`: official inspection outcome data
- `geography_source`: official or otherwise approved search-area geometry or centroid reference

Each source must be represented in manifests with:

- `source_name`
- `source_type`
- `source_uri` or local file path
- `retrieved_at`
- `version` or file hash if available

The implementation must prefer pinned input files over live network calls in normal pipeline runs.

## 11. Functional Requirements

### 11.1 Input

The pipeline must accept a configured search area.

Initial supported mode:

- `KT19`

Future-friendly modes:

- Postcode
- Outward code or district
- Coordinate pair

### 11.2 Filtering

The pipeline must include only in-scope establishments:

- Mainstream primary
- Mainstream secondary
- Mainstream all-through

The pipeline must exclude:

- Independent
- Special
- College-only
- Closed establishments

Any exclusion rule must be source-driven and testable.

### 11.3 Distance

The pipeline must compute `distance_km` from the resolved search point to each school.

Implementation target:

- Haversine formula

### 11.4 Accessibility

The pipeline must assign `accessibility_band` based on school phase and distance.

#### Primary thresholds

- `0-1 km` -> `very_close`
- `1-2 km` -> `close`
- `2-4 km` -> `moderate`
- `4+ km` -> `far`

#### Secondary thresholds

- `0-2 km` -> `very_close`
- `2-5 km` -> `close`
- `5-8 km` -> `moderate`
- `8+ km` -> `far`

#### All-through thresholds

Use an explicit config mapping in V1. Do not infer implicitly.

### 11.5 Ofsted Context

The pipeline must attach the latest available official Ofsted context where present.

Required target fields:

- `ofsted_rating_latest`
- `ofsted_inspection_date_latest`
- `ofsted_report_url`

Missing values must be flagged, not dropped.

### 11.6 Data Quality

The pipeline must evaluate every record and assign:

- `data_quality_status`
- `data_quality_flags`

The pipeline must never remove a record purely because some fields are missing.

### 11.7 Publishing

The pipeline must write:

1. Tabular output in `CSV`
2. Structured output in `JSON` and optionally `parquet` later
3. Summary output in `JSON`
4. Manifest output in `JSON`

For V1, use `CSV` plus `JSON` as the minimum guaranteed publish set. `Parquet` can remain optional until dependencies are justified.

## 12. Canonical Output Schema

Each school record must include these fields:

- `school_name`
- `school_urn`
- `address`
- `postcode`
- `latitude`
- `longitude`
- `phase`
- `establishment_type`
- `is_open`
- `distance_km`
- `accessibility_band`
- `proximity_score`
- `ofsted_rating_latest`
- `ofsted_inspection_date_latest`
- `ofsted_report_url`
- `data_quality_status`
- `data_quality_flags`

### 12.1 Suggested Types

- `school_name`: string
- `school_urn`: string
- `address`: string
- `postcode`: string or null
- `latitude`: float or null
- `longitude`: float or null
- `phase`: enum-like string
- `establishment_type`: string
- `is_open`: boolean
- `distance_km`: float or null
- `accessibility_band`: string or null
- `proximity_score`: float or null
- `ofsted_rating_latest`: string or null
- `ofsted_inspection_date_latest`: ISO date string or null
- `ofsted_report_url`: string or null
- `data_quality_status`: string
- `data_quality_flags`: array of strings

## 13. Data Quality Model

### 13.1 Rules

- Never drop records silently
- Always label issues
- Prefer partial data over erased data
- Preserve enough context for downstream review

### 13.2 Status Values

- `complete`
- `partial`
- `poor`

### 13.3 Example Flags

- `missing_postcode`
- `missing_coordinates`
- `missing_ofsted_rating`
- `invalid_phase`
- `distance_estimated`
- `missing_address`
- `missing_ofsted_report_url`
- `missing_inspection_date`

### 13.4 Suggested Status Logic

This is a planning rule and may be refined later:

- `complete`: no critical flags
- `partial`: some non-critical fields missing, but record remains useful
- `poor`: critical geospatial or classification gaps reduce trust substantially

The exact mapping from flags to status must be implemented transparently and covered by tests.

## 14. Proximity Score Plan

The plan requires a `proximity_score`, but it must not become a misleading black box.

V1 implementation rules:

- Keep it simple
- Keep it deterministic
- Keep it monotonic with distance
- Keep the formula documented
- Do not mix quality, Ofsted, or admissions logic into it

Recommended V1 direction:

- Normalise a score from distance only
- Use separate threshold config by phase if needed
- Return `null` when distance cannot be computed

Until implemented, treat this as an explicit TODO with a documented formula.

## 15. Runtime Architecture And Delivery Topology

The repository should follow this structure:

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
    __init__.py
    cli.py
    settings.py
    types.py
    utils/
    pipelines/
      schools/
        __init__.py
        extract.py
        transform.py
        validate.py
        publish.py
        pipeline.py

tests/
  unit/
  fixtures/
```

This keeps the Python package separate from raw folder names and gives a stable import path.

### 15.1 Multi-Agent Delivery Topology

The MVP should use a constrained multi-agent delivery model with explicit file ownership and one integration path.

Allowed agent roles:

- `spec-guardrail-agent`
- `contracts-agent`
- `runtime-orchestration-agent`
- `source-intake-agent`
- `school-normalization-agent`
- `quality-policy-agent`
- `artifact-publisher-agent`

These roles are delivery lanes only. They are not runtime services and they must not change the MVP product boundary.

### 15.2 Agent Ownership

- `spec-guardrail-agent`
  Owns: `docs/PLAN.md`, `docs/SOURCES.md`
  Responsible for: scope protection, source-policy decisions, V1/V2 guardrails
  Must not own: pipeline implementation files

- `contracts-agent`
  Owns: `src/london_data_model/types.py`
  Responsible for: shared contracts, schema fields, interface compatibility
  Must not own: extraction, transform, validation, publishing logic

- `runtime-orchestration-agent`
  Owns: `src/london_data_model/cli.py`, `src/london_data_model/pipelines/schools/pipeline.py`, `src/london_data_model/settings.py`, `src/london_data_model/utils/config.py`
  Responsible for: pipeline sequencing, config loading, run context creation, CLI behavior
  Must not own: source-field mapping, school scope logic, quality policy

- `source-intake-agent`
  Owns: `src/london_data_model/pipelines/schools/extract.py`
  Responsible for: local source loading, file validation, column mapping, source merge behavior
  Must not own: school eligibility rules, distance logic, publishing

- `school-normalization-agent`
  Owns: `src/london_data_model/pipelines/schools/transform.py`
  Responsible for: canonical school records, scope filtering, distance, accessibility, proximity score
  Must not own: source-file loading, data-quality policy, manifest writing

- `quality-policy-agent`
  Owns: `src/london_data_model/pipelines/schools/validate.py`
  Responsible for: `data_quality_flags`, `data_quality_status`, quality summaries
  Must not own: scope filtering, source merge logic, output layout

- `artifact-publisher-agent`
  Owns: `src/london_data_model/pipelines/schools/publish.py`
  Responsible for: stable output artifacts, summaries, manifests
  Must not own: source ingestion, school-domain rules

### 15.3 Coordination Rule

- `pipeline.py` is the only cross-stage coordinator.
- Agents coordinate through files, tests, manifests, and documented handoffs.
- No agent should introduce a runtime multi-agent system for V1.

## 16. Module Ownership And Handoff Responsibilities

### 16.1 `extract.py`

Responsibilities:

- Load configured raw inputs
- Parse source files into minimally cleaned records
- Attach source metadata needed for traceability

Primary owner:

- `source-intake-agent`

Must not:

- Apply business scoring
- Drop records for missing optional fields
- Edit transform, validation, or publishing rules without an explicit handoff

### 16.2 `transform.py`

Responsibilities:

- Standardise field names
- Filter in-scope school types
- Resolve phase values
- Compute `distance_km`
- Compute `accessibility_band`
- Compute `proximity_score`
- Attach Ofsted fields

Primary owner:

- `school-normalization-agent`

Must not:

- Publish files
- Load source files directly
- Change quality policy without an explicit handoff

### 16.3 `validate.py`

Responsibilities:

- Validate schema presence
- Assign `data_quality_flags`
- Assign `data_quality_status`
- Produce validation summary counts

Primary owner:

- `quality-policy-agent`

Must not:

- Remove records silently
- Reinterpret source-file schemas
- Change runtime orchestration

### 16.4 `publish.py`

Responsibilities:

- Write outputs to `data/marts/`
- Write summary JSON
- Write manifest JSON to `data/manifests/`

Primary owner:

- `artifact-publisher-agent`

### 16.5 `pipeline.py`

Responsibilities:

- Orchestrate extract, transform, validate, publish
- Handle run configuration
- Emit high-level logging
- Return a run result or exit code

Primary owner:

- `runtime-orchestration-agent`

Must not:

- Duplicate stage-specific business logic

## 17. CLI Plan

The project should expose a CLI entry point.

Minimum V1 commands:

- `ldm schools run`
- `ldm schools run --area KT19`
- `ldm schools run --config configs/areas/kt19.yml`

The first scaffold can implement a stub command that validates wiring without real data work.

## 18. Configuration Plan

Configuration should be explicit and file-based.

### 18.1 Area Config

Area config should define:

- `area_id`
- `area_type`
- `label`
- `search_point_method`
- `latitude`
- `longitude`

### 18.2 Threshold Config

Threshold config should define:

- Primary distance bands
- Secondary distance bands
- All-through band mapping
- Proximity score parameters

### 18.3 Source Config

Source config should define:

- File paths
- Format
- Version metadata where available

### 18.4 Delivery Conventions

Delivery should stay issue-driven:

- Create the GitHub issue first
- Put the task goal, scope, constraints, acceptance criteria, and plan in the issue
- Implement only after the issue exists
- Reference the issue number in the commit or PR description whenever work is pushed
- Push changes before closing the issue
- Close the issue with verification details and commit reference
- Backfill commit links in issue comments when older published commits cannot be rewritten safely

## 19. Manifest Requirements

Each run must emit a manifest JSON that includes at minimum:

- `run_id`
- `pipeline_name`
- `pipeline_version`
- `started_at`
- `finished_at`
- `area_id`
- `search_point_method`
- `search_point`
- `input_sources`
- `output_files`
- `record_counts`
- `quality_counts`
- `notes`

This is required for traceability and later debugging.

Optional delivery-trace fields may be added later, but the manifest must remain a product artifact first, not a workflow engine.

## 20. Summary Output Requirements

Each run should emit a summary JSON containing:

- `area_id`
- `school_count_total`
- `school_count_by_phase`
- `school_count_by_accessibility_band`
- `school_count_by_quality_status`
- `missing_ofsted_count`
- `generated_at`

## 21. Delivery Phases

### Phase 1: Scaffold

Deliver:

- Repository structure
- Python package
- CLI stub
- Config folders
- Empty pipeline modules
- `pyproject.toml`
- `Makefile`
- Test scaffold

Do not deliver:

- Real extraction logic
- Live data fetching
- Business scoring

### Phase 2: Pipeline Skeleton

Deliver:

- Function interfaces
- Pipeline orchestration
- Logging
- Stub data contracts

### Phase 3: Distance and Accessibility

Deliver:

- Haversine calculation
- Accessibility band assignment
- Proximity score implementation
- Unit tests

### Phase 4: Data Quality

Deliver:

- Quality flags
- Status logic
- Validation summary
- Unit tests

### Phase 5: KT19 Sample Run

Deliver:

- Sample inputs
- Local run command
- Output artifacts in `data/marts/`
- Manifest and summary

### Phase 6: Source Hardening

Deliver:

- Source documentation
- File versioning strategy
- Better traceability metadata

### Phase 7: Admissions Planning

Deliver:

- Reserved schema extensions
- Additional docs, not implementation

Each phase must end with integration into the single KT19 pipeline before the next delivery slice begins.

## 22. Multi-Agent Delivery Model

The multi-agent model for this repository should stay inside the agreed MVP scope.

### 22.1 Allowed Roles

- `spec-guardrail-agent`
- `contracts-agent`
- `runtime-orchestration-agent`
- `source-intake-agent`
- `school-normalization-agent`
- `quality-policy-agent`
- `artifact-publisher-agent`

### 22.2 Handoff Artifacts

Each agent handoff should use one or more of:

- updated code in owned files
- focused tests for the owned module
- config changes with explicit defaults
- doc updates for scope or source policy
- manifests or summaries when output shape changes

### 22.3 Integration Cadence

- Agents may work in parallel only when write ownership does not overlap
- Shared contract changes should be coordinated before stage-specific changes land
- The integration path remains linear: `extract -> transform -> validate -> publish`
- Human review should confirm that each handoff stays within KT19 MVP scope

### 22.4 Explicit Limits

- No runtime multi-agent orchestration in V1
- No future-platform agents for non-MVP features
- No expansion into admissions or catchment implementation under this delivery model

## 23. Testing Plan

### 23.1 Unit Tests

Required for:

- Distance calculation
- Accessibility band assignment
- Proximity score rules
- Data quality flag assignment
- Status derivation
- Phase mapping

Each agent-owned module should ship with focused tests for its own rules.

### 23.2 Integration Tests

Required for:

- End-to-end pipeline run on fixtures
- Output file creation
- Summary and manifest generation

The integration path should be owned by the `runtime-orchestration-agent`, with regression checks spanning the full KT19 flow.

### 23.3 Golden Fixture Tests

Recommended:

- A small fixed school fixture with known expected output for regression control

## 24. Logging Plan

Logging should be minimal, structured, and useful.

Log at least:

- Pipeline start and end
- Input file resolution
- Record counts after extraction
- Record counts after filtering
- Validation counts
- Output file paths

Avoid noisy per-record logging in normal runs.

During development, logs should also make stage handoffs visible:

- extract complete
- transform complete
- validate complete
- publish complete

## 25. Error Handling Plan

The pipeline should fail fast for:

- Missing required configuration
- Missing required input files
- Invalid schema assumptions that prevent processing

The pipeline should not fail fast for:

- Missing optional school fields on individual records
- Missing Ofsted data for some schools

Those should be flagged instead.

## 26. Performance Plan

Performance is not a V1 priority beyond reasonable local execution.

V1 targets:

- Run comfortably on a laptop
- Use file-based processing
- Keep dependencies light
- Prefer clarity over micro-optimisation

## 27. Risks

### 27.1 Product Risks

- Ambiguous definition of the search point
- Overstating what accessibility implies
- Letting `proximity_score` look more authoritative than it is

### 27.2 Data Risks

- Source schema drift
- Missing or delayed Ofsted fields
- Difficulty mapping establishment types cleanly

### 27.3 Engineering Risks

- Over-engineering before the first usable output exists
- Coupling source-specific assumptions too tightly into transforms
- Adding parquet and heavyweight dependencies too early
- Merge conflicts across adjacent modules
- Inconsistent assumptions across agents
- Premature abstraction caused by parallel work
- Unclear ownership at integration boundaries

## 28. Guardrails For Agents

Agents should follow these rules on this repository:

- Follow this plan strictly
- Keep code minimal and legible
- Do not invent data or source fields
- Do not implement admissions prediction
- Do not add a database
- Do not generalise the architecture prematurely
- Keep high-risk assumptions in config or docs
- Prefer explicit schemas and small functions
- Add tests when logic becomes non-trivial
- Stay within assigned file ownership unless an integration handoff requires otherwise
- Document assumptions at handoff points
- Avoid cross-cutting refactors unless explicitly requested
- Preserve the issue-first workflow for implementation tasks

## 29. MVP Work Packages

### Work Package 1: Scaffold

Scaffold the project from this plan.

Primary owner:

- `runtime-orchestration-agent`

Requirements:

- Create folder structure
- Create Python package under `src/`
- Add CLI entry point
- Add `configs/`
- Add empty pipeline modules
- Add `Makefile`
- Add `pyproject.toml`

Constraints:

- Do not implement business logic yet
- Keep code clean and minimal

Acceptance criteria:

- Project installs locally
- Folder structure matches this plan
- CLI runs as a stub

Integration checkpoint:

- The scaffold is merged into the single KT19 pipeline baseline

### Work Package 2: Pipeline Skeleton

Implement school pipeline skeleton.

Primary owners:

- `contracts-agent`
- `runtime-orchestration-agent`
- `source-intake-agent`
- `artifact-publisher-agent`

Requirements:

- Define function interfaces
- Add logging
- Add placeholder logic
- Wire `pipeline.run()`

Do not:

- Fetch real data yet
- Implement scoring yet

Integration checkpoint:

- The pipeline runs end to end with explicit stage interfaces

### Work Package 3: Distance and Accessibility

Add distance and accessibility logic.

Primary owner:

- `school-normalization-agent`

Requirements:

- Compute `distance_km`
- Assign `accessibility_band`
- Assign `proximity_score`
- Use config for thresholds
- Add unit tests

Integration checkpoint:

- Distance-derived fields appear in the KT19 pipeline without changing scope

### Work Package 4: Data Quality

Add data quality flags.

Primary owner:

- `quality-policy-agent`

Requirements:

- Implement `data_quality_status`
- Implement `data_quality_flags`
- Do not drop records

Integration checkpoint:

- Quality summaries appear in pipeline artifacts and remain deterministic

### Work Package 5: KT19 Sample Run

Add a `KT19` sample run.

Primary owners:

- `source-intake-agent`
- `artifact-publisher-agent`
- `runtime-orchestration-agent`

Requirements:

- Add sample input
- Run pipeline
- Generate outputs in `data/marts/`

Integration checkpoint:

- A reproducible KT19 run is documented and merged into the single MVP path

## 30. Golden Rule

Better to be correct and simple than clever and wrong.
