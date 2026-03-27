# Official Sources

This project is designed to use official local source files for real runs.

## Schools Source

Use the Department for Education's Get Information about Schools service.

- Service: Get Information about Schools (GIAS)
- Role in pipeline: establishment register and core school metadata
- Current public service reference: `https://get-information-schools.service.gov.uk/`
- Source note: the service says downloads are publicly available and updated daily

For local pipeline runs, place a downloaded establishments file at the configured local path in [`configs/pipeline/schools.yml`](../configs/pipeline/schools.yml).

## Ofsted Source

Use Ofsted's state-funded schools inspection data separately from GIAS.

- Publication: Five-Year Ofsted Inspection Data
- Current publication page: `https://www.gov.uk/government/publications/five-year-ofsted-inspection-data`
- Relevant remit: state-funded schools

Important:

- GIAS FAQ states that the Ofsted rating and date of last full inspection fields were removed from GIAS in January 2025.
- Because of that, the pipeline must load Ofsted data from a separate official file rather than relying on GIAS for those fields.

For local pipeline runs, export or prepare a local file from the current official Ofsted state-funded schools dataset and set the configured local path in [`configs/pipeline/schools.yml`](../configs/pipeline/schools.yml).

## Current Integration Model

The pipeline currently supports:

- local `csv` inputs for official source files
- local `json` inputs for sample runs
- a config-driven merge on `school_urn`

## Important Caveats

- The column maps in [`configs/pipeline/schools.yml`](../configs/pipeline/schools.yml) are starter defaults and may need adjusting to the exact headers in the current downloads.
- The official-source integration is local-file based by design. The normal pipeline does not fetch files live from the network.
- If the official schools file does not provide latitude and longitude directly, the pipeline will currently leave those fields empty rather than invent values.
- The current KT19 search point in [`configs/areas/kt19.yml`](../configs/areas/kt19.yml) is a provisional user-supplied district proxy for MVP testing. It should not be treated as an asserted official centroid.
