"""Extraction stage for the schools pipeline."""

import csv
import json
import logging
from datetime import datetime as _dt
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from london_data_model.settings import PROJECT_ROOT
from london_data_model.types import ExtractResult, PipelineContext, SourceDescriptor
from london_data_model.utils.geo import bng_to_wgs84, parse_bng_coordinate


LOGGER = logging.getLogger(__name__)


class OfficialSourceConfigError(ValueError):
    """Raised when official input configuration or file shape is invalid."""


def _resolve_input_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _load_json_array(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []

    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return payload
    raise ValueError("Expected a JSON array in {0}".format(path))


def _load_csv_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise OfficialSourceConfigError("Official input file not found: {0}".format(path))

    with path.open("r", encoding="cp1252", newline="") as handle:
        return list(csv.DictReader(handle))


def _load_csv_header(path: Path) -> List[str]:
    if not path.exists():
        raise OfficialSourceConfigError("Official input file not found: {0}".format(path))

    with path.open("r", encoding="cp1252", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
    if not header:
        raise OfficialSourceConfigError("Official input file has no header row: {0}".format(path))
    return header


def _load_records(path: Path, file_format: str) -> List[Dict[str, Any]]:
    if file_format == "json":
        return _load_json_array(path)
    if file_format == "csv":
        return _load_csv_rows(path)
    raise ValueError("Unsupported input format: {0}".format(file_format))


def _load_csv_rows_utf8(path: Path) -> List[Dict[str, Any]]:
    """Load CSV rows with UTF-8 encoding (used for DfE performance tables)."""
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _load_performance_table(
    input_cfg: Dict[str, Any],
    label: str,
) -> Tuple[Dict[str, Dict[str, Any]], Optional[SourceDescriptor]]:
    """Load a DfE performance table CSV and return a URN-keyed lookup dict.

    Returns (records_by_urn, source_descriptor). If the file is disabled or
    missing, returns ({}, None) — callers treat missing performance data as
    optional enrichment only.
    """
    if not input_cfg.get("enabled", False):
        return {}, None

    raw_path = str(input_cfg.get("path", ""))
    if not raw_path:
        LOGGER.warning("%s: no path configured, skipping", label)
        return {}, None

    path = _resolve_input_path(raw_path)
    column_map = input_cfg.get("column_map", {})

    if not path.exists():
        LOGGER.warning(
            "%s file not found at %s — download manually (see schools.yml for instructions)",
            label,
            path,
        )
        return {}, SourceDescriptor(
            source_name=label,
            source_type=label,
            source_path=str(path),
            status="missing",
            notes=["File not found; performance data will be absent from output."],
        )

    # DfE performance CSVs use UTF-8; fall back to cp1252 if needed
    try:
        rows = _load_csv_rows_utf8(path)
    except UnicodeDecodeError:
        LOGGER.warning("%s: UTF-8 decode failed, retrying with cp1252", label)
        rows = _load_csv_rows(path)

    records_by_urn: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        mapped = _map_performance_row(row, column_map)
        urn = str(mapped.get("school_urn") or "").strip()
        if urn:
            records_by_urn[urn] = mapped

    LOGGER.info("Loaded %d %s records", len(records_by_urn), label)
    return records_by_urn, SourceDescriptor(
        source_name=label,
        source_type=label,
        source_path=str(path),
        status="loaded",
        notes=["Loaded {0} school records.".format(len(records_by_urn))],
    )


def _map_performance_row(row: Dict[str, Any], column_map: Dict[str, str]) -> Dict[str, Any]:
    """Map raw DfE performance row through column_map, normalising suppressed values.

    DfE tables use sentinel strings for suppressed/unavailable data:
      SUPP  — suppressed (fewer than 5-10 pupils; treat as None)
      NE    — not entered / not applicable
      NA    — not available
      LOWCOV — low coverage
      NP    — not published
    Any of these become None in the output.
    """
    _SUPPRESSED = {"supp", "ne", "na", "lowcov", "np", ""}
    mapped: Dict[str, Any] = {}
    for target_field, source_field in column_map.items():
        raw = row.get(source_field)
        if raw is None or str(raw).strip().lower() in _SUPPRESSED:
            mapped[target_field] = None
        else:
            mapped[target_field] = str(raw).strip()
    return mapped


def _validate_column_map(name: str, column_map: Dict[str, str]) -> None:
    if not column_map:
        raise OfficialSourceConfigError(
            "Missing required column map for official source: {0}".format(name)
        )


def _validate_csv_headers(path: Path, column_map: Dict[str, str], source_name: str) -> None:
    header = set(_load_csv_header(path))
    missing_headers = sorted(
        source_field
        for source_field in column_map.values()
        if source_field not in header
    )
    if missing_headers:
        raise OfficialSourceConfigError(
            "{0} is missing required headers: {1}".format(source_name, ", ".join(missing_headers))
        )


def _build_address(record: Dict[str, Any]) -> str:
    parts = [
        record.get("address_line_1"),
        record.get("address_line_2"),
        record.get("address_line_3"),
        record.get("town"),
        record.get("county"),
    ]
    return ", ".join([str(part).strip() for part in parts if part not in (None, "")])


def _normalize_boolean(value: Any) -> Any:
    if value is None:
        return None

    text = str(value).strip().lower()
    if text in ("true", "yes", "1", "open"):
        return True
    if text in ("false", "no", "0", "closed"):
        return False
    return value


def _normalize_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _map_record(raw_record: Dict[str, Any], column_map: Dict[str, str]) -> Dict[str, Any]:
    mapped = {}
    for target_field, source_field in column_map.items():
        mapped[target_field] = raw_record.get(source_field)

    if "address" not in mapped and any(
        mapped.get(field)
        for field in ("address_line_1", "address_line_2", "address_line_3", "town", "county")
    ):
        mapped["address"] = _build_address(mapped)

    if "is_open" in mapped:
        mapped["is_open"] = _normalize_boolean(mapped["is_open"])
    if "latitude" in mapped:
        mapped["latitude"] = _normalize_float(mapped["latitude"])
    if "longitude" in mapped:
        mapped["longitude"] = _normalize_float(mapped["longitude"])

    # Convert BNG easting/northing to WGS84 lat/lon when lat/lon are absent
    if mapped.get("latitude") is None and mapped.get("longitude") is None:
        easting = parse_bng_coordinate(mapped.get("easting"))
        northing = parse_bng_coordinate(mapped.get("northing"))
        if easting is not None and northing is not None:
            try:
                mapped["latitude"], mapped["longitude"] = bng_to_wgs84(easting, northing)
            except Exception:
                pass

    return mapped


def _parse_inspection_date(date_str: Any) -> Optional[_dt]:
    """Parse an Ofsted inspection date string (DD/MM/YYYY) into a datetime."""
    if not date_str:
        return None
    try:
        return _dt.strptime(str(date_str).strip(), "%d/%m/%Y")
    except ValueError:
        return None


def _merge_records(
    school_records: List[Dict[str, Any]],
    ofsted_records: List[Dict[str, Any]],
    merge_key: str,
) -> List[Dict[str, Any]]:
    # Build ofsted_by_key keeping the row with the most recent inspection date per URN.
    # The CSV is ordered newest-first within each school's block; a naive dict comprehension
    # would keep the last (oldest) row per school.  We compare dates explicitly instead.
    ofsted_by_key: Dict[str, Dict[str, Any]] = {}
    for record in ofsted_records:
        key = record.get(merge_key)
        if key in (None, ""):
            continue
        existing = ofsted_by_key.get(key)
        if existing is None:
            ofsted_by_key[key] = record
        else:
            existing_date = _parse_inspection_date(existing.get("ofsted_inspection_date_latest"))
            new_date = _parse_inspection_date(record.get("ofsted_inspection_date_latest"))
            if new_date is not None and (existing_date is None or new_date > existing_date):
                ofsted_by_key[key] = record

    merged: List[Dict[str, Any]] = []
    for school_record in school_records:
        school_key = school_record.get(merge_key)
        merged_record = dict(school_record)
        if school_key in ofsted_by_key:
            merged_record.update(ofsted_by_key[school_key])
        merged.append(merged_record)
    return merged


def _merge_performance(
    school_records: List[Dict[str, Any]],
    performance_by_urn: Dict[str, Dict[str, Any]],
    merge_key: str = "school_urn",
) -> List[Dict[str, Any]]:
    """Enrich school records with performance data, matched by URN.

    Only performance fields (non-merge-key) are copied in; existing school
    fields are never overwritten, so GIAS/Ofsted data always wins on clashes.
    """
    merged: List[Dict[str, Any]] = []
    for record in school_records:
        urn = str(record.get(merge_key) or "").strip()
        perf = performance_by_urn.get(urn)
        if perf:
            enriched = dict(record)
            for k, v in perf.items():
                if k != merge_key:
                    enriched[k] = v
            merged.append(enriched)
        else:
            merged.append(record)
    return merged


def filter_by_la_codes(
    records: List[Dict[str, Any]],
    la_codes: List[int],
) -> List[Dict[str, Any]]:
    """Keep only records whose la_code matches the target set."""
    target = {str(code) for code in la_codes}
    return [r for r in records if str(r.get("la_code", "")).strip() in target]


def _extract_sample_input(context: PipelineContext) -> ExtractResult:
    sample_input = context.pipeline_config.get("sample_input", {})
    schools_path = _resolve_input_path(sample_input.get("schools_path"))
    ofsted_path = _resolve_input_path(sample_input.get("ofsted_path"))

    schools_records = _load_json_array(schools_path)
    _ = _load_json_array(ofsted_path)

    notes = [
        "Extraction currently loads checked-in sample input files only.",
        "Sample input files are placeholders for pipeline execution and documentation.",
    ]
    sources = [
        SourceDescriptor(
            source_name="schools_source",
            source_type="sample_schools",
            source_path=str(schools_path),
            status="loaded" if schools_path.exists() else "missing",
            notes=["Loaded JSON array from the configured sample input path."],
        ),
        SourceDescriptor(
            source_name="ofsted_source",
            source_type="sample_ofsted",
            source_path=str(ofsted_path),
            status="loaded" if ofsted_path.exists() else "missing",
            notes=["Loaded JSON array from the configured sample input path."],
        ),
    ]
    return ExtractResult(records=schools_records, sources=sources, notes=notes)


def load_official_records(
    pipeline_config: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[SourceDescriptor], List[str]]:
    """Load, map, and merge GIAS + Ofsted records without area filtering.

    Returns (merged_records, sources, notes).
    Used by both single-area extract and multi-borough orchestrator.
    """
    official_input = pipeline_config.get("official_input", {})
    schools_path = _resolve_input_path(official_input.get("schools_path"))
    ofsted_path = _resolve_input_path(official_input.get("ofsted_path"))
    schools_format = str(official_input.get("schools_format", "csv")).lower()
    ofsted_format = str(official_input.get("ofsted_format", "csv")).lower()
    schools_column_map = official_input.get("schools_column_map", {})
    ofsted_column_map = official_input.get("ofsted_column_map", {})
    merge_key = str(official_input.get("merge_key", "school_urn"))

    _validate_column_map("schools_column_map", schools_column_map)
    _validate_column_map("ofsted_column_map", ofsted_column_map)
    if schools_format == "csv":
        _validate_csv_headers(schools_path, schools_column_map, "schools_source")
    if ofsted_format == "csv":
        _validate_csv_headers(ofsted_path, ofsted_column_map, "ofsted_source")

    schools_records = [
        _map_record(record, schools_column_map) for record in _load_records(schools_path, schools_format)
    ]
    ofsted_records = [
        _map_record(record, ofsted_column_map) for record in _load_records(ofsted_path, ofsted_format)
    ]
    merged_records = _merge_records(schools_records, ofsted_records, merge_key=merge_key)

    notes = [
        "Extraction loads local official-source files via config.",
        "GIAS no longer carries published Ofsted rating fields for state-funded schools, so Ofsted data must be loaded separately.",
        "Column maps are starter defaults and may need adjusting to the exact current download headers.",
    ]
    sources = [
        SourceDescriptor(
            source_name="schools_source",
            source_type="gias_establishments",
            source_path=str(schools_path),
            status="loaded" if schools_path.exists() else "missing",
            notes=["Configured as the local DfE GIAS establishments file."],
        ),
        SourceDescriptor(
            source_name="ofsted_source",
            source_type="ofsted_state_funded_schools",
            source_path=str(ofsted_path),
            status="loaded" if ofsted_path.exists() else "missing",
            notes=["Configured as the local Ofsted state-funded schools inspection file export."],
        ),
    ]

    # ── KS4 (GCSE) merge ────────────────────────────────────────────────────
    ks4_by_urn, ks4_source = _load_performance_table(
        pipeline_config.get("ks4_input", {}), label="ks4"
    )
    if ks4_by_urn:
        merged_records = _merge_performance(merged_records, ks4_by_urn, merge_key=merge_key)
        notes.append("Merged KS4 performance data for {0} schools.".format(len(ks4_by_urn)))
    if ks4_source:
        sources.append(ks4_source)

    # ── KS5 (A-level) merge ─────────────────────────────────────────────────
    ks5_by_urn, ks5_source = _load_performance_table(
        pipeline_config.get("ks5_input", {}), label="ks5"
    )
    if ks5_by_urn:
        merged_records = _merge_performance(merged_records, ks5_by_urn, merge_key=merge_key)
        notes.append("Merged KS5 performance data for {0} schools.".format(len(ks5_by_urn)))
    if ks5_source:
        sources.append(ks5_source)

    return merged_records, sources, notes


def _extract_official_input(context: PipelineContext) -> ExtractResult:
    records, sources, notes = load_official_records(context.pipeline_config)

    if context.area_config.la_code is not None:
        records = filter_by_la_codes(records, [context.area_config.la_code])
        notes = list(notes) + [
            "Filtered to LA code {0} ({1} records).".format(
                context.area_config.la_code, len(records)
            )
        ]

    return ExtractResult(records=records, sources=sources, notes=notes)


def extract(context: PipelineContext) -> ExtractResult:
    """Load raw source inputs for a schools pipeline run."""
    LOGGER.info("Starting extract stage for area=%s run_id=%s", context.area, context.run_id)
    input_mode = str(context.pipeline_config.get("input_mode", "sample")).lower()
    if input_mode == "official":
        result = _extract_official_input(context)
    else:
        result = _extract_sample_input(context)

    LOGGER.info(
        "Extract stage completed with %s school records using input_mode=%s",
        len(result.records),
        input_mode,
    )
    return result
