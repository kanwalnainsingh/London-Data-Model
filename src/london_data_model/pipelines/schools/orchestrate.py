"""Multi-borough orchestration for the schools pipeline."""

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from london_data_model.pipelines.schools.extract import (
    filter_by_la_codes,
    load_official_records,
)
from london_data_model.pipelines.schools.publish import publish
from london_data_model.pipelines.schools.transform import (
    OFSTED_REPORT_BASE_URL,
    _OFSTED_RATING_MAP,
    _normalize_ofsted_safeguarding,
    _normalize_ofsted_subrating,
    assign_accessibility_band,
    calculate_distance_km,
    calculate_proximity_score,
    is_mainstream_establishment,
    normalize_open_status,
    normalize_phase,
    transform,
)
from london_data_model.pipelines.schools.fetch import fetch as _fetch_sources
from london_data_model.pipelines.schools.validate import validate
from london_data_model.pipelines.schools.pipeline import (
    _build_pipeline_config,
    _preflight_official_mode,
)
from london_data_model.settings import DOCS_DATA_DIR, PROJECT_ROOT
from london_data_model.types import (
    AreaConfig,
    ExtractResult,
    PipelineContext,
    PipelineResult,
    PublishResult,
    SourceProvenance,
    ValidateResult,
)
from london_data_model.utils.config import (
    load_borough_configs,
    load_pipeline_config,
    load_threshold_config,
)


LOGGER = logging.getLogger(__name__)

# LA codes for counties adjacent to Greater London whose schools may be
# closer to a user's postcode than their own borough's schools.
FRINGE_LA_CODES: set = {"936", "919", "886", "881", "867"}  # Surrey, Herts, Kent, Essex, Berkshire

# Approximate centroid of Greater London. Fringe schools within FRINGE_MAX_KM
# of this point are added to london-schools.json after the 33 borough runs.
# The outer London boundary is ~15-24 km from this point; 30 km captures a
# meaningful fringe zone (5-15 km into Surrey / Kent / Essex / Hertfordshire).
GREATER_LONDON_LAT = 51.5074
GREATER_LONDON_LON = -0.1278
FRINGE_MAX_KM = 30.0


def _add_fringe_schools(
    all_records: List[Dict[str, Any]],
    schools_by_urn: Dict[str, Any],
    threshold_config: Dict[str, Any],
) -> int:
    """Capture open mainstream schools in counties adjacent to Greater London.

    After the 33 borough LA-code runs there are valid schools in Surrey,
    Hertfordshire, Kent, Essex and Berkshire that sit within a few km of the
    outer London boundary.  This function adds those schools to the combined
    london-schools.json so that users who live near the boundary see them in
    postcode searches.

    Returns the number of fringe schools added.
    """
    added = 0
    for raw in all_records:
        la = str(raw.get("la_code", "")).strip()
        if la not in FRINGE_LA_CODES:
            continue
        urn = str(raw.get("school_urn", ""))
        if urn in schools_by_urn:
            continue  # already captured in a borough run

        lat = raw.get("latitude")
        lon = raw.get("longitude")
        try:
            lat_f = float(lat)
            lon_f = float(lon)
        except (TypeError, ValueError):
            continue

        dist = calculate_distance_km(GREATER_LONDON_LAT, GREATER_LONDON_LON, lat_f, lon_f)
        if dist is None or dist > FRINGE_MAX_KM:
            continue

        phase = normalize_phase(raw.get("phase"))
        if phase not in ("primary", "secondary", "all_through"):
            continue
        if normalize_open_status(raw.get("is_open")) is not True:
            continue
        if not is_mainstream_establishment(str(raw.get("establishment_type", ""))):
            continue

        raw_rating = str(raw.get("ofsted_rating_latest") or "").strip()
        ofsted_rating = _OFSTED_RATING_MAP.get(raw_rating, raw.get("ofsted_rating_latest"))
        ofsted_date = raw.get("ofsted_inspection_date_latest")

        flags = []
        if not ofsted_rating:
            flags.append("missing_ofsted_rating")
        if not ofsted_date:
            flags.append("missing_inspection_date")

        schools_by_urn[urn] = {
            "school_name": str(raw.get("school_name", "")),
            "school_urn": urn,
            "address": str(raw.get("address", "")),
            "postcode": raw.get("postcode"),
            "latitude": lat_f,
            "longitude": lon_f,
            "phase": phase,
            "establishment_type": str(raw.get("establishment_type", "")),
            "is_open": True,
            "distance_km": dist,
            "accessibility_band": assign_accessibility_band(phase, dist, threshold_config),
            "proximity_score": calculate_proximity_score(phase, dist, threshold_config),
            "ofsted_rating_latest": ofsted_rating,
            "ofsted_inspection_date_latest": ofsted_date,
            "ofsted_report_url": (
                raw.get("ofsted_report_url")
                or (OFSTED_REPORT_BASE_URL + urn if urn else None)
            ),
            "ofsted_quality_of_education": _normalize_ofsted_subrating(raw.get("ofsted_quality_of_education")),
            "ofsted_leadership_management": _normalize_ofsted_subrating(raw.get("ofsted_leadership_management")),
            "ofsted_personal_development": _normalize_ofsted_subrating(raw.get("ofsted_personal_development")),
            "ofsted_behaviour_attitudes": _normalize_ofsted_subrating(raw.get("ofsted_behaviour_attitudes")),
            "ofsted_sixth_form": _normalize_ofsted_subrating(raw.get("ofsted_sixth_form")),
            "ofsted_safeguarding": _normalize_ofsted_safeguarding(raw.get("ofsted_safeguarding")),
            "ks4_progress8": None,
            "ks4_attainment8": None,
            "ks4_strong_pass_pct": None,
            "ks4_standard_pass_pct": None,
            "ks5_avg_point_score": None,
            "ks5_a_star_a_pct": None,
            "ks5_pass_rate_pct": None,
            "ks5_entries": None,
            "data_quality_status": "complete" if not flags else "partial",
            "data_quality_flags": flags,
        }
        added += 1

    return added


def _write_sources_json(
    provenances: List[SourceProvenance],
    pipeline_config: Dict[str, Any],
) -> None:
    """Write docs/data/sources.json with full data-lineage metadata for all sources.

    The file records:
    - What every raw data source is (label, publisher, licence, update frequency)
    - Where the file was fetched from (URL)
    - When the file was last written to disk (proxy for fetch time)
    - How large the file is
    - What date the source data represents (extracted from URL/filename where possible)
    - Which fields from each source the pipeline actually uses

    This lets the GitHub Pages dashboard show parents exactly how fresh the
    underlying data is and where it came from.
    """
    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_mode": pipeline_config.get("input_mode", "sample"),
        "sources": [p.to_dict() for p in provenances],
    }
    path = DOCS_DATA_DIR / "sources.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    LOGGER.info("Written data-lineage manifest: %s (%d sources)", path, len(provenances))


def _publish_london_index(
    borough_results: List[Tuple[AreaConfig, PublishResult, ValidateResult]],
    pipeline_config: Dict[str, Any],
    all_records: Optional[List[Dict[str, Any]]] = None,
    threshold_config: Optional[Dict[str, Any]] = None,
    provenances: Optional[List[SourceProvenance]] = None,
) -> None:
    """Write London-wide index and combined schools JSON to docs/data/."""
    areas = []
    total_schools = 0
    total_quality: Dict[str, int] = {"complete": 0, "partial": 0, "poor": 0}
    # Deduplicate schools across borough runs by URN; keep the closest entry.
    schools_by_urn: Dict[str, Any] = {}

    for borough_config, published, validated in borough_results:
        areas.append({
            "area_id": borough_config.area_id,
            "label": borough_config.label,
            "area_type": "borough",
            "la_code": borough_config.la_code,
            "school_count": published.record_count,
            "quality_counts": validated.quality_summary,
            "status_url": "./data/{0}-status.json".format(borough_config.area_id),
        })
        total_schools += published.record_count
        for key in total_quality:
            total_quality[key] += validated.quality_summary.get(key, 0)

        for record in validated.records:
            d = record.to_dict()
            urn = d.get("school_urn", "")
            existing = schools_by_urn.get(urn)
            if existing is None:
                schools_by_urn[urn] = d
            else:
                # Keep the entry with the smaller distance (school closer to its borough centre)
                existing_dist = existing.get("distance_km") or float("inf")
                new_dist = d.get("distance_km") or float("inf")
                if new_dist < existing_dist:
                    schools_by_urn[urn] = d

    # Level 2: add schools from fringe counties adjacent to Greater London
    if all_records and threshold_config:
        fringe_added = _add_fringe_schools(all_records, schools_by_urn, threshold_config)
        LOGGER.info("Added %d fringe schools from adjacent counties (Surrey/Herts/Kent/Essex/Berkshire)", fringe_added)

    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)

    index_payload = {
        "region_id": "london",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_mode": pipeline_config.get("input_mode", "sample"),
        "borough_count": len(areas),
        "school_count_total": total_schools,
        "quality_counts": total_quality,
        "areas": sorted(areas, key=lambda a: a["area_id"]),
    }
    index_path = DOCS_DATA_DIR / "london-index.json"
    index_path.write_text(json.dumps(index_payload, indent=2), encoding="utf-8")
    LOGGER.info(
        "Published London index: %s (%d boroughs, %d schools)",
        index_path,
        len(areas),
        total_schools,
    )

    combined_schools = sorted(schools_by_urn.values(), key=lambda s: s.get("school_name", ""))
    schools_path = DOCS_DATA_DIR / "london-schools.json"
    schools_path.write_text(
        json.dumps(combined_schools, separators=(",", ":")), encoding="utf-8"
    )
    LOGGER.info(
        "Published London schools: %s (%d unique schools)",
        schools_path,
        len(combined_schools),
    )

    # Write data-lineage manifest with source provenance
    if provenances:
        _write_sources_json(provenances, pipeline_config)


def run_london(
    boroughs: Optional[str] = None,
    config_path: Optional[Path] = None,
    input_mode: Optional[str] = None,
) -> PipelineResult:
    """Run the schools pipeline for multiple London boroughs.

    Args:
        boroughs: Optional comma-separated borough IDs. If None, all 33 boroughs run.
        config_path: Optional path to a custom area config (unused in multi-borough mode).
        input_mode: Optional override for input mode (sample or official).
    """
    pipeline_config = _build_pipeline_config(load_pipeline_config(), input_mode=input_mode)
    threshold_config = load_threshold_config()
    _preflight_official_mode(pipeline_config)

    borough_ids = (
        [b.strip() for b in boroughs.split(",") if b.strip()]
        if boroughs
        else None
    )
    borough_configs = load_borough_configs(borough_ids)

    LOGGER.info(
        "Starting London multi-borough run for %d boroughs (input_mode=%s)",
        len(borough_configs),
        pipeline_config.get("input_mode", "sample"),
    )

    # Load GIAS + Ofsted data once for all boroughs
    input_mode_effective = str(pipeline_config.get("input_mode", "sample")).lower()
    fetch_provenances = []
    if input_mode_effective == "official":
        # Run fetch (skips if files already exist) to collect per-source provenance
        official_input = pipeline_config.get("official_input", {})
        schools_dest = PROJECT_ROOT / str(official_input.get("schools_path", "data/raw/gias_establishments.csv"))
        ofsted_dest = PROJECT_ROOT / str(official_input.get("ofsted_path", "data/raw/ofsted_state_funded_schools.csv"))
        try:
            fetch_result = _fetch_sources(pipeline_config, schools_dest, ofsted_dest)
            fetch_provenances = fetch_result.provenances
        except Exception as exc:  # never let provenance failure abort a run
            LOGGER.warning("Could not build source provenance: %s", exc)

        all_records, sources, base_notes = load_official_records(pipeline_config)
        LOGGER.info("Loaded %d total records from official sources", len(all_records))
    else:
        all_records, sources, base_notes = [], [], [
            "Sample mode: no records loaded for multi-borough run.",
        ]

    borough_results: List[Tuple[AreaConfig, PublishResult, ValidateResult]] = []
    total_schools = 0

    for borough_config in borough_configs:
        if borough_config.la_code is not None and all_records:
            filtered = filter_by_la_codes(all_records, [borough_config.la_code])
        else:
            filtered = list(all_records)

        LOGGER.info(
            "Borough %s (LA %s): %d records",
            borough_config.area_id,
            borough_config.la_code,
            len(filtered),
        )

        context = PipelineContext(
            pipeline_name="schools",
            area=borough_config.area_id,
            run_id=uuid.uuid4().hex[:12],
            started_at=datetime.now(timezone.utc),
            config_path=config_path,
            area_config=borough_config,
            pipeline_config=pipeline_config,
            threshold_config=threshold_config,
        )

        filter_note = (
            "Filtered to LA code {0} ({1} records).".format(
                borough_config.la_code, len(filtered)
            )
            if borough_config.la_code is not None
            else "No LA code filter applied."
        )
        extracted = ExtractResult(
            records=filtered,
            sources=sources,
            notes=list(base_notes) + [filter_note],
        )
        transformed = transform(extracted, context)
        validated = validate(transformed, context)
        published = publish(extracted, transformed, validated, context)

        borough_results.append((borough_config, published, validated))
        total_schools += published.record_count

    _publish_london_index(
        borough_results,
        pipeline_config,
        all_records=all_records if input_mode_effective == "official" else None,
        threshold_config=threshold_config,
        provenances=fetch_provenances if fetch_provenances else None,
    )

    LOGGER.info(
        "London multi-borough run complete: %d boroughs, %d total schools",
        len(borough_results),
        total_schools,
    )

    return PipelineResult(
        pipeline_name="schools",
        area="london",
        status="success",
        message=(
            "London schools pipeline completed for {0} boroughs. "
            "{1} total schools published."
        ).format(len(borough_results), total_schools),
        artifacts={
            "borough_count": len(borough_results),
            "total_schools": total_schools,
        },
    )
