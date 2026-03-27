"""Transform stage for the schools pipeline."""

import logging
import math
from typing import Any, Dict, Optional

from london_data_model.types import ExtractResult, PipelineContext, SchoolRecord, TransformResult


LOGGER = logging.getLogger(__name__)
EARTH_RADIUS_KM = 6371.0


def normalize_phase(phase: Optional[str]) -> Optional[str]:
    if phase is None:
        return None

    normalized = str(phase).strip().lower().replace("-", " ").replace("_", " ")
    if normalized == "all through":
        return "all_through"
    if normalized in ("primary", "secondary"):
        return normalized
    return None


def resolve_threshold_profile(phase: Optional[str], threshold_config: Dict[str, Any]) -> Optional[str]:
    normalized = normalize_phase(phase)
    if normalized in ("primary", "secondary"):
        return normalized
    if normalized == "all_through":
        profile = threshold_config.get("all_through", {}).get("threshold_profile")
        return str(profile) if profile else None
    return None


def resolve_proximity_profile(phase: Optional[str], threshold_config: Dict[str, Any]) -> Optional[str]:
    normalized = normalize_phase(phase)
    if normalized in ("primary", "secondary"):
        return normalized
    if normalized == "all_through":
        profile = threshold_config.get("all_through", {}).get("proximity_profile")
        return str(profile) if profile else None
    return None


def calculate_distance_km(
    origin_latitude: Optional[float],
    origin_longitude: Optional[float],
    school_latitude: Optional[float],
    school_longitude: Optional[float],
) -> Optional[float]:
    if None in (origin_latitude, origin_longitude, school_latitude, school_longitude):
        return None

    origin_lat_rad = math.radians(origin_latitude)
    school_lat_rad = math.radians(school_latitude)
    lat_diff = math.radians(school_latitude - origin_latitude)
    lon_diff = math.radians(school_longitude - origin_longitude)

    haversine = (
        math.sin(lat_diff / 2) ** 2
        + math.cos(origin_lat_rad) * math.cos(school_lat_rad) * math.sin(lon_diff / 2) ** 2
    )
    distance = 2 * EARTH_RADIUS_KM * math.atan2(math.sqrt(haversine), math.sqrt(1 - haversine))
    return round(distance, 3)


def assign_accessibility_band(
    phase: Optional[str],
    distance_km: Optional[float],
    threshold_config: Dict[str, Any],
) -> Optional[str]:
    if distance_km is None:
        return None

    profile = resolve_threshold_profile(phase, threshold_config)
    if profile is None:
        return None

    thresholds = threshold_config.get(profile, {})
    if distance_km <= thresholds["very_close_max_km"]:
        return "very_close"
    if distance_km <= thresholds["close_max_km"]:
        return "close"
    if distance_km <= thresholds["moderate_max_km"]:
        return "moderate"
    return "far"


def calculate_proximity_score(
    phase: Optional[str],
    distance_km: Optional[float],
    threshold_config: Dict[str, Any],
) -> Optional[float]:
    if distance_km is None:
        return None

    profile = resolve_proximity_profile(phase, threshold_config)
    if profile is None:
        return None

    score_zero_at_km = threshold_config.get(profile, {}).get("proximity_zero_at_km")
    if not score_zero_at_km:
        return None

    bounded_distance = min(distance_km, float(score_zero_at_km))
    score = 100 * (1 - (bounded_distance / float(score_zero_at_km)))
    return round(max(score, 0.0), 2)


def build_school_record(raw_record: Dict[str, Any], context: PipelineContext) -> SchoolRecord:
    phase = normalize_phase(raw_record.get("phase"))
    distance_km = calculate_distance_km(
        origin_latitude=context.area_config.latitude,
        origin_longitude=context.area_config.longitude,
        school_latitude=raw_record.get("latitude"),
        school_longitude=raw_record.get("longitude"),
    )
    return SchoolRecord(
        school_name=str(raw_record.get("school_name", "")),
        school_urn=str(raw_record.get("school_urn", "")),
        address=str(raw_record.get("address", "")),
        postcode=raw_record.get("postcode"),
        latitude=raw_record.get("latitude"),
        longitude=raw_record.get("longitude"),
        phase=phase,
        establishment_type=str(raw_record.get("establishment_type", "")),
        is_open=bool(raw_record.get("is_open", True)),
        distance_km=distance_km,
        accessibility_band=assign_accessibility_band(
            phase=phase,
            distance_km=distance_km,
            threshold_config=context.threshold_config,
        ),
        proximity_score=calculate_proximity_score(
            phase=phase,
            distance_km=distance_km,
            threshold_config=context.threshold_config,
        ),
        ofsted_rating_latest=raw_record.get("ofsted_rating_latest"),
        ofsted_inspection_date_latest=raw_record.get("ofsted_inspection_date_latest"),
        ofsted_report_url=raw_record.get("ofsted_report_url"),
    )


def transform(extracted: ExtractResult, context: PipelineContext) -> TransformResult:
    """Standardise records and apply derived fields."""
    LOGGER.info(
        "Starting transform stage for area=%s with %s extracted records",
        context.area,
        len(extracted.records),
    )

    records = [build_school_record(raw_record=record, context=context) for record in extracted.records]
    notes = list(extracted.notes)
    notes.append(
        "Distance, accessibility band, and proximity score are now applied when coordinates are present."
    )

    LOGGER.info("Transform stage completed with %s transformed records", len(records))
    return TransformResult(records=records, excluded_record_count=0, notes=notes)
