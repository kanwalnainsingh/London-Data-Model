"""Transform stage for the schools pipeline."""

import logging
import math
from typing import Any, Dict, Optional

from london_data_model.types import ExtractResult, PipelineContext, SchoolRecord, TransformResult


LOGGER = logging.getLogger(__name__)
EARTH_RADIUS_KM = 6371.0

# DfE performance tables use these strings for suppressed / not-available data
_DFE_SUPPRESSED = frozenset({"supp", "ne", "na", "lowcov", "np", ""})

_OFSTED_RATING_MAP = {
    "1": "Outstanding",
    "2": "Good",
    "3": "Requires improvement",
    "4": "Inadequate",
}
EXCLUDED_ESTABLISHMENT_TERMS = (
    "independent",
    "private",
    "special",
    "pupil referral",
    "hospital",
    "college",
    "further education",
    "sixth form college",
)


def _normalize_dfe_number(value: Any) -> Optional[float]:
    """Convert a DfE performance table value to float, returning None for suppressed entries."""
    if value is None:
        return None
    text = str(value).strip()
    if text.lower() in _DFE_SUPPRESSED:
        return None
    try:
        return float(text)
    except (ValueError, TypeError):
        return None


def _normalize_dfe_int(value: Any) -> Optional[int]:
    """Convert a DfE performance table value to int, returning None for suppressed entries."""
    num = _normalize_dfe_number(value)
    return int(round(num)) if num is not None else None


def normalize_phase(phase: Optional[str]) -> Optional[str]:
    if phase is None:
        return None

    normalized = str(phase).strip().lower().replace("-", " ").replace("_", " ")
    if normalized == "all through" or ("primary" in normalized and "secondary" in normalized):
        return "all_through"
    if normalized == "primary":
        return "primary"
    if normalized == "secondary":
        return normalized
    return None


def normalize_open_status(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None:
        return None

    normalized = str(value).strip().lower()
    if normalized.startswith("open"):
        return True
    if normalized.startswith("closed"):
        return False
    return None


def is_mainstream_establishment(establishment_type: Optional[str]) -> bool:
    if not establishment_type:
        return True

    normalized = str(establishment_type).strip().lower()
    return not any(term in normalized for term in EXCLUDED_ESTABLISHMENT_TERMS)


def is_in_scope_school(record: SchoolRecord) -> bool:
    if record.phase not in ("primary", "secondary", "all_through"):
        return False
    if record.is_open is not True:
        return False
    if not is_mainstream_establishment(record.establishment_type):
        return False
    return True


def is_within_max_distance(record: SchoolRecord, threshold_config: Dict[str, Any]) -> bool:
    if record.distance_km is None:
        # No coordinates → distance unknown; exclude rather than pass through.
        # This prevents online/virtual schools with no physical address from appearing.
        return False
    profile = resolve_threshold_profile(record.phase, threshold_config)
    if profile is None:
        return True
    max_km = threshold_config.get(profile, {}).get("max_distance_km")
    if max_km is None:
        return True
    return record.distance_km <= float(max_km)


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
    is_open = normalize_open_status(raw_record.get("is_open"))
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
        is_open=is_open if is_open is not None else False,
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
        ofsted_rating_latest=_OFSTED_RATING_MAP.get(
            str(raw_record.get("ofsted_rating_latest") or "").strip(),
            raw_record.get("ofsted_rating_latest"),
        ),
        ofsted_inspection_date_latest=raw_record.get("ofsted_inspection_date_latest"),
        ofsted_report_url=raw_record.get("ofsted_report_url"),
        # KS4 (GCSE) — present only when ks4_input is enabled and URN matched
        ks4_progress8=_normalize_dfe_number(raw_record.get("ks4_progress8")),
        ks4_attainment8=_normalize_dfe_number(raw_record.get("ks4_attainment8")),
        ks4_strong_pass_pct=_normalize_dfe_number(raw_record.get("ks4_strong_pass_pct")),
        ks4_standard_pass_pct=_normalize_dfe_number(raw_record.get("ks4_standard_pass_pct")),
        # KS5 (A-level) — present only when ks5_input is enabled and URN matched
        ks5_avg_point_score=_normalize_dfe_number(raw_record.get("ks5_avg_point_score")),
        ks5_a_star_a_pct=_normalize_dfe_number(raw_record.get("ks5_a_star_a_pct")),
        ks5_pass_rate_pct=_normalize_dfe_number(raw_record.get("ks5_pass_rate_pct")),
        ks5_entries=_normalize_dfe_int(raw_record.get("ks5_entries")),
    )


def transform(extracted: ExtractResult, context: PipelineContext) -> TransformResult:
    """Standardise records and apply derived fields."""
    LOGGER.info(
        "Starting transform stage for area=%s with %s extracted records",
        context.area,
        len(extracted.records),
    )

    records = []
    excluded_record_count = 0
    for raw_record in extracted.records:
        record = build_school_record(raw_record=raw_record, context=context)
        if is_in_scope_school(record) and is_within_max_distance(record, context.threshold_config):
            records.append(record)
        else:
            excluded_record_count += 1

    notes = list(extracted.notes)
    notes.append(
        "Distance, accessibility band, and proximity score are now applied when coordinates are present."
    )
    notes.append(
        "Transform applies V1 scope filters for mainstream, open primary/secondary/all-through schools."
    )

    LOGGER.info(
        "Transform stage completed with %s included records and %s excluded records",
        len(records),
        excluded_record_count,
    )
    return TransformResult(records=records, excluded_record_count=excluded_record_count, notes=notes)
