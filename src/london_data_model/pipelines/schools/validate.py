"""Validation stage for the schools pipeline."""

import logging
from typing import Dict, List

from london_data_model.types import PipelineContext, SchoolRecord, TransformResult, ValidateResult


LOGGER = logging.getLogger(__name__)
CRITICAL_FLAGS = {"missing_coordinates", "invalid_phase"}


def derive_data_quality_flags(record: SchoolRecord) -> List[str]:
    flags: List[str] = []

    if not record.address:
        flags.append("missing_address")
    if not record.postcode:
        flags.append("missing_postcode")
    if record.latitude is None or record.longitude is None:
        flags.append("missing_coordinates")
    if record.phase not in ("primary", "secondary", "all_through"):
        flags.append("invalid_phase")
    if not record.ofsted_rating_latest:
        flags.append("missing_ofsted_rating")
    if not record.ofsted_inspection_date_latest:
        flags.append("missing_inspection_date")
    if not record.ofsted_report_url:
        flags.append("missing_ofsted_report_url")
    if record.distance_km is None and record.latitude is not None and record.longitude is not None:
        flags.append("distance_estimated")

    return flags


def derive_data_quality_status(flags: List[str]) -> str:
    if not flags:
        return "complete"
    if any(flag in CRITICAL_FLAGS for flag in flags):
        return "poor"
    return "partial"


def apply_data_quality(record: SchoolRecord) -> SchoolRecord:
    flags = derive_data_quality_flags(record)
    record.data_quality_flags = flags
    record.data_quality_status = derive_data_quality_status(flags)
    return record


def summarize_quality(records: List[SchoolRecord]) -> Dict[str, int]:
    summary = {"complete": 0, "partial": 0, "poor": 0}
    for record in records:
        summary[record.data_quality_status] += 1
    return summary


def validate(transformed: TransformResult, context: PipelineContext) -> ValidateResult:
    """Validate transformed records and assign quality metadata."""
    LOGGER.info(
        "Starting validate stage for area=%s with %s transformed records",
        context.area,
        len(transformed.records),
    )

    records = [apply_data_quality(record) for record in transformed.records]
    quality_summary = summarize_quality(records)
    notes = list(transformed.notes)
    notes.append("Validation applies deterministic data quality flags and status mapping.")

    LOGGER.info("Validate stage completed with quality_summary=%s", quality_summary)
    return ValidateResult(
        records=records,
        quality_summary=quality_summary,
        notes=notes,
    )
