"""Validation stage for the schools pipeline."""

import logging

from london_data_model.types import PipelineContext, TransformResult, ValidateResult


LOGGER = logging.getLogger(__name__)


def validate(transformed: TransformResult, context: PipelineContext) -> ValidateResult:
    """Validate transformed records and assign quality metadata."""
    LOGGER.info(
        "Starting validate stage for area=%s with %s transformed records",
        context.area,
        len(transformed.records),
    )

    quality_summary = {
        "complete": 0,
        "partial": len(transformed.records),
        "poor": 0,
    }
    notes = list(transformed.notes)
    notes.append("Validation placeholders applied; detailed quality rules come in Task 4.")

    LOGGER.info("Validate stage completed with quality_summary=%s", quality_summary)
    return ValidateResult(
        records=list(transformed.records),
        quality_summary=quality_summary,
        notes=notes,
    )
