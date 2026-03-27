"""Transform stage for the schools pipeline."""

import logging

from london_data_model.types import ExtractResult, PipelineContext, TransformResult


LOGGER = logging.getLogger(__name__)


def transform(extracted: ExtractResult, context: PipelineContext) -> TransformResult:
    """Standardise records and apply derived fields."""
    LOGGER.info(
        "Starting transform stage for area=%s with %s extracted records",
        context.area,
        len(extracted.records),
    )

    notes = list(extracted.notes)
    notes.append("No school records transformed yet; business rules remain stubbed.")

    LOGGER.info("Transform stage completed with %s transformed records", 0)
    return TransformResult(records=[], excluded_record_count=0, notes=notes)
