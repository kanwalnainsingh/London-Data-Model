"""Extraction stage for the schools pipeline."""

import logging

from london_data_model.types import ExtractResult, PipelineContext, SourceDescriptor


LOGGER = logging.getLogger(__name__)


def extract(context: PipelineContext) -> ExtractResult:
    """Load raw source inputs for a schools pipeline run."""
    LOGGER.info("Starting extract stage for area=%s run_id=%s", context.area, context.run_id)

    notes = [
        "No source ingestion is implemented yet.",
        "Extraction returns placeholder source descriptors only.",
    ]
    sources = [
        SourceDescriptor(
            source_name="schools_source",
            source_type="official_schools",
            source_path=None,
            status="not_loaded",
            notes=["Waiting for source selection in a later task."],
        ),
        SourceDescriptor(
            source_name="ofsted_source",
            source_type="official_ofsted",
            source_path=None,
            status="not_loaded",
            notes=["Waiting for source selection in a later task."],
        ),
    ]

    LOGGER.info("Extract stage completed with %s source placeholders", len(sources))
    return ExtractResult(records=[], sources=sources, notes=notes)
