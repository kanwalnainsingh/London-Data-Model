"""Extraction stage for the schools pipeline."""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List

from london_data_model.settings import PROJECT_ROOT
from london_data_model.types import ExtractResult, PipelineContext, SourceDescriptor


LOGGER = logging.getLogger(__name__)


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


def extract(context: PipelineContext) -> ExtractResult:
    """Load raw source inputs for a schools pipeline run."""
    LOGGER.info("Starting extract stage for area=%s run_id=%s", context.area, context.run_id)

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

    LOGGER.info(
        "Extract stage completed with %s school records from sample input",
        len(schools_records),
    )
    return ExtractResult(records=schools_records, sources=sources, notes=notes)
