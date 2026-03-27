"""Validation stage for the schools pipeline."""

from __future__ import annotations

import logging
from typing import Any, Dict

from london_data_model.types import PipelineContext


LOGGER = logging.getLogger(__name__)


def validate(transformed: Dict[str, Any], context: PipelineContext) -> Dict[str, Any]:
    """Validate transformed records and assign quality metadata."""
    LOGGER.info("Validate stub invoked for area=%s", context.area)
    return {
        "records": transformed.get("records", []),
        "quality_summary": {
            "complete": 0,
            "partial": 0,
            "poor": 0,
        },
    }
