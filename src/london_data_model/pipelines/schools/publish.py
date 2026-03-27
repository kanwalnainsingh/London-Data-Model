"""Publish stage for the schools pipeline."""

from __future__ import annotations

import logging
from typing import Any, Dict

from london_data_model.types import PipelineContext


LOGGER = logging.getLogger(__name__)


def publish(validated: Dict[str, Any], context: PipelineContext) -> Dict[str, Any]:
    """Publish pipeline outputs and manifests."""
    LOGGER.info("Publish stub invoked for area=%s", context.area)
    return {
        "output_files": [],
        "record_count": len(validated.get("records", [])),
    }
