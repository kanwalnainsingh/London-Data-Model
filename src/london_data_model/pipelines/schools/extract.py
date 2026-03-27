"""Extraction stage for the schools pipeline."""

from __future__ import annotations

import logging
from typing import Any, Dict

from london_data_model.types import PipelineContext


LOGGER = logging.getLogger(__name__)


def extract(context: PipelineContext) -> Dict[str, Any]:
    """Load raw source inputs for a schools pipeline run."""
    LOGGER.info("Extract stub invoked for area=%s", context.area)
    return {
        "schools_source": None,
        "ofsted_source": None,
        "context": context,
    }
