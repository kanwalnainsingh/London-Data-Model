"""Transform stage for the schools pipeline."""

from __future__ import annotations

import logging
from typing import Any, Dict

from london_data_model.types import PipelineContext


LOGGER = logging.getLogger(__name__)


def transform(extracted: Dict[str, Any], context: PipelineContext) -> Dict[str, Any]:
    """Standardise records and apply derived fields."""
    LOGGER.info("Transform stub invoked for area=%s", context.area)
    return {
        "records": [],
        "metadata": extracted,
    }
