"""Top-level orchestration for the schools pipeline."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from london_data_model.pipelines.schools.extract import extract
from london_data_model.pipelines.schools.publish import publish
from london_data_model.pipelines.schools.transform import transform
from london_data_model.pipelines.schools.validate import validate
from london_data_model.types import PipelineContext, PipelineResult


logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
LOGGER = logging.getLogger(__name__)


def run(area: str = "KT19", config_path: Optional[Path] = None) -> PipelineResult:
    """Run the scaffolded schools pipeline."""
    context = PipelineContext(
        pipeline_name="schools",
        area=area,
        config_path=config_path,
    )

    LOGGER.info("Starting schools pipeline for area=%s", context.area)
    extracted = extract(context)
    transformed = transform(extracted, context)
    validated = validate(transformed, context)
    published = publish(validated, context)
    LOGGER.info("Finished schools pipeline for area=%s", context.area)

    return PipelineResult(
        pipeline_name=context.pipeline_name,
        area=context.area,
        status="stub",
        message=f"Schools pipeline scaffold is wired for area={context.area}.",
        artifacts=published,
    )
