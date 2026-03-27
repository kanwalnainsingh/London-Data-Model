"""Top-level orchestration for the schools pipeline."""

import logging
import uuid
from pathlib import Path
from typing import Optional

from london_data_model.pipelines.schools.extract import extract
from london_data_model.pipelines.schools.publish import publish
from london_data_model.pipelines.schools.transform import transform
from london_data_model.pipelines.schools.validate import validate
from london_data_model.types import PipelineContext, PipelineResult
from london_data_model.utils.config import (
    load_area_config,
    load_pipeline_config,
    load_threshold_config,
)


logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
LOGGER = logging.getLogger(__name__)


def run(area: str = "KT19", config_path: Optional[Path] = None) -> PipelineResult:
    """Run the scaffolded schools pipeline."""
    area_config = load_area_config(area=area, config_path=config_path)
    pipeline_config = load_pipeline_config()
    threshold_config = load_threshold_config()
    context = PipelineContext(
        pipeline_name="schools",
        area=area,
        run_id=uuid.uuid4().hex[:12],
        config_path=config_path,
        area_config=area_config,
        pipeline_config=pipeline_config,
        threshold_config=threshold_config,
    )

    LOGGER.info(
        "Starting schools pipeline for area=%s run_id=%s config=%s",
        context.area,
        context.run_id,
        context.config_path,
    )
    extracted = extract(context)
    transformed = transform(extracted, context)
    validated = validate(transformed, context)
    published = publish(validated, context)
    LOGGER.info(
        "Finished schools pipeline for area=%s run_id=%s artifacts=%s",
        context.area,
        context.run_id,
        published.output_files,
    )

    return PipelineResult(
        pipeline_name=context.pipeline_name,
        area=context.area,
        status="success",
        message=(
            "Schools pipeline skeleton completed for area={0}. "
            "Artifacts written to data/marts and data/manifests."
        ).format(context.area),
        artifacts={
            "run_id": context.run_id,
            "record_count": published.record_count,
            "output_files": published.output_files,
            "quality_summary": validated.quality_summary,
        },
    )
