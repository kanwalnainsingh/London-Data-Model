"""Top-level orchestration for the schools pipeline."""

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from london_data_model.pipelines.schools.extract import OfficialSourceConfigError
from london_data_model.pipelines.schools.extract import extract
from london_data_model.pipelines.schools.fetch import FetchError
from london_data_model.pipelines.schools.fetch import fetch
from london_data_model.pipelines.schools.publish import publish
from london_data_model.pipelines.schools.transform import transform
from london_data_model.pipelines.schools.validate import validate
from london_data_model.settings import PROJECT_ROOT
from london_data_model.types import PipelineContext, PipelineResult
from london_data_model.utils.config import (
    load_area_config,
    load_pipeline_config,
    load_threshold_config,
)


logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
LOGGER = logging.getLogger(__name__)


def _resolve_input_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def _build_pipeline_config(base_config: Dict[str, object], input_mode: Optional[str]) -> Dict[str, object]:
    config = dict(base_config)
    if input_mode is not None:
        config["input_mode"] = input_mode
    return config


def _is_official_mode(pipeline_config: Dict[str, object]) -> bool:
    return str(pipeline_config.get("input_mode", "sample")).lower() == "official"


def _fetch_official_sources(pipeline_config: Dict[str, object]) -> None:
    official_input = pipeline_config.get("official_input", {})
    schools_dest = _resolve_input_path(str(official_input.get("schools_path")))
    ofsted_dest = _resolve_input_path(str(official_input.get("ofsted_path")))
    try:
        result = fetch(pipeline_config, schools_dest, ofsted_dest)
        for note in result.notes:
            LOGGER.info("fetch: %s", note)
    except FetchError as exc:
        raise OfficialSourceConfigError(str(exc)) from exc


def _preflight_official_mode(pipeline_config: Dict[str, object]) -> None:
    if not _is_official_mode(pipeline_config):
        return

    official_input = pipeline_config.get("official_input", {})
    schools_path = _resolve_input_path(str(official_input.get("schools_path")))
    ofsted_path = _resolve_input_path(str(official_input.get("ofsted_path")))

    missing_paths = [str(path) for path in (schools_path, ofsted_path) if not path.exists()]
    if missing_paths:
        raise OfficialSourceConfigError(
            "Official mode requires local source files before the run can start. Missing: {0}".format(
                ", ".join(missing_paths)
            )
        )


def run(
    area: str = "KT19",
    config_path: Optional[Path] = None,
    input_mode: Optional[str] = None,
) -> PipelineResult:
    """Run the scaffolded schools pipeline."""
    area_config = load_area_config(area=area, config_path=config_path)
    pipeline_config = _build_pipeline_config(load_pipeline_config(), input_mode=input_mode)
    threshold_config = load_threshold_config()
    if _is_official_mode(pipeline_config):
        _fetch_official_sources(pipeline_config)
    _preflight_official_mode(pipeline_config)
    context = PipelineContext(
        pipeline_name="schools",
        area=area,
        run_id=uuid.uuid4().hex[:12],
        started_at=datetime.now(timezone.utc),
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
    published = publish(extracted, transformed, validated, context)
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
            "Schools pipeline completed for area={0}. "
            "Artifacts written to data/marts and data/manifests."
        ).format(context.area),
        artifacts={
            "run_id": context.run_id,
            "record_count": published.record_count,
            "output_files": published.output_files,
            "quality_summary": validated.quality_summary,
        },
    )
