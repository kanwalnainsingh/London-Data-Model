"""Multi-borough orchestration for the schools pipeline."""

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from london_data_model.pipelines.schools.extract import (
    filter_by_la_codes,
    load_official_records,
)
from london_data_model.pipelines.schools.publish import publish
from london_data_model.pipelines.schools.transform import transform
from london_data_model.pipelines.schools.validate import validate
from london_data_model.pipelines.schools.pipeline import (
    _build_pipeline_config,
    _preflight_official_mode,
)
from london_data_model.settings import DOCS_DATA_DIR
from london_data_model.types import (
    AreaConfig,
    ExtractResult,
    PipelineContext,
    PipelineResult,
    PublishResult,
    ValidateResult,
)
from london_data_model.utils.config import (
    load_borough_configs,
    load_pipeline_config,
    load_threshold_config,
)


LOGGER = logging.getLogger(__name__)


def _publish_london_index(
    borough_results: List[Tuple[AreaConfig, PublishResult, ValidateResult]],
    pipeline_config: Dict[str, Any],
) -> None:
    """Write London-wide index and combined schools JSON to docs/data/."""
    areas = []
    total_schools = 0
    total_quality: Dict[str, int] = {"complete": 0, "partial": 0, "poor": 0}
    # Deduplicate schools across borough runs by URN; keep the closest entry.
    schools_by_urn: Dict[str, Any] = {}

    for borough_config, published, validated in borough_results:
        areas.append({
            "area_id": borough_config.area_id,
            "label": borough_config.label,
            "area_type": "borough",
            "la_code": borough_config.la_code,
            "school_count": published.record_count,
            "quality_counts": validated.quality_summary,
            "status_url": "./data/{0}-status.json".format(borough_config.area_id),
        })
        total_schools += published.record_count
        for key in total_quality:
            total_quality[key] += validated.quality_summary.get(key, 0)

        for record in validated.records:
            d = record.to_dict()
            urn = d.get("school_urn", "")
            existing = schools_by_urn.get(urn)
            if existing is None:
                schools_by_urn[urn] = d
            else:
                # Keep the entry with the smaller distance (school closer to its borough centre)
                existing_dist = existing.get("distance_km") or float("inf")
                new_dist = d.get("distance_km") or float("inf")
                if new_dist < existing_dist:
                    schools_by_urn[urn] = d

    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)

    index_payload = {
        "region_id": "london",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_mode": pipeline_config.get("input_mode", "sample"),
        "borough_count": len(areas),
        "school_count_total": total_schools,
        "quality_counts": total_quality,
        "areas": sorted(areas, key=lambda a: a["area_id"]),
    }
    index_path = DOCS_DATA_DIR / "london-index.json"
    index_path.write_text(json.dumps(index_payload, indent=2), encoding="utf-8")
    LOGGER.info(
        "Published London index: %s (%d boroughs, %d schools)",
        index_path,
        len(areas),
        total_schools,
    )

    combined_schools = sorted(schools_by_urn.values(), key=lambda s: s.get("school_name", ""))
    schools_path = DOCS_DATA_DIR / "london-schools.json"
    schools_path.write_text(
        json.dumps(combined_schools, separators=(",", ":")), encoding="utf-8"
    )
    LOGGER.info(
        "Published London schools: %s (%d unique schools)",
        schools_path,
        len(combined_schools),
    )


def run_london(
    boroughs: Optional[str] = None,
    config_path: Optional[Path] = None,
    input_mode: Optional[str] = None,
) -> PipelineResult:
    """Run the schools pipeline for multiple London boroughs.

    Args:
        boroughs: Optional comma-separated borough IDs. If None, all 33 boroughs run.
        config_path: Optional path to a custom area config (unused in multi-borough mode).
        input_mode: Optional override for input mode (sample or official).
    """
    pipeline_config = _build_pipeline_config(load_pipeline_config(), input_mode=input_mode)
    threshold_config = load_threshold_config()
    _preflight_official_mode(pipeline_config)

    borough_ids = (
        [b.strip() for b in boroughs.split(",") if b.strip()]
        if boroughs
        else None
    )
    borough_configs = load_borough_configs(borough_ids)

    LOGGER.info(
        "Starting London multi-borough run for %d boroughs (input_mode=%s)",
        len(borough_configs),
        pipeline_config.get("input_mode", "sample"),
    )

    # Load GIAS + Ofsted data once for all boroughs
    input_mode_effective = str(pipeline_config.get("input_mode", "sample")).lower()
    if input_mode_effective == "official":
        all_records, sources, base_notes = load_official_records(pipeline_config)
        LOGGER.info("Loaded %d total records from official sources", len(all_records))
    else:
        all_records, sources, base_notes = [], [], [
            "Sample mode: no records loaded for multi-borough run.",
        ]

    borough_results: List[Tuple[AreaConfig, PublishResult, ValidateResult]] = []
    total_schools = 0

    for borough_config in borough_configs:
        if borough_config.la_code is not None and all_records:
            filtered = filter_by_la_codes(all_records, [borough_config.la_code])
        else:
            filtered = list(all_records)

        LOGGER.info(
            "Borough %s (LA %s): %d records",
            borough_config.area_id,
            borough_config.la_code,
            len(filtered),
        )

        context = PipelineContext(
            pipeline_name="schools",
            area=borough_config.area_id,
            run_id=uuid.uuid4().hex[:12],
            started_at=datetime.now(timezone.utc),
            config_path=config_path,
            area_config=borough_config,
            pipeline_config=pipeline_config,
            threshold_config=threshold_config,
        )

        filter_note = (
            "Filtered to LA code {0} ({1} records).".format(
                borough_config.la_code, len(filtered)
            )
            if borough_config.la_code is not None
            else "No LA code filter applied."
        )
        extracted = ExtractResult(
            records=filtered,
            sources=sources,
            notes=list(base_notes) + [filter_note],
        )
        transformed = transform(extracted, context)
        validated = validate(transformed, context)
        published = publish(extracted, transformed, validated, context)

        borough_results.append((borough_config, published, validated))
        total_schools += published.record_count

    _publish_london_index(borough_results, pipeline_config)

    LOGGER.info(
        "London multi-borough run complete: %d boroughs, %d total schools",
        len(borough_results),
        total_schools,
    )

    return PipelineResult(
        pipeline_name="schools",
        area="london",
        status="success",
        message=(
            "London schools pipeline completed for {0} boroughs. "
            "{1} total schools published."
        ).format(len(borough_results), total_schools),
        artifacts={
            "borough_count": len(borough_results),
            "total_schools": total_schools,
        },
    )
