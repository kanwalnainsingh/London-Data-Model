"""Publish stage for the schools pipeline."""

import csv
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from london_data_model.settings import DOCS_DATA_DIR, MANIFESTS_DATA_DIR, MARTS_DATA_DIR
from london_data_model.types import (
    PipelineContext,
    PublishResult,
    SCHOOL_OUTPUT_FIELDS,
    ValidateResult,
)


LOGGER = logging.getLogger(__name__)


def _write_csv(path: Path, validated: ValidateResult) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=SCHOOL_OUTPUT_FIELDS)
        writer.writeheader()
        for record in validated.records:
            writer.writerow(record.to_dict())


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _build_public_status_payload(
    summary_payload: dict, manifest_payload: dict, context: PipelineContext
) -> dict:
    return {
        "area_id": summary_payload["area_id"],
        "pipeline_name": manifest_payload["pipeline_name"],
        "pipeline_version": manifest_payload["pipeline_version"],
        "run_id": manifest_payload["run_id"],
        "generated_at": summary_payload["generated_at"],
        "input_mode": context.pipeline_config.get("input_mode", "sample"),
        "school_count_total": summary_payload["school_count_total"],
        "school_count_by_phase": summary_payload["school_count_by_phase"],
        "school_count_by_accessibility_band": summary_payload[
            "school_count_by_accessibility_band"
        ],
        "quality_counts": manifest_payload["quality_counts"],
        "missing_ofsted_count": summary_payload["missing_ofsted_count"],
        "search_point_method": manifest_payload["search_point_method"],
        "search_point": manifest_payload["search_point"],
        "notes": manifest_payload["notes"],
    }


def _build_public_manifest_payload(manifest_payload: dict) -> dict:
    return {
        "run_id": manifest_payload["run_id"],
        "pipeline_name": manifest_payload["pipeline_name"],
        "pipeline_version": manifest_payload["pipeline_version"],
        "area_id": manifest_payload["area_id"],
        "search_point_method": manifest_payload["search_point_method"],
        "search_point": manifest_payload["search_point"],
        "record_counts": manifest_payload["record_counts"],
        "quality_counts": manifest_payload["quality_counts"],
        "notes": manifest_payload["notes"],
        "public_artifacts": {
            "status_json": "./data/kt19-status.json",
            "summary_json": "./data/kt19-summary.json",
        },
    }


def publish(validated: ValidateResult, context: PipelineContext) -> PublishResult:
    """Publish pipeline outputs and manifests."""
    LOGGER.info(
        "Starting publish stage for area=%s with %s validated records",
        context.area,
        len(validated.records),
    )

    MARTS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    MANIFESTS_DATA_DIR.mkdir(parents=True, exist_ok=True)

    base_name = "{0}-{1}".format(context.pipeline_name, context.area.lower())
    csv_path = MARTS_DATA_DIR / "{0}.csv".format(base_name)
    records_path = MARTS_DATA_DIR / "{0}.json".format(base_name)
    summary_path = MARTS_DATA_DIR / "{0}-summary.json".format(base_name)
    manifest_path = MANIFESTS_DATA_DIR / "{0}-manifest.json".format(base_name)
    public_summary_path = DOCS_DATA_DIR / "{0}-summary.json".format(context.area.lower())
    public_manifest_path = DOCS_DATA_DIR / "{0}-manifest.json".format(context.area.lower())
    public_status_path = DOCS_DATA_DIR / "{0}-status.json".format(context.area.lower())

    _write_csv(csv_path, validated)
    _write_json(records_path, [record.to_dict() for record in validated.records])

    summary_payload = {
        "area_id": context.area_config.area_id,
        "school_count_total": len(validated.records),
        "school_count_by_phase": {},
        "school_count_by_accessibility_band": {},
        "school_count_by_quality_status": validated.quality_summary,
        "missing_ofsted_count": 0,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_json(summary_path, summary_payload)

    manifest_payload = {
        "run_id": context.run_id,
        "pipeline_name": context.pipeline_name,
        "pipeline_version": context.pipeline_config.get("version"),
        "area_id": context.area_config.area_id,
        "search_point_method": context.area_config.search_point_method,
        "search_point": {
            "latitude": context.area_config.latitude,
            "longitude": context.area_config.longitude,
        },
        "output_files": {
            "csv": str(csv_path),
            "records_json": str(records_path),
            "summary_json": str(summary_path),
        },
        "record_counts": {
            "published": len(validated.records),
        },
        "quality_counts": validated.quality_summary,
        "notes": validated.notes,
    }
    _write_json(manifest_path, manifest_payload)
    _write_json(public_summary_path, summary_payload)
    _write_json(public_manifest_path, _build_public_manifest_payload(manifest_payload))
    _write_json(
        public_status_path,
        _build_public_status_payload(summary_payload, manifest_payload, context),
    )

    output_files = {
        "csv": str(csv_path),
        "records_json": str(records_path),
        "summary_json": str(summary_path),
        "manifest_json": str(manifest_path),
        "public_summary_json": str(public_summary_path),
        "public_manifest_json": str(public_manifest_path),
        "public_status_json": str(public_status_path),
    }
    LOGGER.info("Publish stage completed with outputs=%s", output_files)
    return PublishResult(
        output_files=output_files,
        record_count=len(validated.records),
        notes=["Placeholder artifacts published for pipeline skeleton."],
    )
